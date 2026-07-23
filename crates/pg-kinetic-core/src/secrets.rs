use std::collections::BTreeMap;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use getrandom::fill;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

const SCRAM_SHA_256_PREFIX: &str = "SCRAM-SHA-256";
const SCRAM_SHA_256_KEY_LEN: usize = 32;
const DEFAULT_NONCE_LEN: usize = 18;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UserSecret {
    Trust,
    ScramSha256(ScramVerifier),
    Md5(Md5Secret),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScramVerifier {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Md5Secret {
    hex: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserStore {
    case_sensitive: bool,
    secrets: BTreeMap<String, UserSecret>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SecretError {
    #[error("SCRAM verifier must start with {expected}")]
    InvalidPrefix { expected: &'static str },
    #[error("SCRAM verifier is missing a required field")]
    MissingField,
    #[error("SCRAM verifier has an invalid iteration count")]
    InvalidIterations,
    #[error("SCRAM verifier contains invalid base64 in {field}")]
    InvalidBase64 { field: &'static str },
    #[error("SCRAM verifier field {field} must be {expected} bytes, got {actual}")]
    InvalidKeyLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("SCRAM nonce generation failed")]
    NonceGeneration,
    #[error("MD5 secret must use md5 followed by 32 lowercase hex characters")]
    InvalidMd5Secret,
}

impl Md5Secret {
    pub fn parse(secret: &str) -> Result<Self, SecretError> {
        let hex = secret
            .strip_prefix("md5")
            .ok_or(SecretError::InvalidMd5Secret)?;
        if hex.len() != 32
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(SecretError::InvalidMd5Secret);
        }

        Ok(Self {
            hex: hex.to_owned(),
        })
    }

    #[must_use]
    pub fn stored_hex(&self) -> &str {
        &self.hex
    }
}

impl ScramVerifier {
    pub fn parse(verifier: &str) -> Result<Self, SecretError> {
        let (prefix, rest) = verifier.split_once('$').ok_or(SecretError::MissingField)?;
        if prefix != SCRAM_SHA_256_PREFIX {
            return Err(SecretError::InvalidPrefix {
                expected: SCRAM_SHA_256_PREFIX,
            });
        }

        let (iteration_and_salt, keys) = rest.split_once('$').ok_or(SecretError::MissingField)?;
        let (iterations, salt) = iteration_and_salt
            .split_once(':')
            .ok_or(SecretError::MissingField)?;
        let (stored_key, server_key) = keys.split_once(':').ok_or(SecretError::MissingField)?;

        let iterations = iterations
            .parse::<u32>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(SecretError::InvalidIterations)?;

        let salt = STANDARD
            .decode(salt)
            .map_err(|_| SecretError::InvalidBase64 { field: "salt" })?;
        let stored_key = STANDARD
            .decode(stored_key)
            .map_err(|_| SecretError::InvalidBase64 {
                field: "stored_key",
            })?;
        let server_key = STANDARD
            .decode(server_key)
            .map_err(|_| SecretError::InvalidBase64 {
                field: "server_key",
            })?;

        validate_scram_key_length("stored_key", &stored_key)?;
        validate_scram_key_length("server_key", &server_key)?;

        Ok(Self {
            iterations,
            salt,
            stored_key,
            server_key,
        })
    }

    #[must_use]
    pub fn to_postgres_verifier(&self) -> String {
        format!(
            "{SCRAM_SHA_256_PREFIX}${}:{}${}:{}",
            self.iterations,
            STANDARD.encode(&self.salt),
            STANDARD.encode(&self.stored_key),
            STANDARD.encode(&self.server_key),
        )
    }

    #[must_use]
    pub fn verify_password(&self, password: &[u8]) -> bool {
        let derived_stored_key = derive_scram_stored_key(password, &self.salt, self.iterations);
        derived_stored_key
            .as_slice()
            .ct_eq(self.stored_key.as_slice())
            .into()
    }
}

impl UserStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn case_sensitive() -> Self {
        Self {
            case_sensitive: true,
            secrets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn case_insensitive() -> Self {
        Self {
            case_sensitive: false,
            secrets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_case_sensitive(case_sensitive: bool) -> Self {
        if case_sensitive {
            Self::case_sensitive()
        } else {
            Self::case_insensitive()
        }
    }

    #[must_use]
    pub fn is_case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    pub fn insert(
        &mut self,
        username: impl Into<String>,
        secret: UserSecret,
    ) -> Option<UserSecret> {
        let username = self.normalize(username.into());
        self.secrets.insert(username, secret)
    }

    #[must_use]
    pub fn get(&self, username: &str) -> Option<&UserSecret> {
        if self.case_sensitive {
            return self.secrets.get(username);
        }

        let normalized = username.to_ascii_lowercase();
        self.secrets.get(&normalized)
    }

    fn normalize(&self, username: String) -> String {
        if self.case_sensitive {
            username
        } else {
            username.to_ascii_lowercase()
        }
    }
}

impl Default for UserStore {
    fn default() -> Self {
        Self {
            case_sensitive: true,
            secrets: BTreeMap::new(),
        }
    }
}

fn validate_scram_key_length(field: &'static str, value: &[u8]) -> Result<(), SecretError> {
    if value.len() == SCRAM_SHA_256_KEY_LEN {
        Ok(())
    } else {
        Err(SecretError::InvalidKeyLength {
            field,
            expected: SCRAM_SHA_256_KEY_LEN,
            actual: value.len(),
        })
    }
}

pub fn generate_nonce() -> Result<String, SecretError> {
    let mut bytes = [0u8; DEFAULT_NONCE_LEN];
    fill(&mut bytes).map_err(|_| SecretError::NonceGeneration)?;
    Ok(base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes))
}

#[must_use]
fn derive_scram_stored_key(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let salted_password = pbkdf2_hmac_sha256(password, salt, iterations);
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    Sha256::digest(client_key).to_vec()
}

fn pbkdf2_hmac_sha256(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    let mut block = Vec::with_capacity(salt.len() + 4);
    block.extend_from_slice(salt);
    block.extend_from_slice(&1u32.to_be_bytes());

    let mut u = hmac_sha256(password, &block);
    let mut output = u;

    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        xor_in_place(&mut output, &u);
    }

    output
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn xor_in_place(left: &mut [u8; SCRAM_SHA_256_KEY_LEN], right: &[u8; SCRAM_SHA_256_KEY_LEN]) {
    for (left_byte, right_byte) in left.iter_mut().zip(right) {
        *left_byte ^= *right_byte;
    }
}
