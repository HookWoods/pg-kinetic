use std::{fs, path::Path, sync::Arc};

use anyhow::{bail, Context};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{BufMut, BytesMut};
use hmac::{Hmac, KeyInit, Mac};
use md5::Md5;
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{
    config::{AuthConfig, AuthFailureMessageMode, AuthMode},
    proxy::ClientConnection,
};
use pg_kinetic_core::secrets::{generate_nonce, Md5Secret, ScramVerifier, UserSecret, UserStore};
use pg_kinetic_wire::{
    auth::{
        authentication_md5_password, authentication_ok, authentication_sasl_continue,
        authentication_sasl_final, authentication_sasl_scram_sha_256,
    },
    backend::build_error_response,
    frame::{parse_frontend_frame, FrontendFrame},
};

type HmacSha256 = Hmac<Sha256>;

const AUTH_FAILURE_SQLSTATE: &str = "28P01";
const SCRAM_MECHANISM: &str = "SCRAM-SHA-256";
const PASSWORD_MESSAGE_TAG: u8 = b'p';

pub struct BackendCredentials {
    username: String,
    password: String,
    provider: Option<Arc<dyn BackendCredentialProvider>>,
}

impl std::fmt::Debug for BackendCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackendCredentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

impl Clone for BackendCredentials {
    fn clone(&self) -> Self {
        Self {
            username: self.username.clone(),
            password: self.password.clone(),
            provider: self.provider.clone(),
        }
    }
}

impl PartialEq for BackendCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.username == other.username && self.password == other.password
    }
}

impl Eq for BackendCredentials {}

impl BackendCredentials {
    fn with_provider(mut self, provider: Arc<dyn BackendCredentialProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    fn resolve(&self) -> Result<Self, AuthError> {
        self.provider
            .as_ref()
            .map_or_else(|| Ok(self.clone()), |provider| provider.credentials())
    }
}

impl BackendCredentials {
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("auth.backend_user requires auth.backend_password_env_var_name")]
    MissingPasswordEnvironmentVariableName,
    #[error("auth.backend_password_env_var_name requires auth.backend_user")]
    MissingBackendUser,
    #[error("backend service credentials are incompatible with auth_mode=pass_through")]
    PassThroughCredentials,
    #[error("read backend service password from environment")]
    Environment,
    #[error("backend service password from environment is empty")]
    EmptyPassword,
}

pub trait BackendCredentialProvider: Send + Sync {
    fn credentials(&self) -> Result<BackendCredentials, AuthError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentCredentialProvider {
    username: String,
    password_env_var_name: String,
}

impl EnvironmentCredentialProvider {
    pub fn new(username: impl Into<String>, password_env_var_name: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password_env_var_name: password_env_var_name.into(),
        }
    }
}

impl BackendCredentialProvider for EnvironmentCredentialProvider {
    fn credentials(&self) -> Result<BackendCredentials, AuthError> {
        let password = std::env::var_os(&self.password_env_var_name)
            .ok_or(AuthError::Environment)?
            .into_string()
            .map_err(|_| AuthError::Environment)?;
        if password.is_empty() {
            return Err(AuthError::EmptyPassword);
        }

        Ok(BackendCredentials {
            username: self.username.clone(),
            password,
            provider: None,
        })
    }
}

pub(crate) struct BackendAuthSession {
    credentials: BackendCredentials,
    client_nonce: String,
    client_first_bare: Option<String>,
    expected_server_signature: Option<[u8; 32]>,
}

impl BackendAuthSession {
    pub(crate) fn new(credentials: BackendCredentials) -> anyhow::Result<Self> {
        Ok(Self::with_nonce(credentials.resolve()?, generate_nonce()?))
    }

    fn with_nonce(credentials: BackendCredentials, client_nonce: String) -> Self {
        Self {
            credentials,
            client_nonce,
            client_first_bare: None,
            expected_server_signature: None,
        }
    }

    pub(crate) fn respond(
        &mut self,
        request: &[u8],
        backend_is_tls: bool,
    ) -> anyhow::Result<Option<BytesMut>> {
        let code = read_i32(request, 0).context("read backend authentication code")?;
        match code {
            0 => Ok(None),
            3 => {
                anyhow::ensure!(
                    backend_is_tls,
                    "backend requested a cleartext password without TLS"
                );
                let mut password = BytesMut::from(self.credentials.password().as_bytes());
                password.put_u8(0);
                Ok(Some(password_message(&password)))
            }
            5 => Ok(Some(self.md5_response(request)?)),
            10 => Ok(Some(self.scram_initial_response(request)?)),
            11 => Ok(Some(self.scram_final_response(request)?)),
            12 => {
                self.verify_scram_server_final(request)?;
                Ok(None)
            }
            _ => anyhow::bail!("unsupported backend authentication request code {code}"),
        }
    }

    fn md5_response(&self, request: &[u8]) -> anyhow::Result<BytesMut> {
        anyhow::ensure!(
            request.len() == 8,
            "MD5 authentication request has invalid length"
        );
        let salt = &request[4..8];
        let first_digest = Md5::digest(format!(
            "{}{}",
            self.credentials.password(),
            self.credentials.username()
        ));
        let first = hex_lower(first_digest.as_ref());
        let mut second_input = BytesMut::with_capacity(first.len() + salt.len());
        second_input.extend_from_slice(first.as_bytes());
        second_input.extend_from_slice(salt);
        let second_digest = Md5::digest(second_input);
        let mut response =
            BytesMut::from(format!("md5{}", hex_lower(second_digest.as_ref())).as_bytes());
        response.put_u8(0);
        Ok(password_message(&response))
    }

    fn scram_initial_response(&mut self, request: &[u8]) -> anyhow::Result<BytesMut> {
        let mechanisms = std::str::from_utf8(
            request
                .get(4..)
                .context("SCRAM authentication request is missing mechanisms")?,
        )
        .context("parse SCRAM authentication mechanisms")?;
        anyhow::ensure!(
            mechanisms
                .split('\0')
                .any(|mechanism| mechanism == SCRAM_MECHANISM),
            "backend does not support SCRAM-SHA-256"
        );

        let client_first_bare = format!(
            "n={},r={}",
            scram_escape(self.credentials.username()),
            self.client_nonce
        );
        let client_first = format!("n,,{client_first_bare}");
        let mut payload = BytesMut::new();
        payload.extend_from_slice(SCRAM_MECHANISM.as_bytes());
        payload.put_u8(0);
        payload.put_i32(client_first.len() as i32);
        payload.extend_from_slice(client_first.as_bytes());
        self.client_first_bare = Some(client_first_bare);
        Ok(password_message(&payload))
    }

    fn scram_final_response(&mut self, request: &[u8]) -> anyhow::Result<BytesMut> {
        let server_first = std::str::from_utf8(
            request
                .get(4..)
                .context("SCRAM server-first message is missing")?,
        )
        .context("parse SCRAM server-first message")?;
        let server_nonce = scram_attribute(server_first, 'r')?;
        anyhow::ensure!(
            server_nonce.starts_with(&self.client_nonce),
            "SCRAM server nonce does not extend the client nonce"
        );
        let salt = STANDARD
            .decode(scram_attribute(server_first, 's')?)
            .context("decode SCRAM salt")?;
        let iterations = scram_attribute(server_first, 'i')?
            .parse::<u32>()
            .context("parse SCRAM iteration count")?;
        anyhow::ensure!(iterations > 0, "SCRAM iteration count must be positive");
        let client_first_bare = self
            .client_first_bare
            .as_deref()
            .context("received SCRAM server-first before client-first")?;
        let final_without_proof = format!("c=biws,r={server_nonce}");
        let auth_message = format!("{client_first_bare},{server_first},{final_without_proof}");

        let mut salted_password = [0_u8; 32];
        pbkdf2_hmac::<Sha256>(
            self.credentials.password().as_bytes(),
            &salt,
            iterations,
            &mut salted_password,
        );
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(client_key);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof = xor_arrays(&client_key, &client_signature);
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        self.expected_server_signature = Some(hmac_sha256(&server_key, auth_message.as_bytes()));

        let client_final = format!("{final_without_proof},p={}", STANDARD.encode(client_proof));
        Ok(password_message(client_final.as_bytes()))
    }

    fn verify_scram_server_final(&mut self, request: &[u8]) -> anyhow::Result<()> {
        let server_final = std::str::from_utf8(
            request
                .get(4..)
                .context("SCRAM server-final message is missing")?,
        )
        .context("parse SCRAM server-final message")?;
        anyhow::ensure!(
            !server_final.starts_with("e="),
            "backend rejected SCRAM authentication: {server_final}"
        );
        let expected = self
            .expected_server_signature
            .take()
            .context("received SCRAM server-final before client-final")?;
        let actual = STANDARD
            .decode(scram_attribute(server_final, 'v')?)
            .context("decode SCRAM server signature")?;
        let matches: bool = actual.as_slice().ct_eq(expected.as_slice()).into();
        anyhow::ensure!(matches, "backend SCRAM server signature mismatch");
        Ok(())
    }
}

fn password_message(payload: &[u8]) -> BytesMut {
    let mut message = BytesMut::with_capacity(payload.len() + 5);
    message.put_u8(PASSWORD_MESSAGE_TAG);
    message.put_i32((payload.len() + 4) as i32);
    message.extend_from_slice(payload);
    message
}

fn scram_attribute(message: &str, key: char) -> anyhow::Result<&str> {
    message
        .split(',')
        .find_map(|item| item.strip_prefix(&format!("{key}=")))
        .with_context(|| format!("SCRAM message is missing {key} attribute"))
}

fn scram_escape(value: &str) -> String {
    value.replace('=', "=3D").replace(',', "=2C")
}

fn xor_arrays(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = left[index] ^ right[index];
    }
    output
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientAuthOutcome {
    PassThrough,
    Authenticated,
    Rejected,
}

pub(crate) fn load_backend_credentials(
    auth: &AuthConfig,
) -> anyhow::Result<Option<BackendCredentials>> {
    let Some(provider) = load_backend_credential_provider(auth)? else {
        return Ok(None);
    };

    Ok(Some(provider.credentials()?.with_provider(provider)))
}

pub(crate) fn load_backend_credential_provider(
    auth: &AuthConfig,
) -> anyhow::Result<Option<Arc<dyn BackendCredentialProvider>>> {
    match (
        auth.backend_user.as_deref(),
        auth.backend_password_env_var_name.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(AuthError::MissingPasswordEnvironmentVariableName.into()),
        (None, Some(_)) => Err(AuthError::MissingBackendUser.into()),
        (Some(_), Some(_)) if auth.auth_mode == AuthMode::PassThrough => {
            Err(AuthError::PassThroughCredentials.into())
        }
        (Some(username), Some(password_env_var_name)) => Ok(Some(Arc::new(
            EnvironmentCredentialProvider::new(username, password_env_var_name),
        ))),
    }
}

pub fn load_user_store(path: Option<&Path>) -> anyhow::Result<UserStore> {
    let mut store = UserStore::new();
    let Some(path) = path else {
        return Ok(store);
    };

    let contents = fs::read_to_string(path)
        .with_context(|| format!("read auth users file {}", path.display()))?;

    for (line_number, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (username, secret) = line.split_once('=').with_context(|| {
            format!(
                "parse auth users file {} line {}",
                path.display(),
                line_number + 1
            )
        })?;

        let username = username.trim().trim_matches('"');
        let secret = secret.trim().trim_matches('"');

        let user_secret = if secret.eq_ignore_ascii_case("trust") {
            UserSecret::Trust
        } else if secret.starts_with("md5") {
            UserSecret::Md5(Md5Secret::parse(secret).with_context(|| {
                format!(
                    "parse MD5 verifier for user {} in {} line {}",
                    username,
                    path.display(),
                    line_number + 1
                )
            })?)
        } else {
            UserSecret::ScramSha256(ScramVerifier::parse(secret).with_context(|| {
                format!(
                    "parse SCRAM verifier for user {} in {} line {}",
                    username,
                    path.display(),
                    line_number + 1
                )
            })?)
        };

        store.insert(username.to_owned(), user_secret);
    }

    Ok(store)
}

pub(crate) async fn authenticate_client(
    client: &mut ClientConnection,
    username: &str,
    auth: &AuthConfig,
    users: &UserStore,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<ClientAuthOutcome> {
    match auth.auth_mode {
        AuthMode::PassThrough => Ok(ClientAuthOutcome::PassThrough),
        AuthMode::Trust => authenticate_trust(client, username, auth, users).await,
        AuthMode::ScramSha256 => {
            authenticate_scram(client, username, auth, users, max_client_buffer_bytes).await
        }
        AuthMode::Md5 => {
            authenticate_md5(client, username, auth, users, max_client_buffer_bytes).await
        }
    }
}

async fn authenticate_trust(
    client: &mut ClientConnection,
    username: &str,
    auth: &AuthConfig,
    users: &UserStore,
) -> anyhow::Result<ClientAuthOutcome> {
    match users.get(username) {
        Some(UserSecret::Trust) => {
            let ok = authentication_ok();
            client
                .write_all(&ok)
                .await
                .context("write trust authentication ok")?;
            Ok(ClientAuthOutcome::Authenticated)
        }
        Some(UserSecret::ScramSha256(_) | UserSecret::Md5(_)) => {
            reject_authentication(
                client,
                auth.auth_failure_message_mode,
                username,
                "password required",
            )
            .await?;
            Ok(ClientAuthOutcome::Rejected)
        }
        None => {
            reject_authentication(
                client,
                auth.auth_failure_message_mode,
                username,
                "unknown user",
            )
            .await?;
            Ok(ClientAuthOutcome::Rejected)
        }
    }
}

async fn authenticate_scram(
    client: &mut ClientConnection,
    username: &str,
    auth: &AuthConfig,
    users: &UserStore,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<ClientAuthOutcome> {
    let Some(UserSecret::ScramSha256(verifier)) = users.get(username) else {
        reject_authentication(
            client,
            auth.auth_failure_message_mode,
            username,
            "unknown user",
        )
        .await?;
        return Ok(ClientAuthOutcome::Rejected);
    };

    let sasl_request = authentication_sasl_scram_sha_256();
    client
        .write_all(&sasl_request)
        .await
        .context("write SCRAM authentication request")?;

    let initial = read_authentication_frame(client, max_client_buffer_bytes)
        .await
        .context("read SCRAM initial response")?;
    let initial = parse_password_frame(&initial)?;
    let (mechanism, client_first) = parse_scram_initial_response(initial)?;
    anyhow::ensure!(
        mechanism == SCRAM_MECHANISM,
        "unsupported SCRAM mechanism {mechanism}"
    );

    let (client_first_bare, client_first_username, client_nonce) =
        parse_scram_client_first(client_first)?;
    anyhow::ensure!(
        client_first_username == username,
        "SCRAM client username does not match startup user"
    );

    let server_nonce = generate_nonce().context("generate SCRAM server nonce")?;
    let server_first = format!(
        "r={client_nonce}{server_nonce},s={},i={}",
        STANDARD.encode(&verifier.salt),
        verifier.iterations,
    );
    let server_first_message = authentication_sasl_continue(server_first.as_bytes());
    client
        .write_all(&server_first_message)
        .await
        .context("write SCRAM server-first message")?;

    let final_response = read_authentication_frame(client, max_client_buffer_bytes)
        .await
        .context("read SCRAM final response")?;
    let final_response = parse_password_frame(&final_response)?;
    let client_final = std::str::from_utf8(final_response).context("parse SCRAM final response")?;
    let (channel_binding, combined_nonce, proof) = parse_scram_client_final(client_final)?;

    anyhow::ensure!(
        channel_binding == "biws",
        "unsupported SCRAM channel binding"
    );
    anyhow::ensure!(
        combined_nonce == format!("{client_nonce}{server_nonce}"),
        "SCRAM nonce mismatch"
    );

    let auth_message =
        format!("{client_first_bare},{server_first},c={channel_binding},r={combined_nonce}");
    if verify_scram_proof(verifier, auth_message.as_bytes(), &proof).is_err() {
        reject_authentication(
            client,
            auth.auth_failure_message_mode,
            username,
            "invalid password",
        )
        .await?;
        return Ok(ClientAuthOutcome::Rejected);
    }

    let server_signature = scram_server_signature(verifier, auth_message.as_bytes());
    let server_final = format!("v={}", STANDARD.encode(server_signature));
    let server_final_message = authentication_sasl_final(server_final.as_bytes());
    client
        .write_all(&server_final_message)
        .await
        .context("write SCRAM server-final message")?;
    let ok = authentication_ok();
    client
        .write_all(&ok)
        .await
        .context("write SCRAM authentication ok")?;

    Ok(ClientAuthOutcome::Authenticated)
}

async fn authenticate_md5(
    client: &mut ClientConnection,
    username: &str,
    auth: &AuthConfig,
    users: &UserStore,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<ClientAuthOutcome> {
    let Some(UserSecret::Md5(secret)) = users.get(username) else {
        reject_authentication(
            client,
            auth.auth_failure_message_mode,
            username,
            "unknown user",
        )
        .await?;
        return Ok(ClientAuthOutcome::Rejected);
    };

    let mut salt = [0_u8; 4];
    getrandom::fill(&mut salt).context("generate MD5 authentication salt")?;
    client
        .write_all(&authentication_md5_password(salt))
        .await
        .context("write MD5 authentication request")?;

    let response = read_authentication_frame(client, max_client_buffer_bytes)
        .await
        .context("read MD5 password response")?;
    let response = parse_md5_password_response(parse_password_frame(&response)?);
    let Some(response) = response else {
        reject_authentication(
            client,
            auth.auth_failure_message_mode,
            username,
            "malformed md5 response",
        )
        .await?;
        return Ok(ClientAuthOutcome::Rejected);
    };

    let expected = expected_md5_client_hash(secret, salt);
    if bool::from(expected.as_bytes().ct_eq(response)) {
        client
            .write_all(&authentication_ok())
            .await
            .context("write MD5 authentication ok")?;
        Ok(ClientAuthOutcome::Authenticated)
    } else {
        reject_authentication(
            client,
            auth.auth_failure_message_mode,
            username,
            "invalid password",
        )
        .await?;
        Ok(ClientAuthOutcome::Rejected)
    }
}

async fn reject_authentication(
    client: &mut ClientConnection,
    mode: AuthFailureMessageMode,
    username: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let message = match mode {
        AuthFailureMessageMode::Generic => String::from("password authentication failed"),
        AuthFailureMessageMode::Detailed => {
            format!("password authentication failed for user {username}: {reason}")
        }
    };
    let error = build_error_response(AUTH_FAILURE_SQLSTATE, &message);
    client
        .write_all(&error)
        .await
        .context("write auth failure response")
}

async fn read_authentication_frame(
    client: &mut ClientConnection,
    max_client_buffer_bytes: usize,
) -> anyhow::Result<FrontendFrame> {
    let mut buffer = BytesMut::with_capacity(512);
    loop {
        if let Some(frame) = parse_frontend_frame(&mut buffer)? {
            return Ok(frame);
        }

        if buffer.len() >= max_client_buffer_bytes {
            bail!("client authentication message exceeded buffer limit");
        }

        let read = client
            .read_buf(&mut buffer)
            .await
            .context("read client authentication frame")?;
        anyhow::ensure!(read > 0, "client disconnected during authentication");
        anyhow::ensure!(
            buffer.len() <= max_client_buffer_bytes,
            "client authentication message exceeded buffer limit"
        );
    }
}

fn parse_password_frame(frame: &FrontendFrame) -> anyhow::Result<&[u8]> {
    anyhow::ensure!(
        frame.tag == PASSWORD_MESSAGE_TAG,
        "unexpected frontend authentication message tag {:#04x}",
        frame.tag
    );
    Ok(frame.payload.as_ref())
}

fn parse_md5_password_response(payload: &[u8]) -> Option<&[u8]> {
    let payload = payload.strip_suffix(&[0]).unwrap_or(payload);
    let response = payload.strip_prefix(b"md5")?;
    if response.len() == 32
        && response
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Some(response)
    } else {
        None
    }
}

fn expected_md5_client_hash(secret: &Md5Secret, salt: [u8; 4]) -> String {
    let mut second_input = BytesMut::with_capacity(secret.stored_hex().len() + salt.len());
    second_input.extend_from_slice(secret.stored_hex().as_bytes());
    second_input.extend_from_slice(&salt);
    hex_lower(Md5::digest(second_input).as_ref())
}

fn parse_scram_initial_response(payload: &[u8]) -> anyhow::Result<(&str, &str)> {
    let (mechanism, offset) = read_cstr(payload, 0)?;
    let length = read_i32(payload, offset).context("read SCRAM initial response length")?;
    anyhow::ensure!(length >= 0, "SCRAM initial response length is invalid");
    let length = length as usize;
    let start = offset + 4;
    anyhow::ensure!(
        payload.len() >= start + length,
        "SCRAM initial response is incomplete"
    );
    let response = std::str::from_utf8(&payload[start..start + length])
        .context("parse SCRAM initial response")?;
    Ok((mechanism, response))
}

fn parse_scram_client_first(message: &str) -> anyhow::Result<(String, String, String)> {
    anyhow::ensure!(
        message.starts_with("n,,"),
        "SCRAM client first message is malformed"
    );
    let bare = &message[3..];
    let mut username = None;
    let mut nonce = None;

    for item in bare.split(',') {
        let (key, value) = item
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("SCRAM client first item is malformed"))?;
        match key {
            "n" => username = Some(scram_unescape(value)?),
            "r" => nonce = Some(value.to_owned()),
            _ => {}
        }
    }

    let username = username.context("SCRAM client first message is missing username")?;
    let nonce = nonce.context("SCRAM client first message is missing nonce")?;
    Ok((bare.to_owned(), username, nonce))
}

fn parse_scram_client_final(message: &str) -> anyhow::Result<(String, String, String)> {
    let mut channel_binding = None;
    let mut nonce = None;
    let mut proof = None;

    for item in message.split(',') {
        let (key, value) = item
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("SCRAM client final item is malformed"))?;
        match key {
            "c" => channel_binding = Some(value.to_owned()),
            "r" => nonce = Some(value.to_owned()),
            "p" => proof = Some(value.to_owned()),
            _ => {}
        }
    }

    Ok((
        channel_binding.context("SCRAM client final message is missing channel binding")?,
        nonce.context("SCRAM client final message is missing nonce")?,
        proof.context("SCRAM client final message is missing proof")?,
    ))
}

fn verify_scram_proof(
    verifier: &ScramVerifier,
    auth_message: &[u8],
    client_proof_b64: &str,
) -> anyhow::Result<()> {
    let client_proof = STANDARD
        .decode(client_proof_b64)
        .context("decode SCRAM client proof")?;
    anyhow::ensure!(
        client_proof.len() == verifier.stored_key.len(),
        "SCRAM client proof has invalid length"
    );

    let client_signature = hmac_sha256(&verifier.stored_key, auth_message);
    let client_key = xor_bytes(&client_proof, &client_signature);
    let derived_stored_key = Sha256::digest(client_key);
    let proof_matches: bool = derived_stored_key
        .as_slice()
        .ct_eq(verifier.stored_key.as_slice())
        .into();
    anyhow::ensure!(proof_matches, "SCRAM proof verification failed");
    Ok(())
}

fn scram_server_signature(verifier: &ScramVerifier, auth_message: &[u8]) -> [u8; 32] {
    hmac_sha256(&verifier.server_key, auth_message)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn xor_bytes(left: &[u8], right: &[u8; 32]) -> [u8; 32] {
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = left[index] ^ right[index];
    }
    output
}

fn read_cstr(bytes: &[u8], start: usize) -> anyhow::Result<(&str, usize)> {
    let terminator = bytes[start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| start + offset)
        .context("read SCRAM cstring terminator")?;
    let value = std::str::from_utf8(&bytes[start..terminator]).context("parse SCRAM cstring")?;
    Ok((value, terminator + 1))
}

fn read_i32(bytes: &[u8], start: usize) -> anyhow::Result<i32> {
    anyhow::ensure!(bytes.len() >= start + 4, "read i32 from SCRAM payload");
    Ok(i32::from_be_bytes([
        bytes[start],
        bytes[start + 1],
        bytes[start + 2],
        bytes[start + 3],
    ]))
}

fn scram_unescape(value: &str) -> anyhow::Result<String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '=' {
            output.push(ch);
            continue;
        }

        let first = chars
            .next()
            .context("SCRAM escape sequence is incomplete")?;
        let second = chars
            .next()
            .context("SCRAM escape sequence is incomplete")?;
        match (first, second) {
            ('2', 'C') => output.push(','),
            ('3', 'D') => output.push('='),
            _ => bail!("SCRAM escape sequence is invalid"),
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    use super::{BackendAuthSession, BackendCredentials};

    fn credentials() -> BackendCredentials {
        BackendCredentials {
            username: String::from("pool_user"),
            password: String::from("pool-password"),
            provider: None,
        }
    }

    #[test]
    fn backend_auth_sends_cleartext_password_only_over_tls() {
        let mut session = BackendAuthSession::with_nonce(credentials(), String::from("nonce"));

        let response = session
            .respond(&3_i32.to_be_bytes(), true)
            .expect("cleartext auth response")
            .expect("cleartext password frame");

        assert_eq!(response.as_ref(), b"p\0\0\0\x12pool-password\0");
        assert!(session.respond(&3_i32.to_be_bytes(), false).is_err());
    }

    #[test]
    fn backend_auth_sends_the_postgres_md5_password_response() {
        let mut session = BackendAuthSession::with_nonce(credentials(), String::from("nonce"));
        let mut request = 5_i32.to_be_bytes().to_vec();
        request.extend_from_slice(&[1, 2, 3, 4]);

        let response = session
            .respond(&request, true)
            .expect("MD5 auth response")
            .expect("MD5 password frame");

        assert_eq!(
            response.as_ref(),
            b"p\0\0\0(md5f0fd7950af7ca2887fd5d036850c905d\0"
        );
    }

    #[test]
    fn backend_auth_completes_scram_and_verifies_the_server_signature() {
        let mut session = BackendAuthSession::with_nonce(credentials(), String::from("nonce"));
        let mut initial_request = 10_i32.to_be_bytes().to_vec();
        initial_request.extend_from_slice(b"SCRAM-SHA-256\0\0");

        let initial = session
            .respond(&initial_request, true)
            .expect("SCRAM initial response")
            .expect("SCRAM initial frame");
        assert!(initial.ends_with(b"n,,n=pool_user,r=nonce"));

        let mut continue_request = 11_i32.to_be_bytes().to_vec();
        continue_request.extend_from_slice(b"r=nonce-server,s=c2FsdA==,i=4096");
        let final_response = session
            .respond(&continue_request, true)
            .expect("SCRAM final response")
            .expect("SCRAM final frame");
        assert!(final_response.windows(7).any(|window| window == b"c=biws,"));

        let signature = session
            .expected_server_signature
            .expect("expected server signature");
        let mut server_final = 12_i32.to_be_bytes().to_vec();
        server_final.extend_from_slice(format!("v={}", STANDARD.encode(signature)).as_bytes());
        assert_eq!(
            session
                .respond(&server_final, true)
                .expect("valid server signature"),
            None
        );
    }

    #[test]
    fn backend_auth_rejects_an_invalid_scram_server_signature() {
        let mut session = BackendAuthSession::with_nonce(credentials(), String::from("nonce"));
        let mut initial_request = 10_i32.to_be_bytes().to_vec();
        initial_request.extend_from_slice(b"SCRAM-SHA-256\0\0");
        let _ = session
            .respond(&initial_request, true)
            .expect("SCRAM initial response");
        let mut continue_request = 11_i32.to_be_bytes().to_vec();
        continue_request.extend_from_slice(b"r=nonce-server,s=c2FsdA==,i=4096");
        let _ = session
            .respond(&continue_request, true)
            .expect("SCRAM final response");

        let error = session
            .respond(b"\0\0\0\x0cv=not-a-valid-signature", true)
            .expect_err("invalid server signature must fail");
        assert!(error.to_string().contains("signature"));
    }
}
