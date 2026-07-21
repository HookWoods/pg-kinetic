use std::{fs, path::Path};

use anyhow::{bail, Context};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::BytesMut;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{
    config::{AuthConfig, AuthFailureMessageMode, AuthMode},
    proxy::ClientConnection,
};
use pg_kinetic_core::secrets::{generate_nonce, ScramVerifier, UserSecret, UserStore};
use pg_kinetic_wire::{
    auth::{
        authentication_ok, authentication_sasl_continue, authentication_sasl_final,
        authentication_sasl_scram_sha_256,
    },
    backend::build_error_response,
    frame::{parse_frontend_frame, FrontendFrame},
};

type HmacSha256 = Hmac<Sha256>;

const AUTH_FAILURE_SQLSTATE: &str = "28P01";
const SCRAM_MECHANISM: &str = "SCRAM-SHA-256";
const PASSWORD_MESSAGE_TAG: u8 = b'p';

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientAuthOutcome {
    PassThrough,
    Authenticated,
    Rejected,
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
        Some(UserSecret::ScramSha256(_)) => {
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
