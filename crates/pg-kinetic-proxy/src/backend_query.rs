use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use bytes::{BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};

use crate::{
    auth::{BackendAuthSession, BackendCredentials},
    backend::Backend,
    config::{AuthConfig, SocketConfig, TlsConfig},
    reload::BackendCredentialCache,
};
use pg_kinetic_core::secrets::{Md5Secret, ScramVerifier, UserSecret};
use pg_kinetic_wire::{
    backend::parse_backend_frame,
    protocol::{BackendTag, FrontendTag, ProtocolVersion, ReadyStatusByte},
};

const AUTH_QUERY_APPLICATION_NAME: &str = "pg-kinetic-auth-query";
const ROW_DESCRIPTION_TAG: u8 = b'T';

#[derive(Clone, Debug)]
pub(crate) struct AuthQueryService {
    backend_addr: SocketAddr,
    tls: TlsConfig,
    socket: SocketConfig,
    credentials: BackendCredentialCache,
    cache: AuthQueryCache,
}

impl AuthQueryService {
    #[must_use]
    pub(crate) fn new(
        backend_addr: SocketAddr,
        tls: TlsConfig,
        socket: SocketConfig,
        credentials: BackendCredentialCache,
    ) -> Self {
        Self {
            backend_addr,
            tls,
            socket,
            credentials,
            cache: AuthQueryCache::default(),
        }
    }

    pub(crate) async fn lookup(
        &self,
        auth: &AuthConfig,
        username: &str,
        max_backend_buffer_bytes: usize,
    ) -> anyhow::Result<Option<UserSecret>> {
        let ttl = auth.auth_query_cache_ttl();
        if let Some(secret) = self.cache.get(username, ttl).await {
            return Ok(Some(secret));
        }

        let Some(credentials) = self.credentials.load() else {
            bail!("auth query service credentials are not configured");
        };
        let sql = render_auth_query(&auth.auth_query, username)?;
        let mut backend = self.connect_authenticated(credentials.as_ref()).await?;
        let rows = run_simple_query(&mut backend, &sql, max_backend_buffer_bytes).await?;
        let secret = parse_auth_query_result(&rows, username)?;
        if let Some(secret) = secret.clone() {
            self.cache.put(username, secret).await;
        }

        Ok(secret)
    }

    async fn connect_authenticated(
        &self,
        credentials: &BackendCredentials,
    ) -> anyhow::Result<Backend> {
        let mut backend =
            Backend::connect_with_socket(self.backend_addr, &self.tls, &self.socket).await?;
        backend
            .stream_mut()
            .write_all(&auth_query_startup_packet(credentials.username()))
            .await
            .context("send auth query backend startup")?;

        let mut buffer = BytesMut::with_capacity(8192);
        let mut auth = BackendAuthSession::new(credentials.clone())?;
        loop {
            let read = backend
                .stream_mut()
                .read_buf(&mut buffer)
                .await
                .context("read auth query backend startup response")?;
            anyhow::ensure!(read > 0, "backend closed during auth query startup");

            while let Some(frame) = parse_backend_frame(&mut buffer)? {
                if frame.tag == u8::from(BackendTag::Authentication) {
                    if let Some(response) = auth.respond(&frame.payload, backend.is_tls())? {
                        backend
                            .stream_mut()
                            .write_all(&response)
                            .await
                            .context("respond to auth query backend authentication")?;
                    }
                }

                if frame.ready_status().is_some() {
                    anyhow::ensure!(
                        frame.ready_status() == Some(pg_kinetic_wire::backend::ReadyStatus::Idle),
                        "auth query backend startup did not become idle"
                    );
                    return Ok(backend);
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AuthQueryCache {
    entries: std::sync::Arc<RwLock<HashMap<String, AuthQueryCacheEntry>>>,
}

#[derive(Clone, Debug)]
struct AuthQueryCacheEntry {
    secret: UserSecret,
    inserted_at: Instant,
}

impl AuthQueryCache {
    async fn get(&self, username: &str, ttl: Duration) -> Option<UserSecret> {
        if ttl.is_zero() {
            return None;
        }

        let entries = self.entries.read().await;
        let entry = entries.get(username)?;
        (entry.inserted_at.elapsed() < ttl).then(|| entry.secret.clone())
    }

    async fn put(&self, username: &str, secret: UserSecret) {
        let mut entries = self.entries.write().await;
        entries.insert(
            username.to_owned(),
            AuthQueryCacheEntry {
                secret,
                inserted_at: Instant::now(),
            },
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimpleQueryRow {
    pub columns: Vec<Option<String>>,
}

pub(crate) async fn run_simple_query(
    backend: &mut Backend,
    sql: &str,
    max_backend_buffer_bytes: usize,
) -> anyhow::Result<Vec<SimpleQueryRow>> {
    backend
        .stream_mut()
        .write_all(&encode_query_message(sql))
        .await
        .context("send auth query")?;

    let mut rows = Vec::new();
    let mut buffer = BytesMut::with_capacity(8192);
    loop {
        anyhow::ensure!(
            buffer.len() < max_backend_buffer_bytes,
            "auth query response exceeded buffer limit"
        );
        let read = backend
            .stream_mut()
            .read_buf(&mut buffer)
            .await
            .context("read auth query response")?;
        anyhow::ensure!(read > 0, "backend closed during auth query");
        anyhow::ensure!(
            buffer.len() <= max_backend_buffer_bytes,
            "auth query response exceeded buffer limit"
        );

        while let Some(frame) = parse_backend_frame(&mut buffer)? {
            match frame.tag {
                tag if tag == u8::from(BackendTag::DataRow) => {
                    rows.push(parse_data_row(&frame.payload)?);
                }
                tag if tag == u8::from(BackendTag::ErrorResponse) => {
                    bail!("auth query backend returned an error");
                }
                tag if tag == u8::from(BackendTag::ReadyForQuery) => {
                    anyhow::ensure!(
                        frame.payload.as_ref() == [u8::from(ReadyStatusByte::Idle)],
                        "auth query backend did not return idle status"
                    );
                    return Ok(rows);
                }
                ROW_DESCRIPTION_TAG => {}
                tag if tag == u8::from(BackendTag::CommandComplete) => {}
                tag if tag == u8::from(BackendTag::ParameterStatus) => {}
                tag if tag == u8::from(BackendTag::BackendKeyData) => {}
                _ => bail!("unexpected auth query backend frame"),
            }
        }
    }
}

fn parse_auth_query_result(
    rows: &[SimpleQueryRow],
    expected_username: &str,
) -> anyhow::Result<Option<UserSecret>> {
    match rows {
        [] => Ok(None),
        [row] => parse_auth_query_row(row, expected_username).map(Some),
        _ => bail!("auth query returned duplicate rows"),
    }
}

fn parse_auth_query_row(
    row: &SimpleQueryRow,
    expected_username: &str,
) -> anyhow::Result<UserSecret> {
    anyhow::ensure!(
        row.columns.len() == 2,
        "auth query returned unexpected row shape"
    );
    let Some(username) = row.columns[0].as_deref() else {
        bail!("auth query returned null username");
    };
    anyhow::ensure!(
        username == expected_username,
        "auth query returned unexpected username"
    );
    let Some(secret) = row.columns[1].as_deref() else {
        bail!("auth query returned null secret");
    };
    parse_user_secret(secret)
}

fn parse_user_secret(secret: &str) -> anyhow::Result<UserSecret> {
    if secret.starts_with("md5") {
        return Ok(UserSecret::Md5(Md5Secret::parse(secret)?));
    }
    Ok(UserSecret::ScramSha256(ScramVerifier::parse(secret)?))
}

fn render_auth_query(template: &str, username: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        template.match_indices("$1").count() == 1,
        "auth_query must contain exactly one $1 placeholder"
    );
    Ok(template.replace("$1", &quote_literal(username)))
}

fn quote_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push('\'');
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

fn encode_query_message(sql: &str) -> BytesMut {
    let mut frame = BytesMut::with_capacity(sql.len() + 6);
    frame.put_u8(u8::from(FrontendTag::Query));
    frame.put_i32((sql.len() + 5) as i32);
    frame.extend_from_slice(sql.as_bytes());
    frame.put_u8(0);
    frame
}

fn auth_query_startup_packet(username: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(ProtocolVersion::V3.to_i32());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(username.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"application_name\0");
    body.extend_from_slice(AUTH_QUERY_APPLICATION_NAME.as_bytes());
    body.put_u8(0);
    body.put_u8(0);

    let mut packet = BytesMut::with_capacity(body.len() + 4);
    packet.put_i32((body.len() + 4) as i32);
    packet.extend_from_slice(&body);
    packet
}

fn parse_data_row(payload: &[u8]) -> anyhow::Result<SimpleQueryRow> {
    let mut offset = 0;
    let count = read_i16(payload, &mut offset).context("read data row column count")?;
    anyhow::ensure!(count >= 0, "negative data row column count");
    let mut columns = Vec::with_capacity(count as usize);

    for _ in 0..count {
        let len = read_i32(payload, &mut offset).context("read data row column length")?;
        if len == -1 {
            columns.push(None);
            continue;
        }
        anyhow::ensure!(len >= 0, "invalid data row column length");
        let len = len as usize;
        let end = offset
            .checked_add(len)
            .context("data row column length overflow")?;
        anyhow::ensure!(end <= payload.len(), "truncated data row column");
        let value = std::str::from_utf8(&payload[offset..end])
            .context("auth query returned non-utf8 column")?
            .to_owned();
        columns.push(Some(value));
        offset = end;
    }

    anyhow::ensure!(offset == payload.len(), "data row has trailing bytes");
    Ok(SimpleQueryRow { columns })
}

fn read_i16(payload: &[u8], offset: &mut usize) -> anyhow::Result<i16> {
    let end = offset.checked_add(2).context("i16 offset overflow")?;
    let bytes = payload.get(*offset..end).context("missing i16")?;
    *offset = end;
    Ok(i16::from_be_bytes(bytes.try_into()?))
}

fn read_i32(payload: &[u8], offset: &mut usize) -> anyhow::Result<i32> {
    let end = offset.checked_add(4).context("i32 offset overflow")?;
    let bytes = payload.get(*offset..end).context("missing i32")?;
    *offset = end;
    Ok(i32::from_be_bytes(bytes.try_into()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_auth_query_username_literals() {
        assert_eq!(quote_literal("alice"), "'alice'");
        assert_eq!(quote_literal(""), "''");
        assert_eq!(quote_literal("a'lice"), "'a''lice'");
        assert_eq!(
            render_auth_query("select $1", "x'; drop table users; --").expect("render"),
            "select 'x''; drop table users; --'"
        );
        assert!(render_auth_query("select $1, $1", "alice").is_err());
        assert!(render_auth_query("select 'alice'", "alice").is_err());
    }

    #[test]
    fn parses_data_rows_strictly() {
        let mut payload = BytesMut::new();
        payload.put_i16(2);
        payload.put_i32(5);
        payload.extend_from_slice(b"alice");
        payload.put_i32(-1);

        let row = parse_data_row(&payload).expect("parse");
        assert_eq!(
            row,
            SimpleQueryRow {
                columns: vec![Some(String::from("alice")), None]
            }
        );

        let mut trailing = payload.clone();
        trailing.put_u8(0);
        assert!(parse_data_row(&trailing).is_err());

        let mut truncated = BytesMut::new();
        truncated.put_i16(1);
        truncated.put_i32(5);
        truncated.extend_from_slice(b"ali");
        assert!(parse_data_row(&truncated).is_err());
    }

    #[tokio::test]
    async fn cache_expires_after_ttl() {
        let cache = AuthQueryCache::default();
        let secret = UserSecret::Trust;
        cache.put("alice", secret).await;
        assert!(cache.get("alice", Duration::from_secs(60)).await.is_some());
        assert!(cache.get("alice", Duration::ZERO).await.is_none());
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert!(cache.get("alice", Duration::from_millis(1)).await.is_none());
    }
}
