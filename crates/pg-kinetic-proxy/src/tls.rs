use std::{fs, io::Cursor, path::Path, sync::Arc};

use anyhow::{bail, Context};

use crate::config::{BackendTlsMode, ClientTlsMode, TlsConfig};
use tokio::net::TcpStream;
use tokio_rustls::rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    ClientConfig, DigitallySignedStruct, Error, RootCertStore, ServerConfig, SignatureScheme,
};
use tokio_rustls::{client::TlsStream as ClientTlsStream, server::TlsStream, TlsAcceptor, TlsConnector};
use tokio_rustls::rustls::pki_types::{ServerName, UnixTime};

pub fn load_server_config(config: &TlsConfig) -> anyhow::Result<Arc<ServerConfig>> {
    let cert_path = config
        .client_cert_path
        .as_deref()
        .context("client TLS certificate path is required")?;
    let key_path = config
        .client_key_path
        .as_deref()
        .context("client TLS private key path is required")?;

    let cert_chain = load_certificate_chain(cert_path, "client TLS certificate chain")?;
    let private_key = load_private_key(key_path, "client TLS private key")?;

    let builder = match config.client_tls_mode {
        ClientTlsMode::VerifyClient => {
            let ca_path = config
                .client_ca_path
                .as_deref()
                .context("client TLS CA path is required when client TLS mode is verify_client")?;
            let roots = load_root_store(ca_path)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .context("build client certificate verifier")?;
            ServerConfig::builder().with_client_cert_verifier(verifier)
        }
        ClientTlsMode::Disable | ClientTlsMode::Allow | ClientTlsMode::Require => {
            ServerConfig::builder().with_no_client_auth()
        }
    };

    let server_config = builder
        .with_single_cert(cert_chain, private_key)
        .context("build server TLS config")?;

    Ok(Arc::new(server_config))
}

pub fn load_backend_client_config(config: &TlsConfig) -> anyhow::Result<Arc<ClientConfig>> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_path) = config.backend_ca_path.as_deref() {
        let ca_certs = load_certificate_chain(ca_path, "backend TLS CA certificates")?;
        for cert in ca_certs {
            root_store.add(cert).with_context(|| {
                format!("add backend CA certificate from {}", ca_path.display())
            })?;
        }
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Arc::new(client_config))
}

pub fn backend_tls_settings(
    config: &TlsConfig,
) -> anyhow::Result<(Arc<ClientConfig>, ServerName<'static>)> {
    match config.backend_tls_mode {
        BackendTlsMode::Disable => bail!("backend TLS is disabled"),
        BackendTlsMode::Prefer | BackendTlsMode::Require => Ok((
            insecure_backend_client_config(),
            backend_server_name(config, false)?,
        )),
        BackendTlsMode::VerifyCa => Ok((
            verified_backend_client_config(config)?,
            backend_server_name(config, false)?,
        )),
        BackendTlsMode::VerifyFull => Ok((
            verified_backend_client_config(config)?,
            backend_server_name(config, true)?,
        )),
    }
}

pub async fn connect_backend_tls(
    stream: TcpStream,
    client_config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
) -> anyhow::Result<ClientTlsStream<TcpStream>> {
    TlsConnector::from(client_config)
        .connect(server_name, stream)
        .await
        .context("complete backend TLS handshake")
}

pub async fn accept_client_tls(
    stream: TcpStream,
    server_config: &Arc<ServerConfig>,
) -> anyhow::Result<TlsStream<TcpStream>> {
    TlsAcceptor::from(Arc::clone(server_config))
        .accept(stream)
        .await
        .context("complete client TLS handshake")
}

fn load_certificate_chain(
    path: &Path,
    action: &str,
) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let bytes = read_pem_file(path, action)?;
    let mut reader = Cursor::new(bytes);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parse {action} from {}", path.display()))?;

    if certs.is_empty() {
        bail!("no certificates found in {action} at {}", path.display());
    }

    Ok(certs)
}

fn load_private_key(path: &Path, action: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    let bytes = read_pem_file(path, action)?;

    let mut reader = Cursor::new(bytes.clone());
    if let Some(key) = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .next()
        .transpose()
        .with_context(|| format!("parse PKCS#8 {action} from {}", path.display()))?
    {
        return Ok(key.into());
    }

    let mut reader = Cursor::new(bytes.clone());
    if let Some(key) = rustls_pemfile::rsa_private_keys(&mut reader)
        .next()
        .transpose()
        .with_context(|| format!("parse RSA {action} from {}", path.display()))?
    {
        return Ok(key.into());
    }

    let mut reader = Cursor::new(bytes);
    if let Some(key) = rustls_pemfile::ec_private_keys(&mut reader)
        .next()
        .transpose()
        .with_context(|| format!("parse EC {action} from {}", path.display()))?
    {
        return Ok(key.into());
    }

    bail!("no private key found in {action} at {}", path.display());
}

fn load_root_store(path: &Path) -> anyhow::Result<RootCertStore> {
    let certs = load_certificate_chain(path, "CA certificates")?;
    let mut root_store = RootCertStore::empty();

    for cert in certs {
        root_store
            .add(cert)
            .with_context(|| format!("add CA certificate from {}", path.display()))?;
    }

    Ok(root_store)
}

fn verified_backend_client_config(config: &TlsConfig) -> anyhow::Result<Arc<ClientConfig>> {
    let ca_path = config
        .backend_ca_path
        .as_deref()
        .context("backend TLS CA path is required for backend TLS verification")?;
    let _ = ca_path;
    load_backend_client_config(config)
}

fn insecure_backend_client_config() -> Arc<ClientConfig> {
    let mut client_config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    client_config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification));
    Arc::new(client_config)
}

fn backend_server_name(
    config: &TlsConfig,
    require_explicit: bool,
) -> anyhow::Result<ServerName<'static>> {
    let server_name = match config.backend_server_name.as_deref() {
        Some(server_name) => server_name.to_owned(),
        None if require_explicit => {
            bail!("backend TLS server name is required for backend TLS verify_full")
        }
        None => String::from("localhost"),
    };

    ServerName::try_from(server_name).context("build backend TLS server name")
}

fn read_pem_file(path: &Path, action: &str) -> anyhow::Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("{action} from {}", path.display()))
}

#[derive(Debug)]
struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}
