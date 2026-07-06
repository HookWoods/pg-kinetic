use std::{fs, io::Cursor, path::Path, sync::Arc};

use anyhow::{bail, Context};

use crate::config::{ClientTlsMode, TlsConfig};
use tokio_rustls::rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    ClientConfig, RootCertStore, ServerConfig,
};

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

fn read_pem_file(path: &Path, action: &str) -> anyhow::Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("{action} from {}", path.display()))
}
