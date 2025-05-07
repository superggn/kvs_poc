use crate::KvError;

use std::io::Cursor;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{AllowAnyAuthenticatedClient, NoClientAuth, PrivateKey, RootCertStore};
use tokio_rustls::rustls::{Certificate, ClientConfig, ServerConfig, internal::pemfile};
use tokio_rustls::webpki::DNSNameRef;
use tokio_rustls::{
    TlsAcceptor, client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream,
};

/// KV Server 自己的 ALPN (Application-Layer Protocol Negotiation)
const ALPN_KV: &str = "kv";

#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: Arc<ServerConfig>,
}

pub struct TlsClientConnector {
    pub config: Arc<ClientConfig>,
    pub domain: Arc<String>,
}

impl TlsClientConnector {
    /// init a struct with conf in it
    /// cert + other creds => client conf
    pub fn new(
        domain: impl Into<String>,
        identity: Option<(&str, &str)>,
        server_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let mut config = ClientConfig::new();
        // if have client cert => load it
        if let Some((cert, key)) = identity {
            let certs = load_certs(cert)?;
            let key = load_key(key)?;
            config.set_single_client_cert(certs, key)?;
        }

        // load root cert chain
        config.root_store = match rustls_native_certs::load_native_certs() {
            Ok(store) | Err((Some(store), _)) => store,
            Err((None, error)) => return Err(error.into()),
        };

        // if has server_ca => load it
        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert);
            config.root_store.add_pem_file(&mut buf).unwrap();
        }
        Ok(Self {
            config: Arc::new(config),
            domain: Arc::new(domain.into()),
        })
    }

    /// domain str => dns
    /// dns + conf => decrypt stream
    pub async fn connect<S>(&self, stream: S) -> Result<ClientTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let dns = DNSNameRef::try_from_ascii_str(self.domain.as_str())
            .map_err(|_| KvError::Internal("Invalid DNS name".into()))?;
        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await?;
        Ok(stream)
    }
}

impl TlsServerAcceptor {
    /// load server cert / CA cert => ServerConfig
    /// cert path + key path + option<CA_path> => acceptor
    /// acceptor: tls_encrypted_stream <=> plain_stream
    /// tls_handshake => processed in lib, no need to care
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;
        let mut config = match client_ca {
            None => ServerConfig::new(NoClientAuth::new()),
            Some(cert) => {
                // if client_ca exists => add cert into trusted_chain
                let mut cert = Cursor::new(cert);
                let mut client_root_cert_store = RootCertStore::empty();
                client_root_cert_store
                    .add_pem_file(&mut cert)
                    .map_err(|_| KvError::CertificateParseError("CA", "cert"))?;
                let client_auth = AllowAnyAuthenticatedClient::new(client_root_cert_store);
                ServerConfig::new(client_auth)
            }
        };
        // load root cert
        config
            .set_single_cert(certs, key)
            .map_err(|_| KvError::CertificateParseError("server", "cert"))?;
        config.set_protocols(&[Vec::from(ALPN_KV)]);
        Ok(Self {
            inner: Arc::new(config),
        })
    }

    /// server conf => decrypt stream
    pub async fn accept<S>(&self, stream: S) -> Result<ServerTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let acceptor = TlsAcceptor::from(self.inner.clone());
        Ok(acceptor.accept(stream).await?)
    }
}

/// cert &str => Vec<Certificate object>
fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let mut cert = Cursor::new(cert);
    pemfile::certs(&mut cert).map_err(|_| KvError::CertificateParseError("server", "cert"))
}

/// key &str => PrivateKey object
fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    let mut cursor = Cursor::new(key);
    // try loading pkcs8 key
    if let Ok(mut keys) = pemfile::pkcs8_private_keys(&mut cursor) {
        if !key.is_empty() {
            return Ok(keys.remove(0));
        }
    }
    cursor.set_position(0);
    // try loading rsa key
    if let Ok(mut keys) = pemfile::rsa_private_keys(&mut cursor) {
        if !key.is_empty() {
            return Ok(keys.remove(0));
        }
    }
    Err(KvError::CertificateParseError("private", "key"))
}

#[cfg(test)]
pub mod tls_utils {
    use crate::{KvError, TlsClientConnector, TlsServerAcceptor};

    const CA_CERT: &str = include_str!("../../fixtures/ca.cert");
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert");
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key");
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key");

    pub fn tls_connector(client_cert: bool) -> Result<TlsClientConnector, KvError> {
        let ca = Some(CA_CERT);
        let client_identity = Some((CLIENT_CERT, CLIENT_KEY));

        match client_cert {
            false => TlsClientConnector::new("kvserver.acme.inc", None, ca),
            true => TlsClientConnector::new("kvserver.acme.inc", client_identity, ca),
        }
    }

    pub fn tls_acceptor(client_cert: bool) -> Result<TlsServerAcceptor, KvError> {
        let ca = Some(CA_CERT);
        match client_cert {
            true => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca),
            false => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tls_utils::tls_acceptor;
    use super::*;
    use crate::network::tls::tls_utils::tls_connector;
    use anyhow::Result;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    #[tokio::test]
    async fn tls_should_work() -> Result<()> {
        // 正常初始化的 echo server 返回的值是对的
        // 单向 tls
        let addr = start_echo_server(false).await?;
        let connector = tls_connector(false)?;
        // domain + server_ca_cert => client conn
        let stream = TcpStream::connect(addr).await?;
        let mut plain_stream = connector.connect(stream).await?;
        plain_stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        plain_stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");
        Ok(())
    }

    #[tokio::test]
    async fn tls_with_client_cert_should_work() -> Result<()> {
        // 正常初始化的 echo server 返回的值是对的
        // 双向 tls
        let addr = start_echo_server(true).await?;
        let connector = tls_connector(true)?;

        let encrypted_stream = TcpStream::connect(addr).await?;
        let mut plain_stream = connector.connect(encrypted_stream).await?;
        plain_stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        plain_stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");
        Ok(())
    }

    #[tokio::test]
    async fn tls_with_bad_domain_should_not_work() -> Result<()> {
        // client 这边如果域名不对， 就连不起 tls
        // let addr = start_echo_server(None).await?;
        let addr = start_echo_server(false).await?;
        let mut connector = tls_connector(false)?;

        connector.domain = Arc::new("kvserver1.acme.inc".into());

        let encrypted_stream = TcpStream::connect(addr).await?;
        let res = connector.connect(encrypted_stream).await;
        assert!(res.is_err());
        Ok(())
    }

    /// ca object => init an echo server
    async fn start_echo_server(client_cert: bool) -> Result<SocketAddr> {
        // init acceptor + raw_encrypted_stream
        // acceptor + encrypted_stream => plain stream
        // read 12 u8 from stream, then write the same content back
        // init acceptor
        // let acceptor = TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca)?;
        let acceptor = tls_acceptor(client_cert)?;
        // init echo
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut buf = [0; 12];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });
        Ok(addr)
    }
}
