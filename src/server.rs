use anyhow::Result;
use kvs_poc::{MemTable, ProstServerStream, Service, ServiceInner, TlsServerAcceptor};
use tokio::net::TcpListener;
use tracing::info;

/// init addr / tcpListener / service
/// loop {
///     let stream = listener.accept();
///     server = ServerStream::new(stream, service.clone());
///     tokio::spawn(async move {server.process()});
/// }
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";

    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");

    let acceptor = TlsServerAcceptor::new(server_cert, server_key, None)?;
    let service: Service<MemTable> = ServiceInner::new(MemTable::new()).into();
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let tls = acceptor.clone();
        let (raw_stream, addr) = listener.accept().await?;
        let stream = tls.accept(raw_stream).await?;
        info!("Client {:?} connected", addr);
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move { stream.process().await });
    }
}
