use anyhow::Result;
use kvs_poc::{CommandRequest, ProstClientStream, TlsClientConnector};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";
    let ca_cert = include_str!("../fixtures/ca.cert");

    let connector = TlsClientConnector::new("kvserver.acme.inc", None, Some(ca_cert))?;
    let raw_stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(raw_stream).await?;
    let mut client_stream = ProstClientStream::new(stream);
    let cmd = CommandRequest::new_hset("t1", "hello", "world!!!".to_string().into());
    let data = client_stream.execute(cmd).await?;
    info!("got resp: {:?}", data);
    Ok(())
}
