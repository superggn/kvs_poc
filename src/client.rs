use anyhow::Result;
use kvs_poc::{CommandRequest, ProstClientStream};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;
    let mut client_stream = ProstClientStream::new(stream);
    let cmd = CommandRequest::new_hset("t1", "hello", "world!!!".to_string().into());
    let data = client_stream.execute(cmd).await?;
    info!("got resp: {:?}", data);
    Ok(())
}
