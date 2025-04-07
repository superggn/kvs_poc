use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kvs_poc::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;
use tracing::info;

/// tracing
/// ```rust
/// #[tokio::main]
/// async fn main() {
///     init tracing
///     init addr
///     init connection
///     init cmdRequest
///     send request
///     println!(resp)
/// }
/// ```

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;
    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();
    let cmd = CommandRequest::new_hset("t1", "hello", "world!!!".to_string().into());
    client.send(cmd).await?;
    info!("sent");
    if let Some(Ok(data)) = client.next().await {
        info!("got resp: {:?}", data)
    };
    info!("done!!!");
    Ok(())
}
