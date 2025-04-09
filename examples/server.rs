// // server
// #[tokio::main]
// async fn main() {
//     init tracing
//     init stream
//     init service
//     let service = Service::new(storage::new());
//     let listener = TcpListener::listen(port);
//     loop {
//         stream = listener.accept().await?;
//         plain_stream = AsyncProstStream::new(stream);
//         let svc = service.clone();
//         tokio::spawn(move || {
//             while let Some(cmd) = plain_stream.next().await {
//                 let resp = &svc.dispatch(cmd);
//                 plain_stream.send(resp)
//             }
//         })
//     }
// }

use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kvs_poc::{CommandRequest, CommandResponse, MemTable, Service, ServiceInner};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let service: Service<MemTable> = ServiceInner::new(MemTable::new()).into();
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        let svc = service.clone();
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                info!("Got a new command: {:?}", cmd);
                let res = svc.execute(cmd);
                stream.send(res).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
