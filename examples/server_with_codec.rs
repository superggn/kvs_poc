use anyhow::Result;
use futures::prelude::*;
use kvs_poc::{CommandRequest, MemTable, Service, ServiceInner};
use prost::Message;
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
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
            // raw_stream => lengthDelimited stream
            // while let get new buf => decode buf => CommandRequest!
            let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
            while let Some(Ok(mut buf)) = stream.next().await {
                let cmd = CommandRequest::decode(&buf[..]).unwrap();
                info!("Got a new command: {:?}", cmd);
                let resp = svc.execute(cmd);
                buf.clear();
                resp.encode(&mut buf).unwrap();
                stream.send(buf.freeze()).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
            // ========================================
            // myStream = MyProstStream::new(tcp stream);
            // myStream.execute();
            // ========================================
            // let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
            // while let Some(Ok(mut buf)) = stream.next().await {
            //     let cmd = CommandRequest::decode(&buf[..]).unwrap();
            //     info!("Got a new command: {:?}", cmd);
            //     let resp = svc.execute(cmd);
            //     buf.clear();
            //     resp.encode(&mut buf).unwrap();
            //     stream.send(buf.freeze()).await.unwrap();
            // }
            // info!("Client {:?} disconnected", addr);
            // ========================================
            // let mut stream =
            //     AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            // while let Some(Ok(cmd)) = stream.next().await {
            //     info!("Got a new command: {:?}", cmd);
            //     let res = svc.execute(cmd);
            //     stream.send(res).await.unwrap();
            // }
            // info!("Client {:?} disconnected", addr);
        });
    }
}
