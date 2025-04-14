mod frame;

use crate::{CommandRequest, CommandResponse, KvError, Service};

use bytes::BytesMut;
use frame::{FrameCodec, read_frame};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::info;

pub struct ProstServerStream<S> {
    inner: S,
    service: Service,
}

impl<S> ProstServerStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S, service: Service) -> Self {
        Self {
            inner: stream,
            service,
        }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        while let Ok(cmd) = self.recv().await {
            info!("got a new command:{:?}", cmd);
            let res = self.service.execute(cmd);
            self.send(res).await?;
        }
        Ok(())
    }

    async fn send(&mut self, msg: CommandResponse) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        msg.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<CommandRequest, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        CommandRequest::decode_frame(&mut buf)
    }
}

pub struct ProstClientStream<S> {
    inner: S,
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self { inner: stream }
    }

    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError> {
        self.send(cmd).await?;
        Ok(self.recv().await?)
    }

    async fn send(&mut self, cmd: CommandRequest) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        cmd.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<CommandResponse, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        CommandResponse::decode_frame(&mut buf)
    }
}

#[cfg(test)]
mod tests {
    /// 测哪些
    /// basic client-server communication
    /// compression client-server communication
    use super::*;
    use crate::{MemTable, ServiceInner, Value, assert_res_ok};
    use anyhow::Result;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};

    /// init server
    /// test regular request & response
    #[tokio::test]
    async fn client_server_basic_com_should_work() -> Result<()> {
        let addr = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);
        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = client.execute(cmd).await.unwrap();
        assert_res_ok(res, &[Value::default()], &[]);
        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute(cmd).await.unwrap();
        assert_res_ok(res, &["v1".into()], &[]);
        Ok(())
    }

    /// test compression req & resp
    #[tokio::test]
    async fn client_server_compression_should_work() -> Result<()> {
        let addr = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);
        let v_long: Value = Bytes::from(vec![0u8; 16384]).into();
        let cmd = CommandRequest::new_hset("t1", "k1", v_long.clone());
        let res = client.execute(cmd).await.unwrap();
        assert_res_ok(res, &[Value::default()], &[]);
        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute(cmd).await.unwrap();
        assert_res_ok(res, &[v_long], &[]);
        Ok(())
    }

    /// start a server in a new thread
    async fn start_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service: Service = ServiceInner::new(MemTable::new()).into();
                let server = ProstServerStream::new(stream, service);
                tokio::spawn(server.process());
            }
        });
        Ok(addr)
    }
}
