use crate::Value;
use futures::{Future, future, task::Poll};
use std::{marker::PhantomData, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use tracing;
use yamux::{Config, Connection, ConnectionError, Mode};

/// Yamux 控制结构
pub struct YamuxCtrl<S> {
    /// yamux connection
    conn: Arc<Mutex<Connection<Compat<S>>>>,
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// 创建 yamux 客户端
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    /// 创建 yamux 服务端，服务端我们需要具体处理 stream
    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        Self::new(stream, config, false, f)
    }

    // 创建 YamuxCtrl
    /// 把 yamux config 设定一下
    /// config + stream + mode => yamux_conn
    /// yamux_conn => clone
    fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, mut f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        // Create config with more conservative settings
        let mut config = config.unwrap_or_default();
        let max_streams = 100;
        // Set window size to meet yamux minimum requirements (256 KiB per stream)
        let window_size = 256 * 1024;
        let connection_window = window_size * max_streams;

        config.set_max_num_streams(max_streams);
        config.set_max_connection_receive_window(Some(connection_window));
        config.set_read_after_close(true);

        // Create connection
        let conn = Arc::new(Mutex::new(Connection::new(stream.compat(), config, mode)));
        let conn_clone = conn.clone();

        // Handle all streams
        tokio::spawn(async move {
            tracing::info!("Starting yamux connection handler");
            loop {
                let mut conn = conn_clone.lock().await;
                match future::poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                    Some(Ok(stream)) => {
                        tracing::info!("Received new stream");
                        match f(stream).await {
                            Ok(_) => tracing::info!("Stream handled successfully"),
                            Err(e) => {
                                tracing::error!("Error handling stream: {:?}", e);
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("Error receiving stream: {:?}", e);
                        break;
                    }
                    None => {
                        tracing::info!("No more streams, connection closed");
                        break;
                    }
                }
            }
            tracing::info!("Yamux connection handler finished");
            Ok::<_, ConnectionError>(())
        });

        Self {
            conn,
            _conn: PhantomData::default(),
        }
    }

    /// 打开一个新的 stream
    pub async fn open_stream(&mut self) -> Result<Compat<yamux::Stream>, ConnectionError> {
        tracing::info!("Opening new yamux stream");
        let mut conn = self.conn.lock().await;
        let stream = future::poll_fn(|cx| match conn.poll_new_outbound(cx) {
            Poll::Ready(result) => match result {
                Ok(stream) => {
                    tracing::info!("Successfully created new outbound stream");
                    Poll::Ready(Ok(stream))
                }
                Err(e) => {
                    tracing::error!("Failed to create outbound stream: {:?}", e);
                    Poll::Ready(Err(e))
                }
            },
            Poll::Pending => {
                tracing::debug!("Waiting for outbound stream creation...");
                Poll::Pending
            }
        })
        .await?;
        tracing::info!("New yamux stream opened successfully");
        Ok(stream.compat())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::{
        CommandRequest, KvError, MemTable, ProstClientStream, ProstServerStream, Service,
        ServiceInner, Storage, TlsServerAcceptor, assert_res_ok,
        network::tls::tls_utils::{tls_acceptor, tls_connector},
        utils::DummyStream,
    };
    use anyhow::Result;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::server;
    use tracing::warn;

    pub async fn start_server_with<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
        f: impl Fn(server::TlsStream<TcpStream>, Service) + Send + Sync + 'static,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service: Service = ServiceInner::new(store).into();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        tracing::info!("Accepted connection from {}", addr);
                        match tls.accept(stream).await {
                            Ok(stream) => {
                                tracing::info!("TLS connection established with {}", addr);
                                f(stream, service.clone());
                            }
                            Err(e) => {
                                tracing::error!("Failed to establish TLS with {}: {:?}", addr, e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {:?}", e);
                    }
                }
            }
        });

        Ok(addr)
    }

    /// 创建 ymaux server
    pub async fn start_yamux_server<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let f = |stream, service: Service| {
            let mut config = Config::default();
            let max_streams = 100;
            let window_size = 256 * 1024; // 256 KiB per stream
            let connection_window = window_size * max_streams;

            config.set_max_num_streams(max_streams);
            config.set_max_connection_receive_window(Some(connection_window));
            config.set_read_after_close(true);

            tracing::info!(
                "Creating yamux server with config: window_size={}, max_streams={}",
                window_size,
                max_streams
            );

            YamuxCtrl::new_server(stream, Some(config), move |s| {
                let svc = service.clone();
                async move {
                    tracing::info!("Processing new yamux stream");
                    let stream = ProstServerStream::new(s.compat(), svc);

                    // Process the stream in a separate task
                    tokio::spawn(async move {
                        match stream.process().await {
                            Ok(_) => tracing::info!("Stream processed successfully"),
                            Err(e) => tracing::error!("Error processing stream: {:?}", e),
                        }
                    });

                    // Return Ok immediately to allow yamux to continue accepting new streams
                    Ok(())
                }
            });
        };
        start_server_with(addr, tls, store, f).await
    }

    #[tokio::test]
    async fn yamux_ctrl_creation_should_work() -> Result<()> {
        let s = DummyStream::default();
        let mut ctrl = YamuxCtrl::new_client(s, None);
        let stream = ctrl.open_stream().await;

        assert!(stream.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn yamux_ctrl_client_server_should_work() -> Result<()> {
        tracing::info!("Starting yamux client-server test");

        // Create TLS yamux server with timeout
        let acceptor = tls_acceptor(false)?;
        let addr = start_yamux_server("127.0.0.1:0", acceptor, MemTable::new()).await?;
        tracing::info!("Yamux server started at {}", addr);

        // Create TLS yamux client with timeout
        let connector = tls_connector(false)?;
        let stream = match tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(stream)) => {
                tracing::info!("TCP connection established");
                stream
            }
            Ok(Err(e)) => {
                tracing::error!("Failed to establish TCP connection: {:?}", e);
                return Err(e.into());
            }
            Err(_) => {
                tracing::error!("Timeout while establishing TCP connection");
                return Err(anyhow::anyhow!("Timeout while establishing TCP connection"));
            }
        };

        let stream = match tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            connector.connect(stream),
        )
        .await
        {
            Ok(Ok(stream)) => {
                tracing::info!("TLS connection established");
                stream
            }
            Ok(Err(e)) => {
                tracing::error!("Failed to establish TLS connection: {:?}", e);
                return Err(e.into());
            }
            Err(_) => {
                tracing::error!("Timeout while establishing TLS connection");
                return Err(anyhow::anyhow!("Timeout while establishing TLS connection"));
            }
        };

        // Create yamux client with custom config
        let mut config = Config::default();
        let max_streams = 100;
        let window_size = 256 * 1024; // 256 KiB per stream
        let connection_window = window_size * max_streams;

        config.set_max_num_streams(max_streams);
        config.set_max_connection_receive_window(Some(connection_window));
        config.set_read_after_close(true);

        let mut ctrl = YamuxCtrl::new_client(stream, Some(config));
        tracing::info!(
            "Yamux client created with config: window_size={}, max_streams={}",
            window_size,
            max_streams
        );

        // Test sequence with proper cleanup
        let test_result = async {
            // Open stream with timeout
            let stream =
                match tokio::time::timeout(tokio::time::Duration::from_secs(5), ctrl.open_stream())
                    .await
                {
                    Ok(Ok(stream)) => {
                        tracing::info!("Yamux stream opened successfully");
                        stream
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Failed to open yamux stream: {:?}", e);
                        return Err(e.into());
                    }
                    Err(_) => {
                        tracing::error!("Timeout while opening yamux stream");
                        return Err(anyhow::anyhow!("Timeout while opening yamux stream"));
                    }
                };

            // Create client
            let mut client = ProstClientStream::new(stream);
            tracing::info!("ProstClientStream created");

            // Test HSET
            let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
            tracing::info!("Sending HSET command");
            let res = match tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                client.execute_unary(&cmd),
            )
            .await
            {
                Ok(Ok(res)) => {
                    tracing::info!("HSET command completed successfully");
                    res
                }
                Ok(Err(e)) => {
                    tracing::error!("Failed to execute HSET command: {:?}", e);
                    return Err(e.into());
                }
                Err(_) => {
                    tracing::error!("Timeout while executing HSET command");
                    return Err(anyhow::anyhow!("Timeout while executing HSET command"));
                }
            };
            assert_res_ok(res, &[Value::default()], &[]);

            // Test HGET
            let cmd = CommandRequest::new_hget("t1", "k1");
            tracing::info!("Sending HGET command");
            let res = match tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                client.execute_unary(&cmd),
            )
            .await
            {
                Ok(Ok(res)) => {
                    tracing::info!("HGET command completed successfully");
                    res
                }
                Ok(Err(e)) => {
                    tracing::error!("Failed to execute HGET command: {:?}", e);
                    return Err(e.into());
                }
                Err(_) => {
                    tracing::error!("Timeout while executing HGET command");
                    return Err(anyhow::anyhow!("Timeout while executing HGET command"));
                }
            };
            assert_res_ok(res, &["v1".into()], &[]);

            Ok::<_, anyhow::Error>(())
        }
        .await;

        // Ensure cleanup happens
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        tracing::info!("Test cleanup completed");

        test_result
    }
}
