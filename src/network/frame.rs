// use std::io::{Read, Write};

// use crate::{CommandRequest, CommandResponse, KvError};
// use bytes::{Buf, BufMut, BytesMut};
// use flate2::{Compression, read::GzDecoder, write::GzEncoder};
// use prost::Message;
// use tokio::io::{AsyncRead, AsyncReadExt};
// use tracing::debug;

// /// 长度整个占用 4 个字节
// pub const LEN_LEN: usize = 4;
// /// 长度占 31 bit，所以最大的 frame 是 2G
// const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;
// /// 如果 payload 超过了 1436 字节，就做压缩
// const COMPRESSION_LIMIT: usize = 1436;
// /// 代表压缩的 bit（整个长度 4 字节的最高位）
// const COMPRESSION_BIT: usize = 1 << 31;

use crate::{CommandRequest, CommandResponse, KvError};

use bytes::{Buf, BufMut, BytesMut};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use prost::Message;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

/// 长度信息 => 4 bytes
const LEN_LEN: usize = 4;
/// length => 4 bytes => 32 位， 刨除符号位， 还剩31位 => 2 GiB
const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;
const COMPRESSION_LIMIT: usize = 1436;
/// a flag bit: whether this frame is compressed
const COMPRESSION_BIT: usize = 1 << 31;

/// 给 prost message struct 实现的 trait
pub trait FrameCodec
where
    Self: Message + Sized + Default,
{
    /// check self.encoded len
    /// if too long => raise error
    /// if over encode limit => do compression
    ///                      => get encoded payload
    ///                      => compress encoded payload
    ///                      => mark the flag bit
    ///                      => merge back into buf
    /// else => just encode & return
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        // prost encoded len => size
        let size = self.encoded_len();
        if size > MAX_FRAME {
            return Err(KvError::FrameError);
        }
        buf.put_u32(size as _);
        if size > COMPRESSION_LIMIT {
            let mut buf1 = Vec::with_capacity(size);
            self.encode(&mut buf1)?;
            let payload = buf.split_off(LEN_LEN);
            buf.clear();
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&buf1[..])?;
            let payload = encoder.finish()?.into_inner();
            debug!("encode a frame: size {} ({})", size, payload.len());
            buf.put_u32((payload.len() | COMPRESSION_BIT) as _);
            buf.unsplit(payload);
            Ok(())
        } else {
            self.encode(buf)?;
            Ok(())
        }
    }

    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_u32() as usize;
        let (len, compressed) = decode_header(header);
        debug!("got a frame: msg len: {}, compressed: {}", len, compressed);
        if compressed {
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut buf1)?;
            buf.advance(len);
            Ok(Self::decode(&buf1[..buf1.len()])?)
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}

impl FrameCodec for CommandRequest {}
impl FrameCodec for CommandResponse {}

/// 复习一下 buf 的操作
/// buf.reserve() => 给 capacity 扩容
/// buf.put_u32(n) => 把 n 塞到 buf 里
/// buf.advance_mut() => 扩 len
/// stream.read_exact(&mut buf) => AsyncReadExt => buf 有多长， 就从 stream 里读多少数据塞进去
pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send,
{
    let header = stream.read_u32().await? as usize;
    let (len, _compressed) = decode_header(header);
    // buf 留了 LEN_LEN + len
    // put_u32 消耗了 LEN_LEN
    // 后面用 advance_mut(len) 把剩下的 len位 也圈到 self.len() 里
    buf.reserve(LEN_LEN + len);
    buf.put_u32(header as _);
    // 我们后面用 read_exact 把 buf.len 塞满了， 所以这里是安全的
    unsafe { buf.advance_mut(len) };
    // read_exact => 把buf的slice塞满
    // buf 的 slice 有多长 => 截止到 buf.len()
    stream.read_exact(&mut buf[LEN_LEN..]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    /// 主要测什么
    /// - 双向 codec
    /// - compression 是否 work
    /// - read_frame 是否 work
    use super::*;
    use crate::Value;
    use bytes::Bytes;

    #[test]
    fn command_req_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hdel("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();
        assert!(!is_compressed(&buf));
        let cmd1 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, cmd1);
    }

    #[test]
    fn command_resp_codec_should_work() {
        let mut buf = BytesMut::new();
        let values: Vec<Value> = vec![1.into(), "hello".into(), b"data".into()];
        let res: CommandResponse = values.into();
        res.encode_frame(&mut buf).unwrap();
        assert!(!is_compressed(&buf));
        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1);
    }

    #[test]
    fn command_resp_compressed_codec_should_work() {
        let mut buf = BytesMut::new();
        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMIT + 1]).into();
        let res: CommandResponse = value.into();
        res.encode_frame(&mut buf).unwrap();
        assert!(is_compressed(&buf));
        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1);
    }

    struct DummyStream {
        buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        /// todo 写 doc
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data);
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hdel("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream { buf };
        let mut data = BytesMut::new();
        read_frame(&mut stream, &mut data).await.unwrap();
        let cmd1 = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, cmd1);
    }

    fn is_compressed(data: &[u8]) -> bool {
        // frame 的最高位是 compression_bit
        if let [v] = data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }
}
