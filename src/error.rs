use crate::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Not found for table: {0}, key: {1}")]
    NotFound(String, String),
    #[error("Frame is larger than max size")]
    FrameError,
    #[error("Command is invalid: `{0}`")]
    InvalidCommand(String),
    #[error("Cannot convert value {0:?} to {1}")]
    ConvertError(String, &'static str),
    #[error("Cannot process command {0} with table: {1}, key: {2}. Error: {3}")]
    StorageError(&'static str, String, String, String),

    #[error("Certificate parse error: error to load {0} {0}")]
    CertificateParseError(&'static str, &'static str),

    #[error("Failed to encode protobuf message")]
    EncodeError(#[from] prost::EncodeError),
    #[error("Failed to decode protobuf message")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Failed to access sled db")]
    SledError(#[from] sled::Error),
    #[error("I/O error")]
    IoError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("TLS error")]
    TlsError(#[from] tokio_rustls::rustls::TLSError),
}
