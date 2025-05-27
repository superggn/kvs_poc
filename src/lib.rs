pub mod error;
pub mod network;
pub mod pb;
pub mod service;
pub mod storage;

pub use error::KvError;
pub use network::*;
pub use pb::abi::*;
pub use service::*;
pub use storage::*;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
