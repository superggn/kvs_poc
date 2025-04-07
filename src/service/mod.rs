use crate::{
    CommandRequest, CommandResponse, KvError, Kvpair, MemTable, Storage, Value,
    command_request::RequestData,
};
use std::sync::Arc;
use tracing::debug;

mod command_service;

/// Command behavior
pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data within".into()).into(),
        _ => KvError::Internal("Not implemented".into()).into(),
    }
}

#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}

#[derive(Clone)]
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

pub struct ServiceInner<Store> {
    store: Store,
}

impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner { store }),
        }
    }
    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("got request: {:?}", cmd);
        // todo: on_received
        let resp = dispatch(cmd, &self.inner.store);
        // todo: on_executed
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemTable, Value};
    use std::thread;

    #[test]
    fn test_service() {
        let service = Service::new(MemTable::default());
        // service's clone => light
        let svc_clone = service.clone();
        // why threads?
        // => service should be a cross-threads instance
        let handle = thread::spawn(move || {
            let res = svc_clone.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });
        handle.join().unwrap();
        // cur thread => v1 should be valid
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }
}
