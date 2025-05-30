use crate::{
    CommandRequest, CommandResponse, KvError, MemTable, Storage, command_request::RequestData,
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
use crate::{Kvpair, Value};

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

// #[derive(Clone)]
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

pub struct ServiceInner<Store> {
    store: Store,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl<Store: Storage> ServiceInner<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: Vec::new(),
            on_executed: Vec::new(),
            on_before_send: Vec::new(),
            on_after_send: Vec::new(),
        }
    }

    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }

    pub fn fn_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }

    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }

    pub fn fn_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
    }
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<Store: Storage> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// 添加 notify

/// immutable arg
pub trait Notify<Arg> {
    fn notify(&self, arg: &Arg);
}

/// mutable arg
pub trait NotifyMut<Arg> {
    fn notify(&self, arg: &mut Arg);
}

impl<Arg> Notify<Arg> for Vec<fn(&Arg)> {
    #[inline]
    fn notify(&self, arg: &Arg) {
        for f in self {
            f(arg)
        }
    }
}

impl<Arg> NotifyMut<Arg> for Vec<fn(&mut Arg)> {
    #[inline]
    fn notify(&self, arg: &mut Arg) {
        for f in self {
            f(arg)
        }
    }
}

impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner {
                store: store,
                on_received: Vec::new(),
                on_executed: Vec::new(),
                on_before_send: Vec::new(),
                on_after_send: Vec::new(),
            }),
        }
    }
    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("got request: {:?}", cmd);
        // todo: on_received
        self.inner.on_received.notify(&cmd);
        let mut resp = dispatch(cmd, &self.inner.store);
        // todo: on_executed
        self.inner.on_executed.notify(&resp);
        self.inner.on_before_send.notify(&mut resp);

        if !self.inner.on_before_send.is_empty() {
            debug!("Modified Response: {:?}", resp)
        }

        resp
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use http::StatusCode;
    use tracing::info;

    use super::*;
    use crate::{MemTable, Value};

    #[test]
    fn service_should_work() {
        // let service = Service::new(MemTable::default());
        let service: Service = ServiceInner::new(MemTable::default()).into();
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

    #[test]
    fn event_reg_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Got {:?}", cmd);
        }
        fn c(resp: &CommandResponse) {
            info!("{:?}", resp);
        }
        fn d(resp: &mut CommandResponse) {
            resp.status = StatusCode::CREATED.as_u16() as _;
        }
        fn e() {
            info!("Data is sent");
        }

        // tracing_subscriber::fmt::init();
        let service: Service = ServiceInner::new(MemTable::new())
            .fn_received(|_: &CommandRequest| {})
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e)
            .into();
        let resp = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        assert_eq!(resp.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(resp.message, "");
        assert_eq!(resp.values, vec![Value::default()]);
    }
}
