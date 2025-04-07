use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}
// gotta impl resp From Error - done

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(pair) => match store.set(&self.table, pair.key, pair.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => KvError::InvalidCommand(format!("{:?}", self)).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hset() {
        let store = MemTable::new();
        // let svc = Service::new(store);
        let req = CommandRequest::new_hset("t1", "k1", "v1".into());
        // too nasty to manually unwrap the command request
        // I need a func => dispatch
        let resp = dispatch(req.clone(), &store);
        // too nasty to manually assert
        // I need a func => assert_res_ok
        assert_res_ok(resp, &[Value::default()], &[]);
        let resp = dispatch(req, &store);
        assert_res_ok(resp, &["v1".into()], &[]);
    }

    #[test]
    fn test_hget_error_cond() {
        let store = MemTable::new();
        // set
        let req = CommandRequest::new_hget("t1", "k1");
        let resp = dispatch(req, &store);
        assert_res_error(resp, 404, "Not found")
    }

    #[test]
    fn test_hget() {
        let store = MemTable::new();
        // set
        let req = CommandRequest::new_hset("t1", "k1", "v1".into());
        let resp = dispatch(req.clone(), &store);
        assert_res_ok(resp, &[Value::default()], &[]);
        // get
        let req = CommandRequest::new_hget("t1", "k1");
        let resp = dispatch(req.clone(), &store);
        assert_res_ok(resp, &["v1".into()], &[]);
    }

    #[test]
    fn test_hgetall() {
        let store = MemTable::new();
        let req = CommandRequest::new_hset("t1", "k1", "v1".into());
        dispatch(req.clone(), &store);
        let req = CommandRequest::new_hset("t1", "k2", "v2".into());
        dispatch(req.clone(), &store);
        let req = CommandRequest::new_hgetall("t1");
        let resp = dispatch(req.clone(), &store);
        assert_res_ok(
            resp,
            &[],
            &[
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
            ],
        )
    }
}
