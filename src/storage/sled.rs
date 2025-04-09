use crate::{KvError, Kvpair, Storage, StorageIter, Value};
use sled::{Db, IVec};
use std::{convert::TryInto, path::Path, str};

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }
    fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }
    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

impl Storage for SledDb {
    /// 从一个 HashTable 里获取一个 key 的 value
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, key);
        // 首先从 sled 里拿出来的是一个 Ivec， 即 Option<Ivec> => Option<u8 array>
        // Ivec.as_ref() => &[u8]
        // Option<&[u8]> => try into kvs Value => Option<Result<Value, KvError>>
        // let raw1 = self.0.get(name.as_bytes())?;
        // let raw2: Option<Result<Value, KvError>> = raw1.map(|v| v.as_ref().try_into());
        // flip(raw2);
        let result = self.0.get(name.as_bytes())?.map(|v| v.as_ref().try_into());
        flip(result)
    }
    /// 从一个 HashTable 里设置一个 key 的 value，返回旧的 value
    fn set(
        &self,
        table: &str,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Result<Option<Value>, KvError> {
        let key = key.into();
        let name = SledDb::get_full_key(table, &key);
        let data: Vec<u8> = value.into().try_into()?;
        let res = self.0.insert(name, data)?.map(|v| v.as_ref().try_into());
        flip(res)
    }

    /// 查看 HashTable 中是否有 key
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let name = SledDb::get_full_key(table, key);
        Ok(self.0.contains_key(name)?)
    }

    /// 从 HashTable 中删除一个 key
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, key);
        let res = self.0.remove(name)?.map(|v| v.as_ref().try_into());
        flip(res)
    }
    /// 遍历 HashTable，返回所有 kv pair（这个接口不好）
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let res: Vec<Kvpair> = self.0.scan_prefix(prefix).map(|v| v.into()).collect();
        Ok(res)
    }
    /// 遍历 HashTable，返回 kv pair 的 Iterator
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let iter = StorageIter::new(self.0.scan_prefix(prefix));
        Ok(Box::new(iter))
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(v: Result<(IVec, IVec), sled::Error>) -> Self {
        match v {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(k.as_ref()), v),
                Err(_) => Kvpair::default(),
            },
            _ => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    // key 的格式是 table:key， 所以改了之后要 split 一下
    let s = str::from_utf8(ivec).unwrap();
    let mut iter = s.split(':');
    iter.next();
    iter.next().unwrap()
}
