mod memory;
mod sled;

pub use memory::MemTable;
pub use sled::SledDb;

use crate::{KvError, Kvpair, Value};

/// 对存储的抽象，我们不关心数据存在哪儿，但需要定义外界如何和存储打交道
/// 层次结构 => table => key, value
/// get => table, key => value
/// set => table, k, v => old value
/// contains => table, k => result
/// del => table, k => old value
/// get_all => table => Vec<kv pair>
/// get_iter => table => Iter<Item=Kvpair>
pub trait Storage {
    /// 从一个 HashTable 里获取一个 key 的 value
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 从一个 HashTable 里设置一个 key 的 value，返回旧的 value
    fn set(
        &self,
        table: &str,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Result<Option<Value>, KvError>;
    /// 查看 HashTable 中是否有 key
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    /// 从 HashTable 中删除一个 key
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 遍历 HashTable，返回所有 kv pair（这个接口不好）
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    /// 遍历 HashTable，返回 kv pair 的 Iterator
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

/// 提供 Storage iterator，这样 trait 的实现者只需要
/// 把它们的 iterator 提供给 StorageIter，然后它们保证
/// next() 传出的类型实现了 Into<Kvpair> 即可
/// 给 iter 进行的适配， 把 iter<T> 转成 iter<Kvpair>
/// 主要服务于 get_iter
pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<Kvpair>,
{
    type Item = Kvpair;
    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}

// result option value => keyerror
#[cfg(test)]
mod tests {

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn memtable_should_work() {
        let store = MemTable::new();
        test_simple(store);
        let store = MemTable::new();
        test_get_all(store);
        let store = MemTable::new();
        test_get_iter(store);
    }

    #[test]
    fn sleddb_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_simple(store);
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_all(store);
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_iter(store);
    }

    fn test_simple(store: impl Storage) {
        // set
        let v = store.set("t1", "hello", "world");
        assert!(v.unwrap().is_none());
        let v = store.set("t1", "hello", "world1");
        assert_eq!(v, Ok(Some("world".into())));

        // get
        let v = store.get("t1", "hello");
        assert_eq!(v, Ok(Some("world1".into())));

        assert_eq!(Ok(None), store.get("t1", "hello1"));
        // assert_eq!(Ok(None), store.get("t2", "hello1"));
        assert!(store.get("t2", "hello1").unwrap().is_none());

        // contains
        assert_eq!(store.contains("t1", "hello"), Ok(true));
        assert_eq!(store.contains("t1", "hello1"), Ok(false));
        assert_eq!(store.contains("t2", "hello1"), Ok(false));

        // del
        let v = store.del("t1", "hello");
        assert_eq!(v, Ok(Some("world1".into())));

        assert_eq!(Ok(None), store.del("t1", "hello1"));
        assert_eq!(Ok(None), store.del("t2", "hello"));
    }

    fn test_get_all(store: impl Storage) {
        store.set("t1", "k1", "v1").unwrap();
        store.set("t1", "k2", "v2").unwrap();
        let mut res = store.get_all("t1").unwrap();
        res.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_eq!(
            res,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
            ]
        )
    }

    #[allow(dead_code)]
    fn test_get_iter(store: impl Storage) {
        store.set("t2", "k1", "v1").unwrap();
        store.set("t2", "k2", "v2").unwrap();

        let mut res: Vec<Kvpair> = store.get_iter("t2").unwrap().collect();
        res.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_eq!(
            res,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
            ],
        );
    }
}
