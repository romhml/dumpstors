use sled;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Keyspace {
    name: String,
    db: Arc<sled::Db>,
}

impl Keyspace {
    pub fn new(path: String, name: String) -> Self {
        Self {
            name: name.clone(),
            db: Arc::new(sled::open(format!("{}/{}", path, name)).unwrap()),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> sled::Result<Option<sled::IVec>> {
        self.db.get(key)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> sled::Result<()> {
        self.db.insert(key, value)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> sled::Result<()> {
        self.db.remove(key)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_key() {
        let mut ks = Keyspace::new(String::from(".data"), String::from("ks1"));
        ks.insert(b"foo", b"bar").unwrap();
        assert_eq!(ks.get(b"foo"), Ok(Some(sled::IVec::from(b"bar"))));
        assert_eq!(ks.get(b"bar"), Ok(None));
    }

    #[test]
    fn delete_key() {
        let mut ks = Keyspace::new(String::from(".data"), String::from("ks2"));
        ks.insert(b"foo", b"bar").unwrap();
        ks.delete(b"foo").unwrap();
        assert_eq!(ks.get(b"foo"), Ok(None));
    }
}
