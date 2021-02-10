use super::Error;
use super::Result;
use sled;

use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Keyspace {
    pub name: String,
    db: Arc<sled::Db>,
}

impl Keyspace {
    pub fn new(path: String, name: String) -> Result<Self> {
        Ok(Self {
            name: name.clone(),
            db: Arc::new(sled::open(format!("{}/{}", path, name))?),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<sled::IVec>> {
        match self.db.get(key) {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::SledErr(e)),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key, value)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_key() {
        let mut ks = Keyspace::new(String::from(".data"), String::from("ks1")).unwrap();
        ks.insert(b"foo", b"bar").unwrap();
        assert_eq!(ks.get(b"foo").unwrap(), Some(sled::IVec::from(b"bar")));
        assert_eq!(ks.get(b"bar").unwrap(), None);
    }

    #[test]
    fn delete_key() {
        let mut ks = Keyspace::new(String::from(".data"), String::from("ks2")).unwrap();
        ks.insert(b"foo", b"bar").unwrap();
        ks.delete(b"foo").unwrap();
        assert_eq!(ks.get(b"foo").unwrap(), None);
    }
}
