use super::Error;
use super::Result;

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

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        match self.db.get(key)? {
            Some(v) => Ok(v.to_vec()),
            None => Err(Error::KeyNotFound),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key, value)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        match self.db.remove(key)? {
            Some(_) => Ok(()),
            None => Err(Error::KeyNotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn create_random_keyspace() -> Keyspace {
        let path = format!(".data/{}", Uuid::new_v4());
        Keyspace::new(path, String::from("ks")).unwrap()
    }

    #[test]
    fn get_key() {
        let mut ks = create_random_keyspace();
        ks.insert(b"foo", b"bar").unwrap();
        assert_eq!(ks.get(b"foo").unwrap(), b"bar");
    }

    #[test]
    fn get_inexistant_key() {
        let ks = create_random_keyspace();
        match ks.get(b"foo") {
            Err(Error::KeyNotFound) => assert!(true),
            _ => assert!(false, "Key should not exist"),
        };
    }

    #[test]
    fn insert_existing_key() {
        let mut ks = create_random_keyspace();
        ks.insert(b"foo", b"bar").unwrap();
        assert_eq!(ks.get(b"foo").unwrap(), b"bar");

        ks.insert(b"foo", b"dar").unwrap();
        assert_eq!(ks.get(b"foo").unwrap(), b"dar");
    }

    #[test]
    fn delete_key() {
        let mut ks = create_random_keyspace();
        ks.insert(b"foo", b"bar").unwrap();
        ks.delete(b"foo").unwrap();

        match ks.get(b"foo") {
            Err(Error::KeyNotFound) => assert!(true),
            _ => assert!(false, "Key should not exist after being deleted"),
        };
    }
}
