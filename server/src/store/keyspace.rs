use super::{Error, Result};
use dumpstors_lib::models;
use std::iter::Iterator;
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

    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        match self.db.get(key)? {
            Some(v) => Ok(v.to_vec()),
            None => Err(Error::KeyNotFound),
        }
    }

    pub fn insert(&mut self, record: models::Record) -> Result<()> {
        self.db.insert(record.key, record.value)?;
        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        match self.db.remove(key)? {
            Some(_) => Ok(()),
            None => Err(Error::KeyNotFound),
        }
    }

    pub fn batch_insert(&mut self, records: Vec<models::Record>) -> Result<()> {
        let mut batch = sled::Batch::default();
        records
            .into_iter()
            .for_each(|r| batch.insert(r.key, r.value));

        self.db.apply_batch(batch)?;
        Ok(())
    }

    pub fn batch_delete(&mut self, keys: Vec<Vec<u8>>) -> Result<()> {
        let mut batch = sled::Batch::default();
        keys.into_iter().for_each(|k| batch.remove(k));

        self.db.apply_batch(batch)?;
        Ok(())
    }

    pub fn truncate(&mut self) -> Result<()> {
        self.db.clear()?;
        Ok(())
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
        ks.insert(models::Record {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
        })
        .unwrap();
        assert_eq!(ks.get(b"foo".to_vec()).unwrap(), b"bar".to_vec());
    }

    #[test]
    fn get_inexistant_key() {
        let ks = create_random_keyspace();
        match ks.get(b"foo".to_vec()) {
            Err(Error::KeyNotFound) => assert!(true),
            _ => assert!(false, "Key should not exist"),
        };
    }

    #[test]
    fn insert_existing_key() {
        let mut ks = create_random_keyspace();
        ks.insert(models::Record {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
        })
        .unwrap();
        assert_eq!(ks.get(b"foo".to_vec()).unwrap(), b"bar".to_vec());

        ks.insert(models::Record {
            key: b"foo".to_vec(),
            value: b"dar".to_vec(),
        })
        .unwrap();
        assert_eq!(ks.get(b"foo".to_vec()).unwrap(), b"dar".to_vec());
    }

    #[test]
    fn delete_key() {
        let mut ks = create_random_keyspace();
        ks.insert(models::Record {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
        })
        .unwrap();
        ks.delete(b"foo".to_vec()).unwrap();

        match ks.get(b"foo".to_vec()) {
            Err(Error::KeyNotFound) => assert!(true),
            _ => assert!(false, "Key should not exist after being deleted"),
        };
    }

    #[test]
    fn batch_insert_key() {
        let mut ks = create_random_keyspace();
        let records = vec![
            models::Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            models::Record {
                key: b"doo".to_vec(),
                value: b"dar".to_vec(),
            },
            models::Record {
                key: b"boo".to_vec(),
                value: b"far".to_vec(),
            },
            models::Record {
                key: b"qoo".to_vec(),
                value: b"qar".to_vec(),
            },
        ];
        ks.batch_insert(records.clone()).unwrap();

        records
            .into_iter()
            .for_each(|r| assert_eq!(ks.get(r.key).unwrap(), r.value));
    }

    #[test]
    fn batch_delete_key() {
        let mut ks = create_random_keyspace();
        let records = vec![
            models::Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            models::Record {
                key: b"doo".to_vec(),
                value: b"dar".to_vec(),
            },
            models::Record {
                key: b"boo".to_vec(),
                value: b"far".to_vec(),
            },
            models::Record {
                key: b"qoo".to_vec(),
                value: b"qar".to_vec(),
            },
        ];
        ks.batch_insert(records.clone()).unwrap();
        ks.batch_delete(records.clone().into_iter().map(|r| r.key).collect())
            .unwrap();

        records.into_iter().for_each(|r| {
            match ks.get(r.key) {
                Err(Error::KeyNotFound) => assert!(true),
                _ => assert!(false, "Key should not exist after being deleted"),
            };
        });
    }

    #[test]
    fn truncate_test() {
        let mut ks = create_random_keyspace();
        let records = vec![
            models::Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            models::Record {
                key: b"doo".to_vec(),
                value: b"dar".to_vec(),
            },
            models::Record {
                key: b"boo".to_vec(),
                value: b"far".to_vec(),
            },
            models::Record {
                key: b"qoo".to_vec(),
                value: b"qar".to_vec(),
            },
        ];
        ks.batch_insert(records.clone()).unwrap();
        ks.truncate().unwrap();

        records.into_iter().for_each(|r| {
            match ks.get(r.key) {
                Err(Error::KeyNotFound) => assert!(true),
                _ => assert!(false, "Key should not exist after being deleted"),
            };
        });
    }
}
