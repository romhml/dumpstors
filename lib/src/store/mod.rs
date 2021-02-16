tonic::include_proto!("dumpstors.store");
pub mod keyspace;

use sled::Error as SledError;
use std::collections::HashMap;
use std::fs;
use std::io::Error as IoError;
use std::result::Result as StdResult;
use tonic::{Code, Status};

use super::models;
use keyspace::Keyspace;

#[derive(Debug)]
pub enum Error {
    SledErr(SledError),
    IoErr(IoError),

    KeyspaceNotFound,
    KeyspaceAlreadyExists,
    KeyNotFound,
}

impl From<SledError> for Error {
    fn from(err: SledError) -> Self {
        Error::SledErr(err)
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Self {
        Error::IoErr(err)
    }
}

pub type Result<T> = StdResult<T, Error>;

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        match err {
            Error::KeyspaceNotFound => Self::new(Code::NotFound, "Keyspace not found"),
            Error::KeyspaceAlreadyExists => {
                Self::new(Code::AlreadyExists, "Keyspace already exists")
            }
            Error::KeyNotFound => Self::new(Code::NotFound, "Key not found"),
            _ => Self::new(Code::Internal, "Internal Error"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Store {
    keyspaces: HashMap<String, Keyspace>,
    path: String,
}

impl Store {
    fn load_keyspaces(path: String) -> Vec<Result<Keyspace>> {
        fs::create_dir_all(path.clone()).unwrap();
        let dir = fs::read_dir(path).unwrap();

        dir.map(|file| {
            let file = file?;
            let path = file.path().into_os_string().into_string().unwrap();
            let name = file.file_name().into_string().unwrap();

            Keyspace::new(path, name)
        })
        .collect()
    }

    pub fn new(path: String) -> Self {
        let keyspaces = Self::load_keyspaces(path.clone())
            .into_iter()
            .map(|ks| match ks {
                Ok(ks) => Some((ks.name.clone(), ks)),
                Err(e) => {
                    println!("{:?}", e);
                    None
                }
            })
            .flatten()
            .collect();

        Self { keyspaces, path }
    }

    pub fn create_keyspace(&mut self, ks: models::Keyspace) -> Result<()> {
        if self.get_keyspace(ks.name.clone()).is_ok() {
            Err(Error::KeyspaceAlreadyExists)
        } else {
            let keyspace = Keyspace::new(self.path.clone(), ks.name.clone())?;
            self.keyspaces.insert(ks.name, keyspace);
            Ok(())
        }
    }

    pub fn get_keyspace(&mut self, ks: String) -> Result<&mut Keyspace> {
        match self.keyspaces.get_mut(&ks) {
            Some(k) => Ok(k),
            None => Err(Error::KeyspaceNotFound),
        }
    }

    pub fn delete_keyspace(&mut self, ks: String) -> Result<()> {
        match self.keyspaces.remove(&ks) {
            Some(_) => {
                std::fs::remove_dir_all(format!("{}/{}", self.path, ks))?;
                Ok(())
            }
            None => Err(Error::KeyspaceNotFound),
        }
    }

    pub fn truncate_keyspace(&mut self, ks: String) -> Result<()> {
        match self.keyspaces.get_mut(&ks) {
            Some(ks) => {
                ks.truncate()?;
                Ok(())
            }
            None => Err(Error::KeyspaceNotFound),
        }
    }

    pub fn list_keyspaces(&mut self) -> Result<Vec<models::Keyspace>> {
        Ok(self
            .keyspaces
            .values()
            .cloned()
            .map(|ks| ks.into())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn create_random_store() -> Store {
        Store::new(format!(".data/{}", Uuid::new_v4()))
    }

    #[test]
    fn load_keyspaces_works() {
        let mut store = create_random_store();
        let ks1 = models::Keyspace {
            name: String::from("ks1"),
        };
        let ks2 = models::Keyspace {
            name: String::from("ks2"),
        };
        store.create_keyspace(ks1).unwrap();
        store.create_keyspace(ks2.clone()).unwrap();
        store.delete_keyspace(ks2.name).unwrap();

        let store_bis = Store::new(store.path.clone());

        assert_eq!(
            store.keyspaces.keys().cloned().collect::<Vec<String>>(),
            store_bis.keyspaces.keys().cloned().collect::<Vec<String>>()
        )
    }

    #[test]
    fn get_keyspace() {
        let mut store = create_random_store();
        let ks1 = models::Keyspace {
            name: String::from("ks1"),
        };

        store.create_keyspace(ks1.clone()).unwrap();
        let res = store.get_keyspace(ks1.name.clone()).unwrap();

        assert_eq!(ks1, models::Keyspace::from(res.clone()));
    }

    #[test]
    fn get_inexistant_keyspace() {
        let mut store = create_random_store();
        let ks1 = models::Keyspace {
            name: String::from("ks1"),
        };

        match store.get_keyspace(ks1.name) {
            Err(Error::KeyspaceNotFound) => assert!(true),
            _ => assert!(false, "Keyspace should not exist"),
        };
    }

    #[test]
    fn create_existing_keyspace() {
        let mut store = create_random_store();
        let ks1 = models::Keyspace {
            name: String::from("ks1"),
        };

        store.create_keyspace(ks1.clone()).unwrap();

        match store.create_keyspace(ks1.clone()) {
            Err(Error::KeyspaceAlreadyExists) => assert!(true),
            _ => assert!(false, "Keyspace should already exist"),
        };
    }

    #[test]
    fn delete_keyspace() {
        let mut store = create_random_store();
        let ks1 = models::Keyspace {
            name: String::from("ks1"),
        };

        store.create_keyspace(ks1.clone()).unwrap();
        store.delete_keyspace(ks1.name.clone()).unwrap();

        match store.get_keyspace(ks1.name.clone()) {
            Err(Error::KeyspaceNotFound) => assert!(true),
            _ => assert!(false, "Keyspace should not exist after delete"),
        };
    }

    #[test]
    fn truncate_keyspace() {
        let mut store = create_random_store();
        let ks1 = models::Keyspace {
            name: String::from("ks1"),
        };

        store.create_keyspace(ks1.clone()).unwrap();
        store.truncate_keyspace(ks1.name.clone()).unwrap();

        let res = store.get_keyspace(ks1.name.clone()).unwrap();
        assert_eq!(ks1, models::Keyspace::from(res.clone()));
    }

    #[test]
    fn list_keyspaces() {
        let mut store = create_random_store();
        let keyspaces = vec![
            models::Keyspace {
                name: String::from("ks1"),
            },
            models::Keyspace {
                name: String::from("ks2"),
            },
            models::Keyspace {
                name: String::from("ks3"),
            },
        ];

        keyspaces
            .clone()
            .into_iter()
            .for_each(|ks| store.create_keyspace(ks).unwrap());
        assert_eq!(
            keyspaces.clone().sort_by(|a, b| a.name.cmp(&b.name)),
            store
                .list_keyspaces()
                .unwrap()
                .sort_by(|a, b| a.name.cmp(&b.name))
        )
    }
}
