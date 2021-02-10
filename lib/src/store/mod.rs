tonic::include_proto!("dumpstors.store");
pub mod keyspace;

use std::collections::HashMap;
use std::fs;
use std::io::Error as IoError;
use std::result::Result as StdResult;

use sled::Error as SledError;

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

    pub fn create_keyspace(&mut self, ks: String) -> Result<()> {
        if let Ok(_) = self.get_keyspace(ks.clone()) {
            Err(Error::KeyspaceAlreadyExists)
        } else {
            let keyspace = Keyspace::new(self.path.clone(), ks.clone())?;
            self.keyspaces.insert(ks.clone(), keyspace);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn create_random_store() -> Store {
        Store::new(String::from(format!(".data/{}", Uuid::new_v4())))
    }

    #[test]
    fn load_keyspaces_works() {
        let mut store = create_random_store();
        store.create_keyspace(String::from("ks1")).unwrap();
        store.create_keyspace(String::from("ks2")).unwrap();
        store.delete_keyspace(String::from("ks2")).unwrap();

        let mut store_bis = Store::new(store.path.clone());

        assert_eq!(
            store.keyspaces.keys().cloned().collect::<Vec<String>>(),
            store_bis.keyspaces.keys().cloned().collect::<Vec<String>>()
        )
    }

    #[test]
    fn get_keyspace() {
        let mut store = create_random_store();
        let ks_name = String::from("ks");

        store.create_keyspace(ks_name.clone()).unwrap();
        store.get_keyspace(ks_name.clone()).unwrap();
    }

    #[test]
    fn get_inexistant_keyspace() {
        let mut store = create_random_store();
        let ks_name = String::from("ks");

        match store.get_keyspace(ks_name.clone()) {
            Err(Error::KeyspaceNotFound) => assert!(true),
            _ => assert!(false, "Keyspace should not exist"),
        };
    }

    #[test]
    fn create_existing_keyspace() {
        let mut store = create_random_store();
        let ks_name = String::from("ks");

        store.create_keyspace(ks_name.clone()).unwrap();

        match store.create_keyspace(ks_name.clone()) {
            Err(Error::KeyspaceAlreadyExists) => assert!(true),
            _ => assert!(false, "Keyspace should already exist"),
        };
    }

    #[test]
    fn delete_keyspace() {
        let mut store = create_random_store();
        let ks_name = String::from("ks");

        store.create_keyspace(ks_name.clone()).unwrap();
        store.delete_keyspace(ks_name.clone()).unwrap();

        match store.get_keyspace(ks_name.clone()) {
            Err(Error::KeyspaceNotFound) => assert!(true),
            _ => assert!(false, "Keyspace should not exist after delete"),
        };
    }
}
