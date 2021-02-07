mod keyspace;

use std::collections::HashMap;
use std::fs::read_dir;

use keyspace::Keyspace;

#[derive(Debug, Clone)]
pub struct Store {
    keyspaces: HashMap<String, Keyspace>,
    path: String,
}

impl Store {
    fn load_keyspaces(path: String) -> HashMap<String, Keyspace> {
        read_dir(path)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                let path = e.path().into_os_string().into_string().unwrap();
                let name = e.file_name().into_string().unwrap();

                (name.clone(), Keyspace::new(path, name))
            })
            .collect()
    }

    pub fn new(path: String) -> Self {
        Self {
            keyspaces: Self::load_keyspaces(path.clone()),
            path,
        }
    }

    pub fn create_keyspace(&mut self, ks: String) -> Option<&mut Keyspace> {
        let keyspace = Keyspace::new(self.path.clone(), ks.clone());
        self.keyspaces.insert(ks.clone(), keyspace);
        self.get_keyspace(ks)
    }

    pub fn get_keyspace(&mut self, ks: String) -> Option<&mut Keyspace> {
        self.keyspaces.get_mut(&ks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_keyspace() {
        let mut store = Store::new(String::from(".data"));
        let ks_name = String::from("ks3");

        let ks = store.create_keyspace(ks_name.clone()).unwrap();
        ks.insert(b"foo", b"bar").unwrap();

        let ks = store.get_keyspace(ks_name).unwrap();
        assert_eq!(ks.get(b"foo"), Ok(Some(sled::IVec::from(b"bar"))));
    }
}
