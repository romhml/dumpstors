use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use dumpsters_lib::dumpsters_server::Dumpsters;
use dumpsters_lib::store::Store;
use dumpsters_lib::*;

pub struct StoreServer {
    store: Arc<Mutex<Store>>,
}

impl StoreServer {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }
}

#[tonic::async_trait]
impl Dumpsters for StoreServer {
    async fn ping(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_keys(&self, request: Request<GetQuery>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let mut store = self.store.lock().unwrap();

        let ks = store.get_keyspace(request.keyspace.clone()).unwrap();

        let records = request
            .keys
            .iter()
            .map(|k| {
                let value = ks.get(k.as_slice()).unwrap().unwrap();
                Record {
                    key: k.clone(),
                    value: value.to_vec(),
                }
            })
            .collect();

        let reply = GetResponse {
            keyspace: request.keyspace,
            records,
        };

        Ok(Response::new(reply))
    }

    async fn insert_keys(
        &self,
        request: Request<InsertQuery>,
    ) -> Result<Response<InsertResponse>, Status> {
        let request = request.into_inner();
        let mut store = self.store.lock().unwrap();

        let ks = store.get_keyspace(request.keyspace.clone()).unwrap();

        let errors = request
            .records
            .iter()
            .map(|r| match ks.insert(r.key.as_slice(), r.value.as_slice()) {
                Ok(_) => None,
                Err(e) => Some(InsertError {
                    key: r.key.clone(),
                    reason: format!("{}", e),
                }),
            })
            .flatten()
            .collect();

        let reply = InsertResponse {
            keyspace: request.keyspace,
            errors,
        };

        Ok(Response::new(reply))
    }

    async fn delete_keys(
        &self,
        request: Request<DeleteQuery>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let mut store = self.store.lock().unwrap();

        let ks = store.get_keyspace(request.keyspace.clone()).unwrap();

        let errors = request
            .keys
            .iter()
            .map(|k| match ks.delete(k.as_slice()) {
                Ok(_) => None,
                Err(e) => Some(DeleteError {
                    key: k.clone(),
                    reason: format!("{}", e),
                }),
            })
            .flatten()
            .collect();

        let reply = DeleteResponse {
            keyspace: request.keyspace,
            errors,
        };

        Ok(Response::new(reply))
    }
}
