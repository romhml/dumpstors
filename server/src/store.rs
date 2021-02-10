use std::sync::{Arc, Mutex};

use rayon::prelude::*;
use tonic::{Request, Response, Status};

use dumpstors_lib::models::Record;
use dumpstors_lib::store::Store;
use dumpstors_lib::store::*;

use std::result::Result as StdResult;

pub struct DumpstorsStoreServer {
    store: Arc<Mutex<Store>>,
}

impl DumpstorsStoreServer {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl store_server::Store for DumpstorsStoreServer {
    async fn ping(&self, _request: Request<()>) -> StdResult<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_keyspaces(
        &self,
        request: Request<GetKeyspacesQuery>,
    ) -> StdResult<Response<GetKeyspacesResponse>, Status> {
        let mut _store = self.store.lock();
        let _request = request.into_inner();

        let reply = GetKeyspacesResponse { keyspaces: vec![] };

        Ok(Response::new(reply))
    }

    async fn create_keyspaces(
        &self,
        request: Request<CreateKeyspacesQuery>,
    ) -> StdResult<Response<CreateKeyspacesResponse>, Status> {
        let mut store = self.store.lock().unwrap();
        let request = request.into_inner();

        let errors = request
            .keyspaces
            .iter()
            .map(|ks| match store.create_keyspace(ks.clone()) {
                Ok(_) => None,
                Err(e) => Some(CreateKeyspaceError {
                    keyspace: ks.clone(),
                    reason: format!("{:?}", e),
                }),
            })
            .flatten()
            .collect();

        let reply = CreateKeyspacesResponse { errors };

        Ok(Response::new(reply))
    }

    async fn delete_keyspaces(
        &self,
        request: Request<DeleteKeyspacesQuery>,
    ) -> StdResult<Response<DeleteKeyspacesResponse>, Status> {
        let mut store = self.store.lock().unwrap();
        let request = request.into_inner();

        let errors = request
            .keyspaces
            .iter()
            .map(|ks| match store.create_keyspace(ks.clone()) {
                Ok(_) => None,
                Err(e) => Some(DeleteKeyspaceError {
                    keyspace: ks.clone(),
                    reason: format!("{:?}", e),
                }),
            })
            .flatten()
            .collect();

        let reply = DeleteKeyspacesResponse { errors: errors };

        Ok(Response::new(reply))
    }

    async fn get_keys(
        &self,
        request: Request<GetQuery>,
    ) -> StdResult<Response<GetResponse>, Status> {
        let mut store = self.store.lock().unwrap();

        let request = request.into_inner();
        let ks = store.get_keyspace(request.keyspace.clone()).unwrap();

        let records = request
            .keys
            .par_iter()
            .map(|k| {
                let value = ks.get(k.as_slice()).unwrap();
                Record {
                    key: k.clone(),
                    value: value,
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
    ) -> StdResult<Response<InsertResponse>, Status> {
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
                    reason: format!("{:?}", e),
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
    ) -> StdResult<Response<DeleteResponse>, Status> {
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
                    reason: format!("{:?}", e),
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
