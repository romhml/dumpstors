pub mod keyspace;

use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use dumpstors_lib::store::Store;
use dumpstors_lib::store::*;

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
    async fn ping(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_keyspaces(
        &self,
        request: Request<GetKeyspacesQuery>,
    ) -> Result<Response<GetKeyspacesResponse>, Status> {
        let mut _store = self.store.lock();
        let _request = request.into_inner();

        let reply = GetKeyspacesResponse { keyspaces: vec![] };

        Ok(Response::new(reply))
    }

    async fn create_keyspaces(
        &self,
        request: Request<CreateKeyspacesQuery>,
    ) -> Result<Response<CreateKeyspacesResponse>, Status> {
        let mut store = self.store.lock().unwrap();
        let request = request.into_inner();

        let errors = request
            .keyspaces
            .iter()
            .map(|ks| match store.create_keyspace(ks.clone()) {
                Some(_) => None,
                None => Some(CreateKeyspaceError {
                    keyspace: ks.clone(),
                    reason: format!("Could not create keyspace"),
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
    ) -> Result<Response<DeleteKeyspacesResponse>, Status> {
        let mut store = self.store.lock().unwrap();
        let request = request.into_inner();

        let errors = request
            .keyspaces
            .iter()
            .map(|ks| match store.create_keyspace(ks.clone()) {
                Some(_) => None,
                None => Some(DeleteKeyspaceError {
                    keyspace: ks.clone(),
                    reason: format!("Could not delete keyspace"),
                }),
            })
            .flatten()
            .collect();

        let reply = DeleteKeyspacesResponse { errors: errors };

        Ok(Response::new(reply))
    }
}
