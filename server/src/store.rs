use std::sync::{Arc, Mutex};

use rayon::prelude::*;
use tonic::{Request, Response, Status};

use dumpstors_lib::models::*;
use dumpstors_lib::store::store_server;
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
        let mut store = self.store.lock().unwrap();
        let request = request.into_inner();

        let keyspaces = request
            .keyspaces
            .into_iter()
            .map(|ks| match store.get_keyspace(ks) {
                Ok(k) => Some(Keyspace::from(k.clone())),
                // TODO: Find a way to efficiently return errors
                Err(_e) => None,
            })
            .flatten()
            .collect();

        let reply = GetKeyspacesResponse {
            keyspaces,
            errors: vec![],
        };

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
                    keyspace: Some(ks.clone()),
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
            .map(|ks| match store.delete_keyspace(ks.clone()) {
                Ok(_) => None,
                Err(e) => Some(DeleteKeyspaceError {
                    keyspace: ks.clone(),
                    reason: format!("{:?}", e),
                }),
            })
            .flatten()
            .collect();

        let reply = DeleteKeyspacesResponse { errors };

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
                match ks.get(k.as_slice()) {
                    Ok(value) => Some(Record {
                        key: k.clone(),
                        value,
                    }),
                    // TODO: Find a way to efficiently return errors
                    Err(_e) => None,
                }
            })
            .flatten()
            .collect();

        let reply = GetResponse {
            keyspace: request.keyspace,
            records,
            errors: vec![],
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use dumpstors_lib::store::store_server::Store as StoreServer;
    use std::sync::{Arc, Mutex};

    async fn create_random_store_server() -> DumpstorsStoreServer {
        let store = Arc::new(Mutex::new(Store::new(format!(".data/{}", Uuid::new_v4()))));
        super::DumpstorsStoreServer::new(store)
    }

    #[tokio::test]
    async fn store_server_ping_test() {
        let srv: DumpstorsStoreServer = create_random_store_server().await;
        assert_eq!(srv.ping(Request::new(())).await.unwrap().into_inner(), ());
    }

    #[tokio::test]
    async fn store_server_keyspace_test() {
        let srv: DumpstorsStoreServer = create_random_store_server().await;

        let keyspaces = vec![
            Keyspace {
                name: String::from("ks1").clone(),
            },
            Keyspace {
                name: String::from("ks2").clone(),
            },
        ];

        let keyspaces_name: Vec<String> = keyspaces.clone().into_iter().map(|k| k.name).collect();

        let resp = srv
            .create_keyspaces(Request::new(CreateKeyspacesQuery {
                keyspaces: keyspaces.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp, CreateKeyspacesResponse { errors: vec![] });

        let resp = srv
            .get_keyspaces(Request::new(GetKeyspacesQuery {
                keyspaces: keyspaces_name.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            GetKeyspacesResponse {
                keyspaces: keyspaces.clone(),
                errors: vec![]
            }
        );

        let resp = srv
            .delete_keyspaces(Request::new(DeleteKeyspacesQuery {
                keyspaces: vec![String::from("ks1")],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp, DeleteKeyspacesResponse { errors: vec![] });

        let resp = srv
            .get_keyspaces(Request::new(GetKeyspacesQuery {
                keyspaces: keyspaces_name.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            GetKeyspacesResponse {
                keyspaces: keyspaces[1..2].to_vec(),
                errors: vec![]
            }
        );
    }

    #[tokio::test]
    async fn store_server_create_existing_keyspace() {
        let srv: DumpstorsStoreServer = create_random_store_server().await;

        let keyspaces = vec![
            Keyspace {
                name: String::from("ks1").clone(),
            },
            Keyspace {
                name: String::from("ks2").clone(),
            },
        ];

        let keyspaces_name: Vec<String> = keyspaces.clone().into_iter().map(|k| k.name).collect();

        let resp = srv
            .create_keyspaces(Request::new(CreateKeyspacesQuery {
                keyspaces: keyspaces[0..1].to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp, CreateKeyspacesResponse { errors: vec![] });

        let resp = srv
            .create_keyspaces(Request::new(CreateKeyspacesQuery {
                keyspaces: keyspaces.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            CreateKeyspacesResponse {
                errors: vec![CreateKeyspaceError {
                    keyspace: Some(Keyspace {
                        name: String::from("ks1")
                    }),
                    reason: String::from("KeyspaceAlreadyExists")
                }]
            }
        );

        let resp = srv
            .get_keyspaces(Request::new(GetKeyspacesQuery {
                keyspaces: keyspaces_name.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            GetKeyspacesResponse {
                keyspaces: keyspaces.clone(),
                errors: vec![]
            }
        );
    }

    #[tokio::test]
    async fn store_server_key_operations_test() {
        let srv: DumpstorsStoreServer = create_random_store_server().await;
        let ks1 = Keyspace {
            name: String::from("ks1").clone(),
        };

        srv.create_keyspaces(Request::new(CreateKeyspacesQuery {
            keyspaces: vec![ks1.clone()],
        }))
        .await
        .unwrap()
        .into_inner();

        let records = vec![
            Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            Record {
                key: b"doo".to_vec(),
                value: b"doo doo".to_vec(),
            },
            Record {
                key: b"daa".to_vec(),
                value: b"daa daa".to_vec(),
            },
        ];

        let resp = srv
            .insert_keys(Request::new(InsertQuery {
                keyspace: ks1.name.clone(),
                records: records.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            InsertResponse {
                keyspace: ks1.name.clone(),
                errors: vec![]
            }
        );

        let resp = srv
            .get_keys(Request::new(GetQuery {
                keyspace: ks1.name.clone(),
                keys: vec![b"foo".to_vec(), b"doo".to_vec(), b"daa".to_vec()],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            GetResponse {
                keyspace: ks1.name.clone(),
                records: records.clone(),
                errors: vec![]
            }
        );

        let resp = srv
            .delete_keys(Request::new(DeleteQuery {
                keyspace: ks1.name.clone(),
                keys: vec![b"foo".to_vec()],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            DeleteResponse {
                keyspace: ks1.name.clone(),
                errors: vec![]
            }
        );

        let resp = srv
            .get_keys(Request::new(GetQuery {
                keyspace: ks1.name.clone(),
                keys: vec![b"doo".to_vec(), b"daa".to_vec()],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp,
            GetResponse {
                keyspace: ks1.name.clone(),
                records: records[1..3].to_vec(),
                errors: vec![]
            }
        );
    }
}
