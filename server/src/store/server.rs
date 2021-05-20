use futures::Stream;
use rand::Rng;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use super::Store;
use dumpstors_lib::models;
use dumpstors_lib::store::store_server;
use dumpstors_lib::store::*;

use raft::prelude::{Config as RaftConfig, RawNode as RaftRawNode};
use raft::storage::MemStorage;

use std::result::Result as StdResult;

pub struct DumpstorsStoreServer {
    store: Arc<Mutex<Store>>,
    node: Arc<Mutex<RaftRawNode<MemStorage>>>,
    peers: Arc<Mutex<HashMap<String, models::Node>>>,
}

impl DumpstorsStoreServer {
    fn get_store_guard(&self) -> StdResult<MutexGuard<Store>, Status> {
        match self.store.lock() {
            Ok(store) => Ok(store),
            // TODO: Find proper way to shutdown Tokio
            Err(e) => panic!("{:?}\nPoisonError on store Mutex. Shutting down.", e),
        }
    }

    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        // TODO: Implement a metadata store to persist the node ID on disk
        let mut rng = rand::thread_rng();
        let cfg = RaftConfig {
            // The unique ID for the Raft node.
            id: rng.gen(),
            // The Raft node list.
            // Mostly, the peers need to be saved in the storage
            // and we can get them from the Storage::initial_state function, so here
            // you need to set it empty.
            peers: vec![1],
            // Election tick is for how long the follower may campaign again after
            // it doesn't receive any message from the leader.
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            ..Default::default()
        };

        let node = RaftRawNode::new(&cfg, MemStorage::new(), vec![]).unwrap();

        Self {
            store,
            node: Arc::new(Mutex::new(node)),
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl store_server::Store for DumpstorsStoreServer {
    async fn ping(&self, _request: Request<()>) -> StdResult<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_keyspace(
        &self,
        request: Request<GetKeyspaceQuery>,
    ) -> StdResult<Response<models::Keyspace>, Status> {
        let mut store = self.get_store_guard()?;
        let request = request.into_inner();

        let ks = store.get_keyspace(request.keyspace)?;
        Ok(Response::new(models::Keyspace::from(ks.clone())))
    }

    async fn list_keyspaces(
        &self,
        _request: Request<()>,
    ) -> StdResult<Response<ListKeyspacesResponse>, Status> {
        let mut store = self.get_store_guard()?;
        let mut keyspaces = store.list_keyspaces()?;
        keyspaces.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Response::new(ListKeyspacesResponse { keyspaces }))
    }

    async fn create_keyspace(
        &self,
        request: Request<models::Keyspace>,
    ) -> StdResult<Response<()>, Status> {
        let mut store = self.get_store_guard()?;
        let request = request.into_inner();

        store.create_keyspace(request)?;
        Ok(Response::new(()))
    }

    async fn delete_keyspace(
        &self,
        request: Request<DeleteKeyspaceQuery>,
    ) -> StdResult<Response<()>, Status> {
        let mut store = self.get_store_guard()?;
        let request = request.into_inner();

        store.delete_keyspace(request.keyspace)?;
        Ok(Response::new(()))
    }

    async fn truncate_keyspace(
        &self,
        request: Request<TruncateKeyspaceQuery>,
    ) -> StdResult<Response<()>, Status> {
        let mut store = self.get_store_guard()?;
        let request = request.into_inner();

        store.truncate_keyspace(request.keyspace)?;
        Ok(Response::new(()))
    }

    async fn get_key(
        &self,
        request: Request<GetKeyQuery>,
    ) -> StdResult<Response<models::Record>, Status> {
        let request = request.into_inner();

        let mut store = self.get_store_guard()?;
        let ks = store.get_keyspace(request.keyspace.clone())?;

        let value = ks.get(request.key.clone())?;
        Ok(Response::new(models::Record {
            key: request.key,
            value,
        }))
    }

    async fn insert_key(
        &self,
        request: Request<InsertKeyQuery>,
    ) -> StdResult<Response<()>, Status> {
        let request = request.into_inner();
        let record = request.record.unwrap(); // Remove this prost is building Option<T> instead of T

        let mut store = self.get_store_guard()?;
        let ks = store.get_keyspace(request.keyspace)?;

        ks.insert(record)?;
        Ok(Response::new(()))
    }

    async fn delete_key(
        &self,
        request: Request<DeleteKeyQuery>,
    ) -> StdResult<Response<()>, Status> {
        let request = request.into_inner();
        let mut store = self.get_store_guard()?;
        let ks = store.get_keyspace(request.keyspace.clone())?;

        ks.delete(request.key)?;
        Ok(Response::new(()))
    }

    type GetKeysStream =
        Pin<Box<dyn Stream<Item = StdResult<models::Record, Status>> + Send + Sync + 'static>>;

    async fn get_keys(
        &self,
        request: Request<GetKeysQuery>,
    ) -> StdResult<Response<Self::GetKeysStream>, Status> {
        let request = request.into_inner();
        let mut store = self.get_store_guard()?;
        let ks = store.get_keyspace(request.keyspace.clone())?.clone();

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let ks = ks.clone();

            for key in request.keys {
                match ks.clone().get(key.clone()) {
                    Ok(value) => tx
                        .send(Ok(models::Record {
                            key: key.clone(),
                            value,
                        }))
                        .await
                        .unwrap(),
                    Err(e) => tx.send(Err(Status::from(e))).await.unwrap(),
                };
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn insert_keys(
        &self,
        request: Request<InsertKeysQuery>,
    ) -> StdResult<Response<()>, Status> {
        let request = request.into_inner();

        let mut store = self.get_store_guard()?;
        let ks = store.get_keyspace(request.keyspace)?;

        ks.batch_insert(request.records)?;
        Ok(Response::new(()))
    }

    async fn delete_keys(
        &self,
        request: Request<DeleteKeysQuery>,
    ) -> StdResult<Response<()>, Status> {
        let request = request.into_inner();
        let mut store = self.get_store_guard()?;
        let ks = store.get_keyspace(request.keyspace.clone())?;

        ks.batch_delete(request.keys)?;
        Ok(Response::new(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tonic::Code;
    use uuid::Uuid;

    use dumpstors_lib::store::store_server::Store as StoreServer;
    use std::sync::{Arc, Mutex};

    use tonic::IntoRequest;

    async fn create_random_store_server() -> DumpstorsStoreServer {
        let store = Arc::new(Mutex::new(Store::new(format!(".data/{}", Uuid::new_v4()))));
        super::DumpstorsStoreServer::new(store)
    }

    #[tokio::test]
    async fn store_server_ping_test() {
        let srv = create_random_store_server().await;
        assert_eq!(srv.ping(().into_request()).await.unwrap().into_inner(), ());
    }

    #[tokio::test]
    async fn store_server_keyspace_test() {
        let srv = create_random_store_server().await;

        let ks1 = models::Keyspace {
            name: String::from("ks1").clone(),
        };
        let ks2 = models::Keyspace {
            name: String::from("ks2").clone(),
        };

        srv.create_keyspace(ks1.clone().into_request())
            .await
            .unwrap();
        srv.create_keyspace(ks2.clone().into_request())
            .await
            .unwrap();

        let resp = srv
            .get_keyspace(
                GetKeyspaceQuery {
                    keyspace: ks1.name.clone(),
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp, ks1.clone());

        let resp = srv
            .get_keyspace(
                GetKeyspaceQuery {
                    keyspace: ks2.name.clone(),
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp, ks2.clone());

        srv.delete_keyspace(
            DeleteKeyspaceQuery {
                keyspace: ks1.name.clone(),
            }
            .into_request(),
        )
        .await
        .unwrap();

        let resp = srv
            .get_keyspace(
                GetKeyspaceQuery {
                    keyspace: ks1.name.clone(),
                }
                .into_request(),
            )
            .await;

        match resp {
            Err(e) => assert_eq!(e.code(), Code::NotFound),
            _ => assert!(false, "Keyspace should not exist after being deleted"),
        };

        srv.truncate_keyspace(
            TruncateKeyspaceQuery {
                keyspace: ks2.name.clone(),
            }
            .into_request(),
        )
        .await
        .unwrap();

        let resp = srv
            .get_keyspace(
                GetKeyspaceQuery {
                    keyspace: ks2.name.clone(),
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp, ks2.clone());
    }

    #[tokio::test]
    async fn store_server_create_existing_keyspace() {
        let srv = create_random_store_server().await;
        let ks = models::Keyspace {
            name: String::from("ks").clone(),
        };

        srv.create_keyspace(ks.clone().into_request())
            .await
            .unwrap();
        let resp = srv.create_keyspace(ks.clone().into_request()).await;

        match resp {
            Err(e) => assert_eq!(e.code(), Code::AlreadyExists),
            _ => assert!(false, "Creating an existing keyspace must return an error"),
        };
    }

    #[tokio::test]
    async fn get_inexistant_key_test() {
        let srv = create_random_store_server().await;
        let ks = models::Keyspace {
            name: String::from("ks").clone(),
        };
        srv.create_keyspace(ks.clone().into_request())
            .await
            .unwrap();
        let resp = srv
            .get_key(
                GetKeyQuery {
                    keyspace: ks.name.clone(),
                    key: b"foo".to_vec(),
                }
                .into_request(),
            )
            .await;

        match resp {
            Err(e) => assert_eq!(e.code(), Code::NotFound),
            _ => assert!(false, "Getting an inextant key should return an NotFound"),
        };
    }

    #[tokio::test]
    async fn delete_inexistant_key_test() {
        let srv = create_random_store_server().await;
        let ks = models::Keyspace {
            name: String::from("ks").clone(),
        };
        srv.create_keyspace(ks.clone().into_request())
            .await
            .unwrap();
        let resp = srv
            .delete_key(
                DeleteKeyQuery {
                    keyspace: ks.name.clone(),
                    key: b"foo".to_vec(),
                }
                .into_request(),
            )
            .await;

        match resp {
            Err(e) => assert_eq!(e.code(), Code::NotFound),
            _ => assert!(false, "Deleting an inextant key should return an NotFound"),
        };
    }

    #[tokio::test]
    async fn getdelins_inexistant_keyspace_test() {
        let srv = create_random_store_server().await;

        let resp = srv
            .delete_key(
                DeleteKeyQuery {
                    keyspace: String::from("NotFound"),
                    key: b"foo".to_vec(),
                }
                .into_request(),
            )
            .await;

        match resp {
            Err(e) => assert_eq!(e.code(), Code::NotFound),
            _ => assert!(
                false,
                "Deleting a key on a unknown keyspace must return an NotFound"
            ),
        };

        let resp = srv
            .insert_key(
                InsertKeyQuery {
                    keyspace: String::from("NotFound"),
                    record: Some(models::Record {
                        key: b"foo".to_vec(),
                        value: b"foo".to_vec(),
                    }),
                }
                .into_request(),
            )
            .await;

        match resp {
            Err(e) => assert_eq!(e.code(), Code::NotFound),
            _ => assert!(
                false,
                "Inserting a key on a unknown keyspace must return an NotFound"
            ),
        };

        let resp = srv
            .get_key(
                GetKeyQuery {
                    keyspace: String::from("NotFound"),
                    key: b"foo".to_vec(),
                }
                .into_request(),
            )
            .await;

        match resp {
            Err(e) => assert_eq!(e.code(), Code::NotFound),
            _ => assert!(
                false,
                "Getting a key on a unknown keyspace must return an NotFound"
            ),
        };
    }

    #[tokio::test]
    async fn insert_get_key_test() {
        let srv = create_random_store_server().await;
        let ks = models::Keyspace {
            name: String::from("ks").clone(),
        };
        srv.create_keyspace(ks.clone().into_request())
            .await
            .unwrap();

        let records: Vec<models::Record> = vec![
            models::Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            models::Record {
                key: b"doo".to_vec(),
                value: b"dar".to_vec(),
            },
            models::Record {
                key: b"daa".to_vec(),
                value: b"daa".to_vec(),
            },
            models::Record {
                key: b"duu".to_vec(),
                value: b"duu".to_vec(),
            },
        ];

        for r in records.clone() {
            srv.insert_key(
                InsertKeyQuery {
                    keyspace: ks.name.clone(),
                    record: Some(r.clone()), // Fix this Some...
                }
                .into_request(),
            )
            .await
            .unwrap();

            let resp = srv
                .get_key(
                    GetKeyQuery {
                        keyspace: ks.name.clone(),
                        key: r.key.clone(),
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();

            assert_eq!(r, resp)
        }
    }

    #[tokio::test]
    async fn insert_keys_test() {
        let srv = create_random_store_server().await;
        let ks = models::Keyspace {
            name: String::from("ks").clone(),
        };
        srv.create_keyspace(ks.clone().into_request())
            .await
            .unwrap();

        let records: Vec<models::Record> = vec![
            models::Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            models::Record {
                key: b"doo".to_vec(),
                value: b"dar".to_vec(),
            },
            models::Record {
                key: b"daa".to_vec(),
                value: b"daa".to_vec(),
            },
            models::Record {
                key: b"duu".to_vec(),
                value: b"duu".to_vec(),
            },
        ];

        srv.insert_keys(
            InsertKeysQuery {
                keyspace: ks.name.clone(),
                records: records.clone(),
            }
            .into_request(),
        )
        .await
        .unwrap();

        for r in records.clone() {
            let resp = srv
                .get_key(
                    GetKeyQuery {
                        keyspace: ks.name.clone(),
                        key: r.key.clone(),
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();

            assert_eq!(r, resp)
        }
    }

    #[tokio::test]
    async fn delete_keys_test() {
        let srv = create_random_store_server().await;
        let ks = models::Keyspace {
            name: String::from("ks").clone(),
        };
        srv.create_keyspace(ks.clone().into_request())
            .await
            .unwrap();

        let records: Vec<models::Record> = vec![
            models::Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            models::Record {
                key: b"doo".to_vec(),
                value: b"dar".to_vec(),
            },
            models::Record {
                key: b"daa".to_vec(),
                value: b"daa".to_vec(),
            },
            models::Record {
                key: b"duu".to_vec(),
                value: b"duu".to_vec(),
            },
        ];

        srv.insert_keys(
            InsertKeysQuery {
                keyspace: ks.name.clone(),
                records: records.clone(),
            }
            .into_request(),
        )
        .await
        .unwrap();

        srv.delete_keys(
            DeleteKeysQuery {
                keyspace: ks.name.clone(),
                keys: records.clone().into_iter().map(|r| r.key).collect(),
            }
            .into_request(),
        )
        .await
        .unwrap();

        for r in records.clone() {
            let resp = srv
                .get_key(
                    GetKeyQuery {
                        keyspace: ks.name.clone(),
                        key: r.key.clone(),
                    }
                    .into_request(),
                )
                .await;

            match resp {
                Err(e) => assert_eq!(e.code(), Code::NotFound),
                _ => assert!(false, "Getting an inextant key should return an NotFound"),
            };
        }
    }

    #[tokio::test]
    async fn get_keys_stream_test() {
        let srv = create_random_store_server().await;
        let ks = models::Keyspace {
            name: String::from("ks").clone(),
        };
        srv.create_keyspace(ks.clone().into_request())
            .await
            .unwrap();

        let records: Vec<models::Record> = vec![
            models::Record {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            models::Record {
                key: b"doo".to_vec(),
                value: b"dar".to_vec(),
            },
            models::Record {
                key: b"daa".to_vec(),
                value: b"daa".to_vec(),
            },
            models::Record {
                key: b"duu".to_vec(),
                value: b"duu".to_vec(),
            },
        ];

        for r in records.clone() {
            srv.insert_key(
                InsertKeyQuery {
                    keyspace: ks.name.clone(),
                    record: Some(r.clone()), // Fix this Some...
                }
                .into_request(),
            )
            .await
            .unwrap();
        }

        let mut resp = srv
            .get_keys(
                GetKeysQuery {
                    keyspace: ks.name.clone(),
                    keys: records.clone().into_iter().map(|r| r.key).collect(),
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();

        let mut inserted_records = vec![];
        while let Some(r) = resp.next().await {
            inserted_records.push(r.unwrap());
        }

        assert_eq!(records, inserted_records)
    }
}
