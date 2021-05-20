use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;
use std::pin::Pin;

use log::{error, info};

use std::result::Result as StdResult;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

use dumpstors_lib::models;
use dumpstors_lib::node::{node_client::NodeClient, node_server};
use dumpstors_lib::store::Result;

#[derive(Debug, Clone)]
pub struct Node {
    // TODO: Should be a system table
    info: models::Node,
    peers: Arc<Mutex<HashMap<u64, models::Node>>>,
    conns: HashMap<u64, NodeClient<tonic::transport::Channel>>,
}

impl Node {
    pub fn new(id: u64, host: String) -> Self {
        Self {
            info: models::Node {
                id,
                host,
                status: 0,
            },
            peers: Arc::new(Mutex::new(HashMap::new())),
            conns: HashMap::new(),
        }
    }

    pub async fn bootstrap(&mut self, seeds: Vec<String>) -> Result<()> {
        let node_futures = seeds
            .into_iter()
            // Add a copy of `self.info` in each future context
            .map(|seed| (seed, self.info.clone()))
            // Map the list of seeds to a list of futures that return a list of nodes
            .map(|(seed, info)| {
                async move {
                    info!("Sending advertisement to seed {}", seed);
                    let mut client = NodeClient::connect(seed.clone()).await?;
                    let resp = client.advertise(Request::new(info)).await?;
                    info!("Received advertisement response from seed {}", seed);
                    Ok(resp.into_inner().nodes)
                }
                .boxed()
            })
            .collect::<Vec<
                Pin<
                    Box<
                        dyn futures::Future<Output = Result<Vec<models::Node>>> + std::marker::Send,
                    >,
                >,
            >>();

        // Pick the result of the first successful future
        let (nodes, _) = futures::future::select_ok(node_futures.into_iter()).await?;

        info!("{:?}", nodes);
        // TODO: Implement Gossip service and start it here

        Ok(())
    }

    // TODO: Look at how similar distributed technologies handles sharing peers and joining the
    // cluster.
    // Cassandra:
    // - Uses a seed node: New nodes send a request to seed nodes at startup to fetch the list of
    // peers
    // - Nodes are probing each other all the time using the "Gossip" Protocol. It allows
    // nodes to determine the state of other nodes.

    // TODO: Implement a probe / gossip
}

#[tonic::async_trait]
impl node_server::Node for Node {
    async fn ping(&self, _request: Request<()>) -> StdResult<Response<()>, Status> {
        Ok(Response::new(()))
    }

    // TODO: Move this to an internal endpoint
    async fn advertise(
        &self,
        request: Request<models::Node>,
    ) -> StdResult<Response<models::Nodes>, Status> {
        let node = request.into_inner();
        info!(
            "Received advertisement from node {} ({})",
            node.id, node.host
        );
        let mut peers = self.peers.lock().unwrap();

        peers.insert(node.id.clone(), node);
        let response = models::Nodes {
            nodes: peers.values().cloned().collect(),
        };
        Ok(Response::new(response))
    }
}
