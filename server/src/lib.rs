extern crate serde;
#[macro_use]
extern crate serde_derive;

pub mod node;
pub mod settings;
mod store;

use log::*;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;

use dumpstors_lib::node::node_server::NodeServer;
use dumpstors_lib::store::store_server::StoreServer;

use node::Node;
use store::{server::DumpstorsStoreServer, Store};

use rand::Rng;

pub async fn start_server(conf: settings::Settings) -> Result<(), Box<dyn std::error::Error>> {
    let sockaddr: SocketAddr = format!("{}:{}", conf.listen_addr, conf.port).parse()?;

    let mut rng = rand::thread_rng();

    info!("Loading store at '{}'", conf.store.path);
    let store = Arc::new(Mutex::new(Store::new(conf.store.path)));
    let store_srv = DumpstorsStoreServer::new(store);

    let mut node_srv = Node::new(rng.gen(), sockaddr.to_string());

    info!("Starting server on '{}'", sockaddr);

    let srv = Server::builder()
        .add_service(StoreServer::new(store_srv))
        .add_service(NodeServer::new(node_srv.clone()))
        .serve(sockaddr);

    /*
     *node_srv.bootstrap(conf.seeds).await.unwrap_or_else(|e| {
     *    panic!("Could not reach any seeds: {:#?}", e)
     *});
     */
    node_srv.bootstrap(conf.seeds).await;

    srv.await?;

    Ok(())
}
