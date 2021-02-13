extern crate serde;
#[macro_use]
extern crate serde_derive;

use std::sync::{Arc, Mutex};
use tonic::transport::Server;
use log::*;

use dumpstors_lib::store::store_server::StoreServer;
use dumpstors_lib::store::Store;

mod settings;
mod store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let conf = settings::Settings::new().unwrap();
    let sockaddr = format!("{}:{}", conf.listen_addr, conf.port);

    info!("Loading store at '{}'", conf.store.path);
    let store = Arc::new(Mutex::new(Store::new(conf.store.path)));
    let store_srv = store::DumpstorsStoreServer::new(store.clone());

    info!("Starting server on '{}'", sockaddr);

    Server::builder()
        .add_service(StoreServer::new(store_srv))
        .serve(sockaddr.parse()?)
        .await?;

    Ok(())
}
