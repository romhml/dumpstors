extern crate serde;
#[macro_use]
extern crate serde_derive;

use log::*;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;

use dumpstors_lib::store::store_server::StoreServer;

pub mod settings;
mod store;

use store::{server::DumpstorsStoreServer, Store};

pub async fn start_server(conf: settings::Settings) -> Result<(), Box<dyn std::error::Error>> {
    let sockaddr = format!("{}:{}", conf.listen_addr, conf.port).parse()?;

    info!("Loading store at '{}'", conf.store.path);
    let store = Arc::new(Mutex::new(Store::new(conf.store.path)));
    let store_srv = DumpstorsStoreServer::new(store);

    info!("Starting server on '{}'", sockaddr);

    Server::builder()
        .add_service(StoreServer::new(store_srv))
        .serve(sockaddr)
        .await?;

    Ok(())
}
