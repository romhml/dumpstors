extern crate serde;
#[macro_use]
extern crate serde_derive;

use tonic::transport::Server;

use dumpstors_lib::dumpstors_server::DumpstorsServer;
use dumpstors_lib::store::Store;

mod settings;
mod store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = settings::Settings::new().unwrap();
    let sockaddr = format!("{}:{}", conf.listen_addr, conf.port);
    let store_srv = store::StoreServer::new(Store::new(conf.store.path));

    Server::builder()
        .add_service(DumpstorsServer::new(store_srv))
        .serve(sockaddr.parse()?)
        .await?;

    Ok(())
}
