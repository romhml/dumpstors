use dumpstors_lib::store::store_client::StoreClient;
use structopt::StructOpt;

mod query;
pub mod store;

use query::*;
use store::keyspace::*;

async fn execute(q: Query) -> Result<QueryResult, Box<dyn std::error::Error>> {
    let mut client = StoreClient::connect(q.bootstrap.clone()).await.unwrap();

    let resp: QueryResult = match q.opts {
        QueryOpt::Get(args) => client.get_key(args).await?.into(),

        QueryOpt::Insert(args) => client.insert_key(args).await?.into(),

        QueryOpt::Delete(args) => client.delete_key(args).await?.into(),

        QueryOpt::Keyspaces(ks) => match ks {
            KeyspaceCommand::Get(args) => client.get_keyspace(args).await?.into(),

            KeyspaceCommand::Create(args) => client.create_keyspace(args).await?.into(),

            KeyspaceCommand::Delete(args) => client.delete_keyspace(args).await?.into(),
        },
    };

    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q = Query::from_args();
    match execute(q).await {
        Ok(resp) => println!("{}", resp),
        Err(e) => panic!("{}", e),
    }
    Ok(())
}
