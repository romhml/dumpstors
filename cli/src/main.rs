use structopt::StructOpt;
use tonic::Request;

use dumpstors_lib::models::{Keyspace, Record};
use dumpstors_lib::store as store_lib;
use dumpstors_lib::store::store_client::StoreClient;

mod store;
use store::keyspace::*;
use store::*;

#[derive(Debug, StructOpt)]
pub struct QueryOpt {
    #[structopt(short, long, default_value = "http://localhost:4242")]
    bootstrap: String,

    #[structopt(flatten)]
    command: Command,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = QueryOpt::from_args();
    let mut client = StoreClient::connect(args.bootstrap.clone()).await.unwrap();

    let resp: QueryResult = match args.command {
        Command::Get(args) => QueryResult::Get(
            client
                .get_keys(Request::new(store_lib::GetQuery {
                    keyspace: args.keyspace,
                    keys: vec![args.key.into_bytes()],
                }))
                .await?,
        ),

        Command::Insert(args) => QueryResult::Insert(
            client
                .insert_keys(Request::new(store_lib::InsertQuery {
                    keyspace: args.keyspace,
                    records: vec![Record {
                        value: args.value.into_bytes(),
                        key: args.key.into_bytes(),
                    }],
                }))
                .await?,
        ),

        Command::Delete(args) => QueryResult::Delete(
            client
                .delete_keys(Request::new(store_lib::DeleteQuery {
                    keyspace: args.keyspace,
                    keys: vec![args.key.into_bytes()],
                }))
                .await?,
        ),

        Command::Keyspaces(ks) => match ks {
            KeyspaceCommand::Get(args) => QueryResult::GetKeyspace(
                client
                    .get_keyspaces(Request::new(store_lib::GetKeyspacesQuery {
                        keyspaces: vec![args.keyspace],
                    }))
                    .await?,
            ),

            KeyspaceCommand::Create(args) => QueryResult::CreateKeyspace(
                client
                    .create_keyspaces(Request::new(store_lib::CreateKeyspacesQuery {
                        keyspaces: vec![Keyspace {
                            name: args.keyspace,
                        }],
                    }))
                    .await?,
            ),

            KeyspaceCommand::Delete(args) => QueryResult::DeleteKeyspace(
                client
                    .delete_keyspaces(Request::new(store_lib::DeleteKeyspacesQuery {
                        keyspaces: vec![args.keyspace],
                    }))
                    .await?,
            ),
        },
    };

    println!("{:#?}", resp);
    Ok(())
}
