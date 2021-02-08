use structopt::StructOpt;

use tonic::Request;

use dumpstors_lib::models::Record;

use dumpstors_lib::{ store, store::keyspace};
use dumpstors_lib::store::keyspace::{keyspace_client::KeyspaceClient};
use dumpstors_lib::store::{store_client::StoreClient};

#[derive(Debug, StructOpt)]
struct CreateKeyspaceOpt {
    #[structopt(short, long)]
    keyspace: String,
}

#[derive(Debug, StructOpt)]
struct GetKeyspaceOpt {
    #[structopt(short, long)]
    keyspace: String,
}

#[derive(Debug, StructOpt)]
struct DeleteKeyspaceOpt {
    keyspace: String,
}

#[derive(Debug, StructOpt)]
pub enum KeyspaceCommand {
    Create(CreateKeyspaceOpt),
    Get(GetKeyspaceOpt),
    Delete(DeleteKeyspaceOpt)
}

#[derive(Debug, StructOpt)]
struct KeyInsertOpt {
    #[structopt(long)]
    keyspace: String,

    #[structopt(long)]
    key: String,

    #[structopt(long)]
    value: String
}

#[derive(Debug, StructOpt)]
struct KeyGetOpt {
    #[structopt(long)]
    keyspace: String,
    #[structopt(long)]
    key: String
}

#[derive(Debug, StructOpt)]
struct KeyDeleteOpt {
    #[structopt(long)]
    keyspace: String,

    key: String
}

#[derive(Debug, StructOpt)]
pub enum KeyCommand {
    Insert(KeyInsertOpt),
    Get(KeyGetOpt),
    Delete(KeyDeleteOpt)
}

#[derive(Debug, StructOpt)]
pub enum Command {
    Keys(KeyCommand),
    Keyspaces(KeyspaceCommand),
}

#[derive(Debug, StructOpt)]
struct QueryOpt {
    bootstrap: String,

    #[structopt(flatten)]
    command: Command
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = QueryOpt::from_args();

    println!("{:?}", args);

    match args.command {

        Command::Keys(k) => {
            let mut client = KeyspaceClient::connect(args.bootstrap.clone()).await.unwrap();
            match k {
                KeyCommand::Get(args) => client.get_keys(Request::new(keyspace::GetQuery { keyspace: args.keyspace, keys: vec![args.key.into_bytes()] })),
                KeyCommand::Insert(args) => client.insert_keys(Request::new(keyspace::InsertQuery { keyspace: args.keyspace, records: vec![{ value: vec![args.value.into_bytes()], key: vec![args.key.into_bytes()] }])),
                KeyCommand::Delete(args) => client.delete_keys(Request::new(keyspace::DeleteQuery { keyspace: args.keyspace, keys: vec![args.key.into_bytes()] })),
            };
        }

        Command::Keyspaces(ks) => {
            let mut client = StoreClient::connect(args.bootstrap.clone()).await.unwrap();
            match ks {
                KeyspaceCommand::Get(args) => client.get_keyspaces(Request::new(store::GetKeyspacesQuery { keyspaces: vec![args.keyspace] })),
                KeyspaceCommand::Create(args) => client.create_keyspaces(Request::new(store::CreateKeyspacesQuery { keyspaces: vec![args.keyspace] })),
                KeyspaceCommand::Delete(args) => client.delete_keyspaces(Request::new(store::DeleteKeyspacesQuery { keyspaces: vec![args.keyspace] }))
            }
        }
    }

    Ok(())
}