#![feature(allocator_api)]
use structopt::StructOpt;

use tonic::{Request, Response};

use dumpstors_lib::models::Record;

use dumpstors_lib::store;
use dumpstors_lib::store::store_client::StoreClient;

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
enum KeyspaceCommand {
    Create(CreateKeyspaceOpt),
    Get(GetKeyspaceOpt),
    Delete(DeleteKeyspaceOpt),
}

#[derive(Debug, StructOpt)]
struct InsertOpt {
    #[structopt(long)]
    keyspace: String,

    #[structopt(long)]
    key: String,

    #[structopt(long)]
    value: String,
}

#[derive(Debug, StructOpt)]
struct GetOpt {
    #[structopt(long)]
    keyspace: String,
    key: String,
}

#[derive(Debug, StructOpt)]
struct DeleteOpt {
    #[structopt(long)]
    keyspace: String,
    key: String,
}

#[derive(Debug, StructOpt)]
enum KeyCommand {
    Insert(InsertOpt),
    Get(GetOpt),
    Delete(DeleteOpt),
}

#[derive(Debug, StructOpt)]
enum Command {
    Keys(KeyCommand),
    Keyspaces(KeyspaceCommand),
}

#[derive(Debug, StructOpt)]
struct QueryOpt {
    #[structopt(short, long)]
    bootstrap: String,

    #[structopt(flatten)]
    command: Command,
}

#[derive(Debug)]
enum QueryResult {
    Get(Response<store::GetResponse>),
    Insert(Response<store::InsertResponse>),
    Delete(Response<store::DeleteResponse>),

    GetKeyspace(Response<store::GetKeyspacesResponse>),
    CreateKeyspace(Response<store::CreateKeyspacesResponse>),
    DeleteKeyspace(Response<store::DeleteKeyspacesResponse>),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = QueryOpt::from_args();
    let mut client = StoreClient::connect(args.bootstrap.clone()).await.unwrap();

    let resp: QueryResult = match args.command {
        Command::Keys(k) => match k {
            KeyCommand::Get(args) => QueryResult::Get(
                client
                    .get_keys(Request::new(store::GetQuery {
                        keyspace: args.keyspace,
                        keys: vec![args.key.into_bytes()],
                    }))
                    .await?,
            ),
            KeyCommand::Insert(args) => QueryResult::Insert(
                client
                    .insert_keys(Request::new(store::InsertQuery {
                        keyspace: args.keyspace,
                        records: vec![Record {
                            value: args.value.into_bytes(),
                            key: args.key.into_bytes(),
                        }],
                    }))
                    .await?,
            ),
            KeyCommand::Delete(args) => QueryResult::Delete(
                client
                    .delete_keys(Request::new(store::DeleteQuery {
                        keyspace: args.keyspace,
                        keys: vec![args.key.into_bytes()],
                    }))
                    .await?,
            ),
        },

        Command::Keyspaces(ks) => match ks {
            KeyspaceCommand::Get(args) => QueryResult::GetKeyspace(
                client
                    .get_keyspaces(Request::new(store::GetKeyspacesQuery {
                        keyspaces: vec![args.keyspace],
                    }))
                    .await?,
            ),
            KeyspaceCommand::Create(args) => QueryResult::CreateKeyspace(
                client
                    .create_keyspaces(Request::new(store::CreateKeyspacesQuery {
                        keyspaces: vec![args.keyspace],
                    }))
                    .await?,
            ),
            KeyspaceCommand::Delete(args) => QueryResult::DeleteKeyspace(
                client
                    .delete_keyspaces(Request::new(store::DeleteKeyspacesQuery {
                        keyspaces: vec![args.keyspace],
                    }))
                    .await?,
            ),
        },
    };

    println!("{:#?}", resp);
    Ok(())
}
