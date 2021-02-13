pub mod keyspace;

use dumpstors_lib::store::*;
use structopt::StructOpt;
use tonic::Response;

#[derive(Debug)]
pub enum QueryResult {
    Get(Response<GetResponse>),
    Insert(Response<InsertResponse>),
    Delete(Response<DeleteResponse>),

    GetKeyspace(Response<GetKeyspacesResponse>),
    CreateKeyspace(Response<CreateKeyspacesResponse>),
    DeleteKeyspace(Response<DeleteKeyspacesResponse>),
}

#[derive(Debug, StructOpt)]
pub struct InsertOpt {
    #[structopt(long, short)]
    pub keyspace: String,

    pub key: String,
    pub value: String,
}

#[derive(Debug, StructOpt)]
pub struct GetOpt {
    #[structopt(long, short)]
    pub keyspace: String,
    pub key: String,
}

#[derive(Debug, StructOpt)]
pub struct DeleteOpt {
    #[structopt(long, short)]
    pub keyspace: String,
    pub key: String,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    Insert(InsertOpt),
    Get(GetOpt),
    Delete(DeleteOpt),
    Keyspaces(keyspace::KeyspaceCommand),
}
