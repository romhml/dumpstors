use std::fmt;
use std::str;
use structopt::StructOpt;
use tonic::Response;

use super::store::*;
use dumpstors_lib::models::*;
use dumpstors_lib::store as store_lib;

#[derive(Debug, StructOpt)]
pub enum QueryOpt {
    Insert(InsertKeyOpt),
    Get(GetKeyOpt),
    Delete(DeleteKeyOpt),
    Keyspaces(keyspace::KeyspaceCommand),
}

#[derive(Debug, StructOpt)]
pub struct Query {
    #[structopt(short, long, default_value = "http://localhost:4242")]
    pub bootstrap: String,

    #[structopt(flatten)]
    pub opts: QueryOpt,
}

fn format_bytes(bytes: &[u8]) -> String {
    match str::from_utf8(bytes) {
        Ok(s) => String::from(s),
        Err(_) => format!("{:?}", bytes),
    }
}

#[derive(Debug)]
pub enum QueryResult {
    Record(Response<Record>),
    Keyspace(Response<Keyspace>),
    KeyspaceList(Response<store_lib::ListKeyspacesResponse>),
    Empty(Response<()>),
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Record(resp) => {
                let record = resp.get_ref();

                write!(
                    f,
                    "{}={}",
                    format_bytes(record.key.as_slice()),
                    format_bytes(record.value.as_slice())
                )
            }
            Self::KeyspaceList(resp) => write!(
                f,
                "{}",
                resp.get_ref()
                    .keyspaces
                    .clone()
                    .into_iter()
                    .map(|ks| ks.name)
                    .collect::<Vec<String>>()
                    .join("\n")
            ),
            Self::Keyspace(resp) => write!(f, "{}", resp.get_ref().name),
            Self::Empty(_) => write!(f, ""),
        }
    }
}

impl Into<QueryResult> for Response<Record> {
    fn into(self) -> QueryResult {
        QueryResult::Record(self)
    }
}

impl Into<QueryResult> for Response<Keyspace> {
    fn into(self) -> QueryResult {
        QueryResult::Keyspace(self)
    }
}

impl Into<QueryResult> for Response<store_lib::ListKeyspacesResponse> {
    fn into(self) -> QueryResult {
        QueryResult::KeyspaceList(self)
    }
}

impl Into<QueryResult> for Response<()> {
    fn into(self) -> QueryResult {
        QueryResult::Empty(self)
    }
}
