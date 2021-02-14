use std::fmt;
use std::str;
use structopt::StructOpt;
use tonic::Response;

use super::store::*;
use dumpstors_lib::models::*;

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

#[derive(Debug)]
pub enum QueryResult {
    Record(Response<Record>),
    Keyspace(Response<Keyspace>),
    Empty(Response<()>),
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            _ => write!(f, "{:?}", self),
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

impl Into<QueryResult> for Response<()> {
  fn into(self) -> QueryResult {
      QueryResult::Empty(self)
  }
}