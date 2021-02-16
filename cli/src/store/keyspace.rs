use structopt::StructOpt;

use dumpstors_lib::models::*;
use dumpstors_lib::store::*;
use tonic::{IntoRequest, Request};

#[derive(Debug, StructOpt)]
pub enum KeyspaceCommand {
    Create(CreateKeyspaceOpt),
    Get(GetKeyspaceOpt),
    Delete(DeleteKeyspaceOpt),
    List,
    Truncate(TruncateKeyspaceOpt),
}

#[derive(Debug, StructOpt)]
pub struct CreateKeyspaceOpt {
    pub name: String,
}

impl IntoRequest<Keyspace> for CreateKeyspaceOpt {
    fn into_request(self) -> Request<Keyspace> {
        Keyspace { name: self.name }.into_request()
    }
}

#[derive(Debug, StructOpt)]
pub struct GetKeyspaceOpt {
    pub keyspace: String,
}

impl IntoRequest<GetKeyspaceQuery> for GetKeyspaceOpt {
    fn into_request(self) -> Request<GetKeyspaceQuery> {
        GetKeyspaceQuery {
            keyspace: self.keyspace,
        }
        .into_request()
    }
}

#[derive(Debug, StructOpt)]
pub struct DeleteKeyspaceOpt {
    pub keyspace: String,
}

impl IntoRequest<DeleteKeyspaceQuery> for DeleteKeyspaceOpt {
    fn into_request(self) -> Request<DeleteKeyspaceQuery> {
        DeleteKeyspaceQuery {
            keyspace: self.keyspace,
        }
        .into_request()
    }
}

#[derive(Debug, StructOpt)]
pub struct TruncateKeyspaceOpt {
    pub keyspace: String,
}

impl IntoRequest<TruncateKeyspaceQuery> for TruncateKeyspaceOpt {
    fn into_request(self) -> Request<TruncateKeyspaceQuery> {
        TruncateKeyspaceQuery {
            keyspace: self.keyspace,
        }
        .into_request()
    }
}
