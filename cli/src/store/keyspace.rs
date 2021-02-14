use structopt::StructOpt;

use dumpstors_lib::models::*;
use dumpstors_lib::store::*;
use tonic::{IntoRequest, Request};

#[derive(Debug, StructOpt)]
pub enum KeyspaceCommand {
    Create(CreateKeyspaceOpt),
    Get(GetKeyspaceOpt),
    Delete(DeleteKeyspaceOpt),
}

#[derive(Debug, StructOpt)]
pub struct CreateKeyspaceOpt {
    pub name: String,
}

impl IntoRequest<Keyspace> for CreateKeyspaceOpt {
    fn into_request(self) -> Request<Keyspace> {
        Request::new(Keyspace { name: self.name })
    }
}

#[derive(Debug, StructOpt)]
pub struct GetKeyspaceOpt {
    pub keyspace: String,
}

impl IntoRequest<GetKeyspaceQuery> for GetKeyspaceOpt {
    fn into_request(self) -> Request<GetKeyspaceQuery> {
        Request::new(GetKeyspaceQuery {
            keyspace: self.keyspace,
        })
    }
}

#[derive(Debug, StructOpt)]
pub struct DeleteKeyspaceOpt {
    pub keyspace: String,
}

impl IntoRequest<DeleteKeyspaceQuery> for DeleteKeyspaceOpt {
    fn into_request(self) -> Request<DeleteKeyspaceQuery> {
        Request::new(DeleteKeyspaceQuery {
            keyspace: self.keyspace,
        })
    }
}
