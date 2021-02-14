pub mod keyspace;

use tonic::IntoRequest;

use dumpstors_lib::models::*;
use dumpstors_lib::store::*;

use structopt::StructOpt;
use tonic::Request;

#[derive(Debug, StructOpt)]
pub struct InsertKeyOpt {
    #[structopt(long, short)]
    pub keyspace: String,

    pub key: String,
    pub value: String,
}

impl IntoRequest<InsertKeyQuery> for InsertKeyOpt {
    fn into_request(self) -> Request<InsertKeyQuery> {
        Request::new(InsertKeyQuery {
            keyspace: self.keyspace,
            record: Some(Record {
                key: self.key.as_bytes().to_vec(),
                value: self.value.as_bytes().to_vec(),
            }),
        })
    }
}

#[derive(Debug, StructOpt)]
pub struct GetKeyOpt {
    #[structopt(long, short)]
    pub keyspace: String,
    pub key: String,
}

impl IntoRequest<GetKeyQuery> for GetKeyOpt {
    fn into_request(self) -> Request<GetKeyQuery> {
        Request::new(GetKeyQuery {
            keyspace: self.keyspace,
            key: self.key.as_bytes().to_vec(),
        })
    }
}

#[derive(Debug, StructOpt)]
pub struct DeleteKeyOpt {
    #[structopt(long, short)]
    pub keyspace: String,
    pub key: String,
}

impl IntoRequest<DeleteKeyQuery> for DeleteKeyOpt {
    fn into_request(self) -> Request<DeleteKeyQuery> {
        Request::new(DeleteKeyQuery {
            keyspace: self.keyspace,
            key: self.key.as_bytes().to_vec(),
        })
    }
}
