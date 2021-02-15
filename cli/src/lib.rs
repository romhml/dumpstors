use dumpstors_lib::store::store_client::StoreClient;

pub mod query;
pub mod store;

use query::*;
use store::keyspace::*;

pub async fn execute(q: Query) -> Result<QueryResult, tonic::Status> {
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
