use structopt::StructOpt;

use tonic::Request;

use dumpsters_lib::dumpsters_client::DumpstersClient;
use dumpsters_lib::*;

#[derive(Debug, StructOpt)]
struct GetQueryOpt {
    bootstrap: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = GetQueryOpt::from_args();

    println!("{:?}", args);
    let mut client = DumpstersClient::connect(args.bootstrap.clone()).await?;

    let req = Request::new(InsertQuery {
        keyspace: String::from("ks1"),
        records: vec![Record {
            key: vec![0],
            value: vec![0, 1, 2, 3],
        }],
    });

    let response = client.insert_keys(req).await?;
    println!("{:?}", response);

    let req = Request::new(GetQuery {
        keyspace: String::from("ks1"),
        keys: vec![vec![0]],
    });

    let response = client.get_keys(req).await?;
    println!("RESPONSE\n{:?}", response);

    Ok(())
}
