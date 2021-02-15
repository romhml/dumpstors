use dumpstors_cli::{execute, query::*};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q = Query::from_args();
    match execute(q).await {
        Ok(resp) => println!("{}", resp),
        Err(e) => panic!("{}", e),
    }
    Ok(())
}
