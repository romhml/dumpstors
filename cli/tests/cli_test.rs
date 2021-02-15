mod common;

use std::thread::sleep;
use std::time::Duration;

use dumpstors_cli::{execute, query::*};
use structopt::StructOpt;

#[tokio::test]
async fn test_cli_getinsdel() {
    common::setup().await.unwrap();
    sleep(Duration::from_secs(2));

    let q = Query::from_iter(&["dumpstors_cli", "keyspaces", "create", "ks1"]);
    execute(q).await.unwrap();

    let q = Query::from_iter(&[
        "dumpstors_cli",
        "insert",
        "--keyspace",
        "ks1",
        "key",
        "value",
    ]);
    execute(q).await.unwrap();

    let q = Query::from_iter(&["dumpstors_cli", "get", "--keyspace", "ks1", "key"]);
    execute(q).await.unwrap();

    let q = Query::from_iter(&["dumpstors_cli", "delete", "--keyspace", "ks1", "key"]);
    execute(q).await.unwrap();

    let q = Query::from_iter(&["dumpstors_cli", "keyspaces", "delete", "ks1"]);
    execute(q).await.unwrap();
}
