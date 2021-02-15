mod common;

use std::thread::sleep;
use std::time::Duration;

use dumpstors_cli::{execute, query::*};
use structopt::StructOpt;

#[tokio::test]
async fn test_cli_getinsdel() {
    let port = 55028;
    common::start_ephemeral_server(port).await.unwrap();
    let addr = &format!("http://localhost:{}", port);

    let q = Query::from_iter(&["dumpstors_cli", "-b", addr, "keyspaces", "create", "ks1"]);
    let result: QueryResult = execute(q).await.unwrap();
    assert_eq!(format!("{}", result), "");

    let q = Query::from_iter(&["dumpstors_cli", "-b", addr, "keyspaces", "get", "ks1"]);
    let result: QueryResult = execute(q).await.unwrap();
    assert_eq!(format!("{}", result), "Keyspace { name: \"ks1\" }");

    let q = Query::from_iter(&[
        "dumpstors_cli",
        "-b",
        addr,
        "insert",
        "--keyspace",
        "ks1",
        "key",
        "value",
    ]);
    let result: QueryResult = execute(q).await.unwrap();
    assert_eq!(format!("{}", result), "");

    let q = Query::from_iter(&[
        "dumpstors_cli",
        "-b",
        addr,
        "get",
        "--keyspace",
        "ks1",
        "key",
    ]);
    let result: QueryResult = execute(q).await.unwrap();
    assert_eq!(format!("{}", result), "key=value");

    let q = Query::from_iter(&[
        "dumpstors_cli",
        "-b",
        addr,
        "delete",
        "--keyspace",
        "ks1",
        "key",
    ]);
    let result: QueryResult = execute(q).await.unwrap();
    assert_eq!(format!("{}", result), "");

    let q = Query::from_iter(&["dumpstors_cli", "-b", addr, "keyspaces", "delete", "ks1"]);
    let result: QueryResult = execute(q).await.unwrap();
    assert_eq!(format!("{}", result), "");
}
