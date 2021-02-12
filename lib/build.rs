use std::error::Error;
use std::process::exit;

fn main() {
    tonic_build::configure()
        .format(false) // disable code formatting since docs.rs will otherwise break
        .compile(&["proto/models.proto", "proto/store.proto"], &["."]).unwrap();

    println!("cargo:rerun-if-changed=proto/models.proto");
    println!("cargo:rerun-if-changed=proto/store.proto");
}
