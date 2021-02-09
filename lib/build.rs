use std::error::Error;
use std::process::exit;

fn run() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        // .format(false) // disable code formatting since docs.rs will otherwise break
        .compile(&["proto/models.proto", "proto/store.proto"], &["."])?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        exit(1);
    }

    println!("cargo:rerun-if-changed=proto/");
}
