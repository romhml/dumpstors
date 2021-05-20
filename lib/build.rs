use std::error::Error;
use std::process::exit;

#[cfg(not(feature = "structopt"))]
fn compile_prototypes() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        // .format(false) // disable code formatting since docs.rs will otherwise break
        .compile(
            &[
                "proto/models.proto",
                "proto/store.proto",
                "proto/node.proto",
            ],
            &["."],
        )?;
    Ok(())
}

fn main() {
    if let Err(err) = compile_prototypes() {
        eprintln!("{}", err);
        exit(1);
    }

    println!("cargo:rerun-if-changed=proto/store.proto");
    println!("cargo:rerun-if-changed=proto/node.proto");
    println!("cargo:rerun-if-changed=proto/models.proto");
}
