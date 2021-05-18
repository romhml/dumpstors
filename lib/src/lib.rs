pub mod store;

#[cfg(feature = "structopt")]
extern crate structopt;

pub mod models {
    tonic::include_proto!("dumpstors.models");
}
