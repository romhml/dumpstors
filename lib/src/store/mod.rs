use sled::Error as SledError;
use std::io::Error as IoError;
use std::result::Result as StdResult;
use tonic::{Code, Status};

tonic::include_proto!("dumpstors.store");

#[derive(Debug)]
pub enum Error {
    SledErr(SledError),
    IoErr(IoError),

    KeyspaceNotFound,
    KeyspaceAlreadyExists,
    KeyNotFound,
}

impl From<SledError> for Error {
    fn from(err: SledError) -> Self {
        Error::SledErr(err)
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Self {
        Error::IoErr(err)
    }
}

pub type Result<T> = StdResult<T, Error>;

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        match err {
            Error::KeyspaceNotFound => Self::new(Code::NotFound, "Keyspace not found"),
            Error::KeyspaceAlreadyExists => {
                Self::new(Code::AlreadyExists, "Keyspace already exists")
            }
            Error::KeyNotFound => Self::new(Code::NotFound, "Key not found"),
            _ => Self::new(Code::Internal, "Internal Error"),
        }
    }
}
