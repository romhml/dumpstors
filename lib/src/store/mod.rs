use sled::Error as SledError;
use std::io::Error as IoError;
use std::result::Result as StdResult;
use tonic::{transport, Code, Status};

tonic::include_proto!("dumpstors.store");

#[derive(Debug)]
pub enum Error {
    SledErr(SledError),
    IoErr(IoError),

    TonicStatus(Status),
    TransportErr(transport::Error),

    KeyspaceNotFound,
    KeyspaceAlreadyExists,
    KeyNotFound,
}

impl From<Status> for Error {
    fn from(err: Status) -> Self {
        Error::TonicStatus(err)
    }
}

impl From<transport::Error> for Error {
    fn from(err: transport::Error) -> Self {
        Error::TransportErr(err)
    }
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
