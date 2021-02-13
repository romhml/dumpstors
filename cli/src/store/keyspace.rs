use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub enum KeyspaceCommand {
    Create(CreateKeyspaceOpt),
    Get(GetKeyspaceOpt),
    Delete(DeleteKeyspaceOpt),
}

#[derive(Debug, StructOpt)]
pub struct CreateKeyspaceOpt {
    pub keyspace: String,
}

#[derive(Debug, StructOpt)]
pub struct GetKeyspaceOpt {
    pub keyspace: String,
}

#[derive(Debug, StructOpt)]
pub struct DeleteKeyspaceOpt {
    pub keyspace: String,
}
