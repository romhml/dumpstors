pub mod store;

pub mod models {
    use super::store;

    tonic::include_proto!("dumpstors.models");

    impl From<store::keyspace::Keyspace> for Keyspace {
        fn from(ks: store::keyspace::Keyspace) -> Keyspace {
            Keyspace { name: ks.name }
        }
    }
}
