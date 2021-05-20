use config::{Config, ConfigError};

#[derive(Debug, Deserialize)]
pub struct Store {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub listen_addr: String,
    pub port: u16,
    pub store: Store,

    pub seeds: Vec<String>,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.set("listen_addr", "127.0.0.1")?;
        s.set("port", "4243")?;
        s.set("store.path", ".data/")?;

        s.set(
            "seeds",
            vec!["http://localhost:4242", "http://localhost:4243"],
        )?;
        s.try_into()
    }
}
