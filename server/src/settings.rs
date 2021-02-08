use config::{Config, ConfigError};

#[derive(Debug, Deserialize)]
pub struct Store {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub listen_addr: String,
    pub port: u32,
    pub store: Store,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.set("listen_addr", "127.0.0.1")?;
        s.set("port", "4242")?;
        s.set("store.path", "./.data")?;

        s.try_into()
    }
}
