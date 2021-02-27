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
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.set("listen_addr", "0.0.0.0")?;
        s.set("port", "4242")?;
        s.set("store.path", "/var/lib/dumpstors/data")?;

        s.try_into()
    }
}
