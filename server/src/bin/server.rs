use dumpstors::{settings::Settings, start_server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let conf = Settings::new().unwrap();

    start_server(conf).await
}
