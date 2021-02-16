use dumpstors;
use uuid::Uuid;

pub async fn start_ephemeral_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let conf = dumpstors::settings::Settings {
        listen_addr: "127.0.0.1".to_string(),
        port: port,
        store: dumpstors::settings::Store {
            path: format!("./.data/{}", Uuid::new_v4()),
        },
    };

    tokio::spawn(async move {
        dumpstors::start_server(conf).await.unwrap();
    });

    Ok(())
}
