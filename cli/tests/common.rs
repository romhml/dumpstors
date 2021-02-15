use dumpstors;

pub async fn setup() -> Result<(), Box<dyn std::error::Error>> {
    let conf = dumpstors::settings::Settings {
        listen_addr: "127.0.0.1".to_string(),
        port: 4242,
        store: dumpstors::settings::Store {
            path: "./.data".to_string(),
        },
    };

    tokio::spawn(async move {
        dumpstors::start_server(conf).await.unwrap();
    });

    Ok(())
}
