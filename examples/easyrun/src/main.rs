use env_logger::Env;
use easyamqp::rabbitclient;
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

fn main() {
    let env = Env::default().filter_or("LOG_LEVEL", "info");
    env_logger::init_from_env(env);

    let params = rabbitclient::RabbitConParams {
        con_name: None,
        server: "127.0.0.1".to_string(),
        port: 5672,
        user: "guest".to_string(),
        password: "guest".to_string(),
    };
    info!("started ...");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let (tx_panic, mut rx_panic): (Sender<u32>, Receiver<u32>) = mpsc::channel(1);

            info!("started 1");
            let client = rabbitclient::RabbitClient::new(params).await;
            info!("started 2");
            client.connect().await.unwrap();
            info!("started 3");
            rx_panic.recv().await;
            info!("started 4");
        });
}

