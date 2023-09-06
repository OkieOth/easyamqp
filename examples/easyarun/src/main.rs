use log::{info};
use env_logger::Env;

use easyamqp::rabbitclient;

fn main() {
    let env = Env::default()
    .filter_or("LOG_LEVEL", "info");

    env_logger::init_from_env(env);

    println!("I am running in loglevel: {}", log::max_level());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on( async {
            let mut client = rabbitclient::RabbitClient::new().await;
            let dummy_result = client.dummy(13).await.unwrap();

            info!("received from dummy: {}", dummy_result);
            client.close().await;
        });
}
