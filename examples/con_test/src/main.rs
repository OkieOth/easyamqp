use env_logger::Env;
use log::{debug, error, info};
use std::time;

use easyamqp::rabbitclient::{self, RabbitConParams, RabbitWorker};
use easyamqp::utils::get_env_var_str;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

async fn create_workers<'a>(
    client: &mut rabbitclient::RabbitClient,
    workers: &'a mut Vec<RabbitWorker>,
) {
    for id in 0..30 {
        let w = client.new_worker(id).await.unwrap();
        info!("Try to create worker ...: Worker-{}", id);
        workers.push(w);
    }
}

async fn main_channel() {
    let user_name = get_env_var_str("RABBIT_USER", "guest");
    let password = get_env_var_str("RABBIT_PASSWORD", "guest");
    let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");
    let connection_name = get_env_var_str("CONN_NAME", "con_test");

    let con_params = Some(RabbitConParams {
        server: rabbit_server,
        port: 5672,
        user: user_name,
        password,
    });

    let (tx_panic, mut rx_panic): (Sender<String>, Receiver<String>) = mpsc::channel(1);
    let mra = 3;
    let mut client: rabbitclient::RabbitClient =
        rabbitclient::RabbitClient::new_with_name(connection_name, con_params, Some(tx_panic), mra)
            .await;

    let _ = client.connect().await;

    let mut workers: Vec<RabbitWorker> = Vec::new();

    tokio::spawn(async move {
        let s = rx_panic.recv().await.unwrap();
        error!("received panic request and will panic ...");
        panic!("{}", s);
    });

    create_workers(&mut client, &mut workers).await;

    debug!("I am running until I receive a panic request ... or get killed");
    let duration = time::Duration::from_secs(5);
    let mut t: tokio::time::Sleep = tokio::time::sleep(duration);

    loop {
        for w in workers.iter() {
            let (ready, con_is_open, chan_is_open) = w.get_state().await;
            info!(
                "   I am still here: Worker-{}, ready: {}, conn: {}, channel: {}",
                w.id, ready, con_is_open, chan_is_open
            );
        }

        t.await;
        t = tokio::time::sleep(duration);
    }
}

fn main() {
    let env = Env::default().filter_or("LOG_LEVEL", "info");
    let mut i = 0;
    env_logger::init_from_env(env);
    i += 1;
    println!("I am running in loglevel: {}", log::max_level());
    i += 1;
    println!("dummy: i={}", i);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            main_channel().await;
        });
}
