use log::{info, error, debug};
use env_logger::Env;
use std::{thread, time};

use easyamqp::rabbitclient::{self, RabbitConParams};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, Receiver};

async fn task(input: i32, client: rabbitclient::RabbitClient) {
    for _i in 0..100 {
        let output = client.dummy(input).await.unwrap();
        if input == output {
            info!("dummy :) ({})", input);
        } else {
            error!("dummy :( (input={}, output={})", input, output);
        }
    }
}

async fn main_multitask() {
            let mut client: rabbitclient::RabbitClient = rabbitclient::RabbitClient::new(None,None, 0).await;

            let task1_handle = tokio::spawn(task(14, client.clone()));
            let task2_handle = tokio::spawn(task(28, client.clone()));
            let task3_handle = tokio::spawn(task(13, client.clone()));
            let task4_handle = tokio::spawn(task(280, client.clone()));
            let task5_handle = tokio::spawn(task(1, client.clone()));
            let task6_handle = tokio::spawn(task(8, client.clone()));
            let task7_handle = tokio::spawn(task(114, client.clone()));
            let task8_handle = tokio::spawn(task(21, client.clone()));
            let task9_handle = tokio::spawn(task(99, client.clone()));
            let task10_handle = tokio::spawn(task(78, client.clone()));

            let _r = tokio::join!(task1_handle, task2_handle,
                task3_handle, task4_handle, task5_handle,
                task6_handle, task7_handle, task8_handle, task9_handle, task10_handle);
            client.close().await
}

async fn main_connection() {
    let con_params = Some(RabbitConParams{
        server: "127.0.0.1".to_string(),
        port: 5672,
        user: "guest".to_string(),
        password: "guest".to_string(),
    });
    let (tx_panic, mut rx_panic): (Sender<String>, Receiver<String>) = mpsc::channel(1);
    let mra = 3;
    let mut client: rabbitclient::RabbitClient = rabbitclient::RabbitClient::new(con_params,Some(tx_panic), mra).await;

    client.connect().await;

    let sleep_time = time::Duration::from_secs(2);
    thread::sleep(sleep_time);
    client.connect().await;
    client.connect().await;
    client.connect().await;
    client.connect().await;
    client.connect().await;



    debug!("I am running until I receive a panic request ... or get killed");
    if let Some(msg) = rx_panic.recv().await {
        error!("receive panic msg: {}", msg);
    }
    info!("close program");
    client.close().await
}


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
        main_connection().await;
    });
}