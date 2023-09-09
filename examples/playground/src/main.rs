use log::{info, error};
use env_logger::Env;

use easyamqp::rabbitclient;
use tokio::task::JoinError;

async fn task(input: i32, client: rabbitclient::RabbitClient) {
    for i in 0..100 {
        let output = client.dummy(input).await.unwrap();
        if input == output {
            info!("dummy :) ({})", input);
        } else {
            error!("dummy :( (input={}, output={})", input, output);
        }
    }
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
            let mut client: rabbitclient::RabbitClient = rabbitclient::RabbitClient::new().await;

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
        });
}
