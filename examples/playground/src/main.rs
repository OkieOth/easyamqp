use log::{info, error, debug};
use env_logger::Env;
use std::{thread, time};

use easyamqp::rabbitclient::{self, RabbitConParams, RabbitWorker};
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

    let _ = client.connect().await;

    let sleep_time = time::Duration::from_secs(2);
    thread::sleep(sleep_time);
    let _ = client.connect().await;
    let _ = client.connect().await;
    let _ = client.connect().await;
    let _ = client.connect().await;
    let _ = client.connect().await;



    debug!("I am running until I receive a panic request ... or get killed");
    if let Some(msg) = rx_panic.recv().await {
        error!("receive panic msg: {}", msg);
    }
    info!("close program");
    client.close().await
}

async fn task_create_channel(id: i32, mut client: rabbitclient::RabbitClient) -> RabbitWorker {
    
    info!("Try to create worker ...: Worker-{} ...", id);
    let worker1 = client.new_worker(id).await.unwrap();
    info!("Worker-{} created", id);
    worker1

    // let worker3 = client.new_worker(id).await.unwrap();
    // let worker4 = client.new_worker(id).await.unwrap();
    // let worker5 = client.new_worker(id).await.unwrap();
    // let worker6 = client.new_worker(id).await.unwrap();
    // let worker7 = client.new_worker(id).await.unwrap();
    // let worker8 = client.new_worker(id).await.unwrap();
    // let worker9 = client.new_worker(id).await.unwrap();
    // let worker10 = client.new_worker(id).await.unwrap();
    // let worker11 = client.new_worker(id).await.unwrap();
    // let worker12 = client.new_worker(id).await.unwrap();
    // let worker13 = client.new_worker(id).await.unwrap();
    // let worker14 = client.new_worker(id).await.unwrap();
    // let worker15 = client.new_worker(id).await.unwrap();
    // let worker16 = client.new_worker(id).await.unwrap();
    // let worker17 = client.new_worker(id).await.unwrap();
    // let worker18 = client.new_worker(id).await.unwrap();
    // let worker19 = client.new_worker(id).await.unwrap();
    // let worker20 = client.new_worker(id).await.unwrap();
    // info!("got worker: Thread-{}", id);
    //     loop {
    //         let sleep_time = time::Duration::from_secs(5);
    //         thread::sleep(sleep_time); 
    //         print_channel_info(id, &worker1).await;
    //         print_channel_info(id, &worker2).await;
    //         // info!("I am still here: Thread-{}, open-3: {}", id, worker3.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-4: {}", id, worker4.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-5: {}", id, worker5.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-6: {}", id, worker6.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-7: {}", id, worker7.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-8: {}", id, worker8.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-9: {}", id, worker9.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-10: {}", id, worker10.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-11: {}", id, worker11.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-12: {}", id, worker12.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-13: {}", id, worker13.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-14: {}", id, worker14.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-15: {}", id, worker15.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-16: {}", id, worker16.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-17: {}", id, worker17.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-18: {}", id, worker18.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-19: {}", id, worker19.channel.is_connection_open());
    //         // info!("I am still here: Thread-{}, open-20: {}", id, worker20.channel.is_connection_open());
    // }
}

async fn create_workers<'a>(client: &mut rabbitclient::RabbitClient, workers: &'a mut Vec<RabbitWorker>) {

    for id in 0..30 {
        let w = client.new_worker(id).await.unwrap();
        info!("Try to create worker ...: Worker-{}", id);
        workers.push(w);
    }
}

async fn main_channel() {
    let con_params = Some(RabbitConParams{
        server: "127.0.0.1".to_string(),
        port: 5672,
        user: "guest".to_string(),
        password: "guest".to_string(),
    });

    let (tx_panic, mut rx_panic): (Sender<String>, Receiver<String>) = mpsc::channel(1);
    let mra = 3;
    let mut client: rabbitclient::RabbitClient = rabbitclient::RabbitClient::new_with_name("playground".to_string(), con_params,Some(tx_panic), mra).await;

    let _ = client.connect().await;

    let mut workers: Vec<RabbitWorker> = Vec::new();
    create_workers(&mut client, &mut workers).await;


    debug!("I am running until I receive a panic request ... or get killed");
    let duration = time::Duration::from_secs(5);
    let mut t: tokio::time::Sleep = tokio::time::sleep(duration);
    loop {
        match rx_panic.try_recv() {
            Ok(msg) => {
                error!("receive panic msg: {}", msg);
                break;
            },
            Err(e) => {
                debug!("received TryRecvError: {}", e.to_string());
                for w in workers.iter() {
                    let (ready, con_is_open, chan_is_open) = w.get_state().await;
                    info!("   I am still here: Worker-{}, ready: {}, conn: {}, channel: {}", w.id, ready, con_is_open, chan_is_open);
                }
            }
        }
        t.await;
        t = tokio::time::sleep(duration);
        
    }
    info!("number of valid workers: {}", workers.len());
    info!("close program");
    client.close().await;
    info!("bye.");
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
        main_channel().await;
    });
}