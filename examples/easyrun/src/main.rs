use env_logger::Env;
use easyamqp::{RabbitClient, RabbitConParams,
    ExchangeDefinition, ExchangeType, 
    QueueDefinition, QueueBindingDefinition,
    Publisher};
use log::info;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};


fn get_exchange_def1() -> ExchangeDefinition {
    ExchangeDefinition { 
        name: "test_e_1".to_string(), 
        exchange_type: ExchangeType::Topic, 
        durable: false, 
        auto_delete: true }
}

fn get_exchange_def2() -> ExchangeDefinition {
    ExchangeDefinition { 
        name: "test_e_2".to_string(), 
        exchange_type: ExchangeType::Topic, 
        durable: false, 
        auto_delete: true }
}

fn get_exchange_def3() -> ExchangeDefinition {
    ExchangeDefinition { 
        name: "test_e_3".to_string(), 
        exchange_type: ExchangeType::Topic, 
        durable: false, 
        auto_delete: true }
}

fn get_queue_def1() -> QueueDefinition {
    QueueDefinition { 
        name: "test_q_1".to_string(), 
        durable: false,
        exclusive: false, 
        auto_delete: true }
}

fn get_queue_def2() -> QueueDefinition {
    QueueDefinition { 
        name: "test_q_2".to_string(), 
        durable: false,
        exclusive: false, 
        auto_delete: true }
}

fn get_queue_def3() -> QueueDefinition {
    QueueDefinition { 
        name: "test_q_3".to_string(), 
        durable: false,
        exclusive: false, 
        auto_delete: true }
}

fn get_queue_def4() -> QueueDefinition {
    QueueDefinition { 
        name: "test_q_4".to_string(), 
        durable: false,
        exclusive: false, 
        auto_delete: true }
}

fn get_binding_def1() -> QueueBindingDefinition {
    QueueBindingDefinition { 
        exchange: "test_e_1".to_string(), 
        queue: "test_q_1".to_string(),
        routing_key: "test.*".to_string()}
}

fn get_binding_def2() -> QueueBindingDefinition {
    QueueBindingDefinition { 
        exchange: "test_e_2".to_string(), 
        queue: "test_q_2".to_string(),
        routing_key: "test.*".to_string()}
}

fn get_binding_def3() -> QueueBindingDefinition {
    QueueBindingDefinition { 
        exchange: "test_e_3".to_string(), 
        queue: "test_q_3".to_string(),
        routing_key: "test.*".to_string()}
}

fn get_binding_def4() -> QueueBindingDefinition {
    QueueBindingDefinition { 
        exchange: "test_e_2".to_string(), 
        queue: "test_q_4".to_string(),
        routing_key: "test.*".to_string()}
}

fn main() {
    let env = Env::default().filter_or("LOG_LEVEL", "info");
    env_logger::init_from_env(env);

    let params = RabbitConParams::builder()
        .server("127.0.0.1")
        .user("guest")
        .password("guest")
        .build();
    
    
    //  {
    //     con_name: None,
    //     server: "127.0.0.1".to_string(),
    //     port: 5672,
    //     user: "guest".to_string(),
    //     password: "guest".to_string(),
    // };
    info!("started ...");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let (tx_panic, mut rx_panic): (Sender<String>, Receiver<String>) = mpsc::channel(1);

            info!("started 1");
            let mut client = RabbitClient::new(params).await;
            info!("started 2");
            client.connect().await.unwrap();
            client.set_panic_sender(tx_panic).await;

            // define topology
            client.declare_exchange(get_exchange_def1()).await.unwrap();
            client.declare_exchange(get_exchange_def2()).await.unwrap();
            client.declare_exchange(get_exchange_def3()).await.unwrap();

            client.declare_queue(get_queue_def1()).await.unwrap();
            client.declare_queue(get_queue_def2()).await.unwrap();
            client.declare_queue(get_queue_def3()).await.unwrap();
            client.declare_queue(get_queue_def4()).await.unwrap();

            client.declare_queue_binding(get_binding_def1()).await.unwrap();
            client.declare_queue_binding(get_binding_def2()).await.unwrap();
            client.declare_queue_binding(get_binding_def3()).await.unwrap();
            client.declare_queue_binding(get_binding_def4()).await.unwrap();

            let p1 = client.new_publisher().await.unwrap();
            let p2 = client.new_publisher().await.unwrap();
            let p3 = client.new_publisher().await.unwrap();
            let p4 = client.new_publisher().await.unwrap();
            let p5 = client.new_publisher().await.unwrap();
            let p6 = client.new_publisher().await.unwrap();
            let p7 = client.new_publisher().await.unwrap();
            let p8 = client.new_publisher().await.unwrap();
            let p9 = client.new_publisher().await.unwrap();
            let p10 = client.new_publisher().await.unwrap();
            let p11 = client.new_publisher().await.unwrap();
            let p12 = client.new_publisher().await.unwrap();
            let p13 = client.new_publisher().await.unwrap();
            let p14 = client.new_publisher().await.unwrap();
            let p15 = client.new_publisher().await.unwrap();
            let p16 = client.new_publisher().await.unwrap();
            let p17 = client.new_publisher().await.unwrap();
            let p18 = client.new_publisher().await.unwrap();
            let p19 = client.new_publisher().await.unwrap();
            let p20 = client.new_publisher().await.unwrap();

            info!("started 3");
            match rx_panic.recv().await {
                Some(msg) => info!("Received panic request: {}", msg),
                None => info!("Received panic request w/o message"),
            }

            info!("done.");
        });
}

