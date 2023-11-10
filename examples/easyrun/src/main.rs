use env_logger::Env;
use easyamqp::{RabbitClient, RabbitConParams,
    ExchangeDefinition, ExchangeType, 
    QueueDefinition, QueueBindingDefinition,
    Publisher, PublishingParams,
    Subscriber, SubscribeParams, SubscriptionContent, SubscriptionResponse};
use log::{info, error};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};


fn get_exchange_def1() -> ExchangeDefinition {
    ExchangeDefinition { 
        name: "test_e_1".to_string(), 
        exchange_type: ExchangeType::Topic, 
        durable: false, 
        auto_delete: false }
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
        auto_delete: false }
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

            let p1: Publisher = client.new_publisher().await.unwrap();
            // let _p2: Publisher = client.new_publisher().await.unwrap();
            // let _p3: Publisher = client.new_publisher().await.unwrap();
            // let _p4: Publisher = client.new_publisher().await.unwrap();
            // let _p5: Publisher = client.new_publisher().await.unwrap();
            // let _p6: Publisher = client.new_publisher().await.unwrap();
            // let _p7: Publisher = client.new_publisher().await.unwrap();
            // let _p8: Publisher = client.new_publisher().await.unwrap();
            // let _p9: Publisher = client.new_publisher().await.unwrap();
            // let _p10: Publisher = client.new_publisher().await.unwrap();
            // let _p11: Publisher = client.new_publisher().await.unwrap();
            // let _p12: Publisher = client.new_publisher().await.unwrap();
            // let _p13: Publisher = client.new_publisher().await.unwrap();
            // let _p14: Publisher = client.new_publisher().await.unwrap();
            // let _p15: Publisher = client.new_publisher().await.unwrap();
            // let _p16: Publisher = client.new_publisher().await.unwrap();
            // let _p17: Publisher = client.new_publisher().await.unwrap();
            // let _p18: Publisher = client.new_publisher().await.unwrap();
            // let _p19: Publisher = client.new_publisher().await.unwrap();
            // let _p20: Publisher = client.new_publisher().await.unwrap();


            info!("do publishing ...");
            // exchange: "test_e_1".to_string(), 
            // queue: "test_q_1".to_string(),
            // routing_key: "test.*".to_string()}

            let params = PublishingParams::builder()
                .exchange("test_e_1")
                .routing_key("test.p10")
                .build();

            tokio::spawn(async move {
                let content = String::from(
                    r#"
                        {
                            "publisher": "example"
                            "data": "Hello, amqprs!"
                        }
                    "#,
                )
                .into_bytes();
                loop {
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                    if let Err(e) = p1.publish_with_params(content.clone(), &params).await {
                        error!("error while publishing: {}", e.to_string());
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            });

            let sub_params = SubscribeParams::builder("test_q_1", "test_q_1.consumer")
            .auto_ack(false)
            .exclusive(true)
            .build();
            let mut subscriber: Subscriber;
            if let Ok(s) = client.new_subscriber(sub_params).await {
                subscriber = s;
            } else {
                panic!("error while create new subscriber");
            }
    

            tokio::spawn(async move {
                let rx_content: &mut Receiver<SubscriptionContent>;
                let tx_response: &Sender<SubscriptionResponse>;
                if let Ok((rxc,txr )) = subscriber.subscribe().await {
                    rx_content = rxc;
                    tx_response = txr;
                } else {
                    return;
                }
                loop {
                    match rx_content.recv().await {
                        Some(content) => {
                            info!("received content :)");
                            let _ = tx_response.send(SubscriptionResponse { delivery_tag: content.delivery_tag, ack: true }).await;
                        },
                        None => {
                            error!("content channel closed - finish");
                            return;
                        },
                    }
                }
            });

            info!("started 3");
            match rx_panic.recv().await {
                Some(msg) => info!("Received panic request: {}", msg),
                None => info!("Received panic request w/o message"),
            }

            info!("done. worker_count={}", client.get_worker_count().await);
        });
}

