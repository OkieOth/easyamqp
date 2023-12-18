use std::sync::Arc;

use env_logger::Env;
use log::{info, error};

use easyamqp::{
    RabbitClient, SubscribeParams, QueueDefinition, 
    ExchangeDefinition, QueueBindingDefinition,
    PublisherParams, PublishingParams};
use tokio::task::JoinHandle;

async fn get_client(con_name: &str) -> RabbitClient {
    let (mut client, _) = RabbitClient::get_default_client_with_name(con_name).await;
    client.connect().await.unwrap();
    info!("rabbit client connected");
    client
}

async fn init_exchange(client: &RabbitClient, exchange_name: &str) {
    let exchange_def = ExchangeDefinition::builder(exchange_name)
        .durable(true)
        .build();
    if let Err(msg) = client.declare_exchange(exchange_def).await {
        panic!("Error while create subscriber exchange: {}", msg);
    }
}

async fn do_subscribe() -> usize {
    let exchange_name = "simple_pubsub_exchange";
    let routing_key = "test.*";
    let queue_name = "simple_pubsub_sub_queue";
    let client = get_client("simple_pubsub_sub").await;

    let queue_def = QueueDefinition::builder(queue_name).build(); 
    if let Err(msg) = client.declare_queue(queue_def).await {
        panic!("Error while create subscriber queue: {}", msg);
    };
    let binding_def = QueueBindingDefinition::new(queue_name, exchange_name, routing_key);
    if let Err(msg) = client.declare_queue_binding(binding_def).await {
        panic!("Error while create queue binding for subscription: {}", msg);
    }

    let params = SubscribeParams::builder(queue_name, "simple_pubsub_sub")
        .auto_ack(true)
        .build();
    let mut subscr = client.new_subscriber(params).await.unwrap();
    match subscr.subscribe_with_auto_ack().await {
        Ok(recv) => {
            let mut ret: usize = 0;
            for _ in 0 .. 10 {
                match recv.recv().await {
                    Some(content) => {
                        let mut buf = [0u8; std::mem::size_of::<usize>()];
                        buf.copy_from_slice(&content.data[..std::mem::size_of::<usize>()]);
                        let value = usize::from_le_bytes(buf);
                        info!("received content: {}", value);
                        ret += value;
                    },
                    None => {
                        error!("content channel closed - finish");
                        return 0;
                    },
                }
            }
            return ret;
        },
        Err(e) => {
            panic!("Error while subscribe: {}", e);
        },
    }
}

async fn do_publish() -> usize {
    let client = get_client("simple_pubsub_pub").await;
    let exchange_name = "simple_pubsub_exchange";
    init_exchange(&client, exchange_name).await;
    let params = PublisherParams::builder()
        .exchange(exchange_name)
        .build();
    match client.new_publisher_from_params(params).await {
        Ok(p) => {
            let mut ret = 0;
            for i in 0 .. 10 {
                let u: usize = i as usize;
                ret += u;
                let pp = PublishingParams::builder()
                    .routing_key(&format!("test.{}", i))
                    .build();
                if let Err(msg) = p.publish_with_params(u.to_le_bytes().to_vec(), &pp).await {
                    error!("Error while publishing: {}", msg);
                }
            }
            ret
        },
        Err(msg) => {
            panic!("Error while creating a publisher: {}", msg)
        },
    }
}

fn main() {
    let env = Env::default().filter_or("LOG_LEVEL", "info");
    env_logger::init_from_env(env);

    info!("'simple pub/sub example started");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {

        let subscriber_task: JoinHandle<usize> = tokio::spawn(async move {
            do_subscribe().await
        });

        let publisher_task: JoinHandle<usize> = tokio::spawn(async move {
            // Publisher task
            do_publish().await
        });

        let resp1 = publisher_task.await.unwrap();
        let resp2 = subscriber_task.await.unwrap();
        assert_eq!(resp1, resp2);
    });

}
