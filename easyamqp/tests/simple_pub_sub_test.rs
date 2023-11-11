use easyamqp::{RabbitClient, ExchangeDefinition,
    QueueDefinition, QueueBindingDefinition,
    Publisher, PublisherParams, 
    Subscriber, SubscribeParams, SubscriptionContent};
use tokio::time::{timeout, Duration};
use tokio::sync::mpsc::Receiver;
use tokio::task;
use log::error;


#[test]
#[ignore]
fn test_simple_pub_sub() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        let (mut client, _) = RabbitClient::get_default_client().await;
        client.connect().await.unwrap();

        let exchange_name = "test_simple_pub_sub";
        let queue_name = "test_simple_pub_sub.queue";
        let routing_key = "test";

        let exchange_def = ExchangeDefinition::builder(exchange_name).build();
        let _ = client.declare_exchange(exchange_def).await;
        
        let queue_def = QueueDefinition::builder(queue_name)
            .durable(true)
            .exclusive(true)
            .auto_delete(true)
            .build();
        let _ = client.declare_queue(queue_def).await;
        let binding_def = QueueBindingDefinition::new(queue_name, exchange_name, routing_key); 
        let _ = client.declare_queue_binding(binding_def).await;

        let pub_params = PublisherParams::builder()
            .exchange(exchange_name)
            .routing_key(routing_key)
            .build();

        let p1: Publisher = client.new_publisher_from_params(pub_params).await.unwrap();
        assert_eq!(1, client.get_worker_count().await);

        task::spawn(async move {
            let content = String::from(
                r#"
                    {
                        "publisher": "example"
                        "data": "Hello, amqprs!"
                    }
                "#,
            )
            .into_bytes();
            for i in 0 .. 10 {
                if let Err(e) = p1.publish(content.clone()).await {
                    error!("error while publishing {i}: {}", e.to_string());
                }
            }
        });

        let sub_params = SubscribeParams::builder(queue_name, "test_simple_pub_sub")
            .auto_ack(true)
            .exclusive(true)
            .build();

        let mut subscriber: Subscriber;
        if let Ok(s) = client.new_subscriber(sub_params).await {
            subscriber = s;
        } else {
            assert!(false);
            return;
        }
        let rx_content: &mut Receiver<SubscriptionContent>;
        if let Ok(rxc ) = subscriber.subscribe_with_auto_ack().await {
            rx_content = rxc;
        } else {
            assert!(false);
            return;
        }
        let mut received_count = 0;
        let mut failure_count = 0;
        loop {
            const TIMEOUT_SECS: u64 = 3;
            match timeout(Duration::from_secs(TIMEOUT_SECS), rx_content.recv()).await {
                Ok(timeout_result) => {
                    match timeout_result {
                        Some(_) => {
                            received_count += 1;
                        },
                        None => {
                            error!("didn't receive proper subscription response");
                            failure_count += 1;
                        },
                    }
                },
                Err(_) => {
                    // timeout
                    error!("didn't receive subscription response in timeout ({} s)", TIMEOUT_SECS);
                    failure_count += 1;
                },
            }

            if (received_count == 10) || (failure_count == 10) {
                break;
            }
        }
        assert_eq!(10, received_count);
    });
}
