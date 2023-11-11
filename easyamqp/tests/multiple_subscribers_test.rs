use easyamqp::{RabbitClient, RabbitConParams, 
    ExchangeDefinition, ExchangeType,
    QueueDefinition, QueueBindingDefinition,
    Publisher, PublisherParams, 
    Subscriber, SubscribeParams, SubscriptionContent, SubscriptionResponse};
use easyamqp::utils::get_env_var_str;
use serde_json::Value;
use serde_json_path::JsonPath;
use tokio::time::{timeout, sleep, Duration};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;
use tokio::task;
use log::error;


#[test]
#[ignore]
fn test_multiple_subscribers() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        let (mut client, _) = RabbitClient::get_default_client().await;
        client.connect().await.unwrap();

        let exchange_name = "test_multiple_subscribers";
        let queue_name = "test_multiple_subscribers.queue";
        let routing_key = "test";

        let exchange_def = ExchangeDefinition::builder(exchange_name).build();
        let _ = client.declare_exchange(exchange_def).await;

        let queue_def = QueueDefinition::builder(queue_name)
            .durable(false)
            .exclusive(false)
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
            for i in 0 .. 100 {
                if let Err(e) = p1.publish(content.clone()).await {
                    error!("error while publishing {i}: {}", e.to_string());
                }
            }
        });


        let sub_params_1 = SubscribeParams::builder(queue_name, "subscriber_1")
            .auto_ack(false)
            .exclusive(false)
            .build();
        let mut subscriber_1: Subscriber;
        if let Ok(s) = client.new_subscriber(sub_params_1).await {
            subscriber_1 = s;
        } else {
            assert!(false);
            return;
        }
        let sub_params_2 = SubscribeParams::builder(queue_name, "subscriber_2")
            .auto_ack(false)
            .exclusive(false)
            .build();
        let mut subscriber_2: Subscriber;
        if let Ok(s) = client.new_subscriber(sub_params_2).await {
            subscriber_2 = s;
        } else {
            assert!(false);
            return;
        }

        let mut received_count_1 = 0;
        let mut failure_count_1 = 0;
        let mut received_count_2 = 0;
        let mut failure_count_2 = 0;
 
        let rx_content_1: &mut Receiver<SubscriptionContent>;
        let tx_response_1: &Sender<SubscriptionResponse>;
        match subscriber_1.subscribe().await {
            Ok((r, txr)) => {
                rx_content_1 = r;
                tx_response_1 = txr;
            },
            Err(e) => {
                print!("{}", e.to_string());
                assert!(false);
                return;
            }
        }
        let rx_content_2: &mut Receiver<SubscriptionContent>;
        let tx_response_2: &Sender<SubscriptionResponse>;
        if let Ok((rxc, txr) ) = subscriber_2.subscribe().await {
            rx_content_2 = rxc;
            tx_response_2 = txr;
        } else {
            assert!(false);
            return;
        }

        let sleep_obj = sleep(Duration::from_secs(5));
        tokio::pin!(sleep_obj);

        'outer: loop {
            const TIMEOUT_SECS: u64 = 3;
            tokio::select! {
                res_1 = rx_content_1.recv() => {
                    match res_1 {
                        Some(x) => {
                            received_count_1 += 1;
                            sleep(Duration::from_millis(TIMEOUT_SECS)).await;
                            let _ = tx_response_1.send(SubscriptionResponse::new(
                                x.delivery_tag,
                                true
                            )).await;
                        },
                        None => failure_count_1 += 1,
                    }
                },
                res_2 = rx_content_2.recv() => {
                    match res_2 {
                        Some(x) => {
                            received_count_2 += 1;
                            sleep(Duration::from_millis(TIMEOUT_SECS)).await;
                            let _ = tx_response_2.send(SubscriptionResponse::new(
                                x.delivery_tag,
                                true
                            )).await;
                        },
                        None => failure_count_2 += 1,
                    }
                },
                _ = &mut sleep_obj => {
                    println!("timeout reached");
                    break 'outer;
                }
            }
        }
        assert!(received_count_1>0);
        assert!(received_count_2>0);
        assert_eq!(received_count_1 + received_count_2, 100);
    });
}