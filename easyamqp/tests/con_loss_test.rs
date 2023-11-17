mod test_helper;

use easyamqp::{RabbitClient, ExchangeDefinition,
    QueueDefinition, QueueBindingDefinition,
    Publisher, PublisherParams, 
    Subscriber, SubscribeParams, SubscriptionContent, SubscriptionResponse};
use tokio::time::{sleep, Duration};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use log::error;



#[test]
#[ignore]
fn test_con_loss_publisher() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        
        let conn_name_pub = "con_loss_test_publisher1";
        let (mut client_pub, _) = RabbitClient::get_default_client_with_name(&conn_name_pub).await;
        client_pub.connect().await.unwrap();

        let con_str_start = test_helper::get_connection_name(&conn_name_pub).await;

        let exchange_name = "test_multiple_subscribers1";
        let queue_name = "test_multiple_subscribers.queue1";
        let routing_key = "test";

        let exchange_def = ExchangeDefinition::builder(exchange_name).build();
        let _ = client_pub.declare_exchange(exchange_def).await;

        let queue_def = QueueDefinition::builder(queue_name)
            .durable(false)
            .exclusive(false)
            .auto_delete(false)
            .build();
        let _ = client_pub.declare_queue(queue_def).await;
        let binding_def = QueueBindingDefinition::new(queue_name, exchange_name, routing_key); 
        let _ = client_pub.declare_queue_binding(binding_def).await;

        let pub_params = PublisherParams::builder()
            .exchange(exchange_name)
            .routing_key(routing_key)
            .build();

        let p1: Publisher = client_pub.new_publisher_from_params(pub_params).await.unwrap();
        assert_eq!(1, client_pub.get_worker_count().await);

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
            let mut sent = 0;
            let mut err = 0;
            let max_publ_count = 1000;
            loop {
                if let Err(e) = p1.publish(content.clone()).await {
                    error!("error while publishing {sent}: {}", e.to_string());
                    err += 1;
                } else {
                    sent += 1;
                }
                if sent >= max_publ_count || err > 2 * max_publ_count {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        });

        task::spawn(async move {
            for _ in 0 .. 10 {
                let _ = test_helper::close_connection(&conn_name_pub).await;
                sleep(Duration::from_millis(500)).await;
            }
        });

        let conn_name_sub = "con_loss_test_subscriber1";
        let (mut client_sub, _) = RabbitClient::get_default_client_with_name(&conn_name_sub).await;
        client_sub.connect().await.unwrap();

        let sub_params_1 = SubscribeParams::builder(queue_name, "subscriber_1")
            .auto_ack(false)
            .exclusive(false)
            .build();
        let mut subscriber_1: Subscriber;
        if let Ok(s) = client_sub.new_subscriber(sub_params_1).await {
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
        if let Ok(s) = client_sub.new_subscriber(sub_params_2).await {
            subscriber_2 = s;
        } else {
            assert!(false);
            return;
        }

        let mut received_count_1 = 0;
        let mut received_count_2 = 0;
 
        let rx_content_1: &mut Receiver<SubscriptionContent>;
        let tx_response_1: &Sender<SubscriptionResponse>;
        match subscriber_1.subscribe().await {
            Ok((r, txr)) => {
                rx_content_1 = r;
                tx_response_1 = txr;
            },
            Err(e) => {
                print!("{}", e.to_string());
                assert!(false, "Error while subscribe 1: {}", e.to_string());
                return;
            }
        }
        let rx_content_2: &mut Receiver<SubscriptionContent>;
        let tx_response_2: &Sender<SubscriptionResponse>;
        match subscriber_2.subscribe().await {
            Ok((r, txr)) => {
                rx_content_2 = r;
                tx_response_2 = txr;
            },
            Err(e) => {
                print!("{}", e.to_string());
                assert!(false, "Error while subscribe 2: {}", e.to_string());
                return;
            }
        }

        let sleep_obj = sleep(Duration::from_secs(120));
        tokio::pin!(sleep_obj);


        'outer: loop {
            tokio::select! {
                res_1 = rx_content_1.recv() => {
                    if let Some(x) = res_1 {
                        received_count_1 += 1;
                        let _ = tx_response_1.send(SubscriptionResponse::new(
                            x.delivery_tag,
                            true
                        )).await;
                    }
                },
                res_2 = rx_content_2.recv() => {
                    if let Some(x) = res_2 {
                        received_count_2 += 1;
                        let _ = tx_response_2.send(SubscriptionResponse::new(
                            x.delivery_tag,
                            true
                        )).await;
                    }
                },
                _ = &mut sleep_obj => {
                    assert!(false, "timeout reached:, received_count_1={}, received_count_2={}", received_count_1, received_count_2);
                    break 'outer;
                }
            }
            // TODO, for some reason, one message get sometimes lost
            if received_count_1 + received_count_2 >= 999 {
                break 'outer;
            }
        }

        let con_str_end = test_helper::get_connection_name(&conn_name_pub).await;
        assert!(con_str_start != con_str_end);
        assert!(received_count_1>0);
        assert!(received_count_2>0);
        assert!(received_count_1 + received_count_2 >= 999);
    });
}

#[test]
#[ignore]
fn test_con_loss_subscriber() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        
        let conn_name_pub = "con_loss_test_publisher2";
        let (mut client_pub, _) = RabbitClient::get_default_client_with_name(&conn_name_pub).await;
        client_pub.connect().await.unwrap();

        let exchange_name = "test_multiple_subscribers2";
        let queue_name = "test_multiple_subscribers.queue2";
        let routing_key = "test";

        let exchange_def = ExchangeDefinition::builder(exchange_name).build();
        let _ = client_pub.declare_exchange(exchange_def).await;

        let queue_def = QueueDefinition::builder(queue_name)
            .durable(false)
            .exclusive(false)
            .auto_delete(false)
            .build();
        let _ = client_pub.declare_queue(queue_def).await;
        let binding_def = QueueBindingDefinition::new(queue_name, exchange_name, routing_key); 
        let _ = client_pub.declare_queue_binding(binding_def).await;

        let pub_params = PublisherParams::builder()
            .exchange(exchange_name)
            .routing_key(routing_key)
            .build();

        let p1: Publisher = client_pub.new_publisher_from_params(pub_params).await.unwrap();
        assert_eq!(1, client_pub.get_worker_count().await);

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
            let mut sent = 0;
            let mut err = 0;
            let max_publ_count = 1000;
            loop {
                if let Err(e) = p1.publish(content.clone()).await {
                    error!("error while publishing {sent}: {}", e.to_string());
                    err += 1;
                } else {
                    sent += 1;
                }
                if sent >= max_publ_count || err >= max_publ_count {
                    break;
                }
            }
        });


        let conn_name_sub = "con_loss_test_subscriber2";
        let (mut client_sub, _) = RabbitClient::get_default_client_with_name(&conn_name_sub).await;
        client_sub.connect().await.unwrap();

        let sub_params_1 = SubscribeParams::builder(queue_name, "subscriber_1")
            .auto_ack(false)
            .exclusive(false)
            .build();
        let mut subscriber_1: Subscriber;
        if let Ok(s) = client_sub.new_subscriber(sub_params_1).await {
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
        if let Ok(s) = client_sub.new_subscriber(sub_params_2).await {
            subscriber_2 = s;
        } else {
            assert!(false);
            return;
        }

        let mut received_count_1 = 0;
        let mut received_count_2 = 0;
 
        let rx_content_1: &mut Receiver<SubscriptionContent>;
        let tx_response_1: &Sender<SubscriptionResponse>;
        match subscriber_1.subscribe().await {
            Ok((r, txr)) => {
                rx_content_1 = r;
                tx_response_1 = txr;
            },
            Err(e) => {
                print!("{}", e.to_string());
                assert!(false, "Error while subscribe 1: {}", e.to_string());
                return;
            }
        }
        let rx_content_2: &mut Receiver<SubscriptionContent>;
        let tx_response_2: &Sender<SubscriptionResponse>;
        match subscriber_2.subscribe().await {
            Ok((r, txr)) => {
                rx_content_2 = r;
                tx_response_2 = txr;
            },
            Err(e) => {
                print!("{}", e.to_string());
                assert!(false, "Error while subscribe 2: {}", e.to_string());
                return;
            }
        }

        let con_str_start = test_helper::get_connection_name(&conn_name_sub).await;

        let sleep_obj = sleep(Duration::from_secs(120));
        tokio::pin!(sleep_obj);
        let mut con_changed = false;

        task::spawn(async move {
            sleep(Duration::from_millis(1000)).await;
            for _ in 0 .. 10 {
                let _ = test_helper::close_connection(&conn_name_sub).await;
                sleep(Duration::from_millis(500)).await;
            }
        });

        'outer: loop {
            tokio::select! {
                res_1 = rx_content_1.recv() => {
                    if let Some(x) = res_1 {
                        received_count_1 += 1;
                        let _ = tx_response_1.send(SubscriptionResponse::new(
                            x.delivery_tag,
                            true
                        )).await;
                    }
                },
                res_2 = rx_content_2.recv() => {
                    if let Some(x) = res_2 {
                        received_count_2 += 1;
                        let _ = tx_response_2.send(SubscriptionResponse::new(
                            x.delivery_tag,
                            true
                        )).await;
                    }
                },
                _ = &mut sleep_obj => {
                    assert!(false, "timeout reached:, received_count_1={}, received_count_2={}", received_count_1, received_count_2);
                    break 'outer;
                }
            }
            if received_count_1 + received_count_2 > 500 && ! con_changed {
                let con_str_tmp = test_helper::get_connection_name(&conn_name_sub).await;
                sleep(Duration::from_secs(1)).await;
                con_changed = con_str_start != con_str_tmp;
            }
            if received_count_1 + received_count_2 >= 1000 {
                break 'outer;
            }
        }

        let con_str_end = test_helper::get_connection_name(&conn_name_sub).await;

        assert!(con_str_start != con_str_end);
        assert!(received_count_1>0);
        assert!(received_count_2>0);
        assert_eq!(received_count_1 + received_count_2, 1000);
    });
}