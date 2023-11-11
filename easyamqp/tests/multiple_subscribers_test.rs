use easyamqp::{RabbitClient, RabbitConParams, 
    ExchangeDefinition, ExchangeType,
    QueueDefinition, QueueBindingDefinition,
    Publisher, PublisherParams, 
    Subscriber, SubscribeParams, SubscriptionContent};
use easyamqp::utils::get_env_var_str;
use serde_json::Value;
use serde_json_path::JsonPath;
use tokio::time::{timeout, sleep, Duration};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task;
use log::error;


// #[test]
// #[ignore]
// fn test_multiple_subscribers() {
//     tokio::runtime::Builder::new_multi_thread()
//         .enable_all()
//         .build()
//         .unwrap()
//         .block_on(async {
//         let (mut client, _) = RabbitClient::get_default_client().await;
//         client.connect().await.unwrap();

//         let exchange_name = "test_multiple_subscribers";
//         let queue_name = "test_multiple_subscribers.queue";
//         let routing_key = "test";

//         let exchange_def = ExchangeDefinition::builder(exchange_name).build();
//         let _ = client.declare_exchange(exchange_def).await;

//         let queue_def = QueueDefinition::builder(queue_name)
//             .durable(false)
//             .exclusive(false)
//             .auto_delete(true)
//             .build();
//         let _ = client.declare_queue(queue_def).await;
//         let binding_def = QueueBindingDefinition::new(queue_name, exchange_name, routing_key); 
//         let _ = client.declare_queue_binding(binding_def).await;

//         let pub_params = PublisherParams::builder()
//             .exchange(exchange_name)
//             .routing_key(routing_key)
//             .build();

//         let p1: Publisher = client.new_publisher_from_params(pub_params).await.unwrap();
//         assert_eq!(1, client.get_worker_count().await);

//         task::spawn(async move {
//             let content = String::from(
//                 r#"
//                     {
//                         "publisher": "example"
//                         "data": "Hello, amqprs!"
//                     }
//                 "#,
//             )
//             .into_bytes();
//             for i in 0 .. 100 {
//                 if let Err(e) = p1.publish(content.clone()).await {
//                     error!("error while publishing {i}: {}", e.to_string());
//                 }
//             }
//         });


//         let sub_params_1 = SubscribeParams::builder(queue_name, "subscriber_1")
//             .auto_ack(true)
//             .exclusive(false)
//             .build();
//         let mut subscriber_1: Subscriber;
//         if let Ok(s) = client.new_subscriber(sub_params_1).await {
//             subscriber_1 = s;
//         } else {
//             assert!(false);
//             return;
//         }
//         let sub_params_2 = SubscribeParams::builder(queue_name, "subscriber_2")
//             .auto_ack(true)
//             .exclusive(false)
//             .build();
//         let mut subscriber_2: Subscriber;
//         if let Ok(s) = client.new_subscriber(sub_params_2).await {
//             subscriber_2 = s;
//         } else {
//             assert!(false);
//             return;
//         }


//         let mut received_count_1 = 0;
//         let mut failure_count_1 = 0;


//         task::spawn(async move {
//             let rx_content_1: &mut Receiver<SubscriptionContent>;
//             match subscriber_1.subscribe_with_auto_ack().await {
//                 Ok(r) => rx_content_1 = r,
//                 Err(e) => {
//                     print!("{}", e.to_string());
//                     assert!(false);
//                     return;
//                 }
//             }
//             loop {
//                 const TIMEOUT_SECS: u64 = 3;
//                 match timeout(Duration::from_secs(TIMEOUT_SECS), rx_content_1.recv()).await {
//                     Ok(timeout_result) => {
//                         match timeout_result {
//                             Some(_) => {
//                                 received_count_1 += 1;
//                             },
//                             None => {
//                                 error!("didn't receive proper subscription response");
//                                 failure_count_1 += 1;
//                             },
//                         }
//                     },
//                     Err(_) => {
//                         // timeout
//                         error!("didn't receive subscription response in timeout ({} s)", TIMEOUT_SECS);
//                         failure_count_1 += 1;
//                     },
//                 }
//             }
//         });

//         let mut received_count_2 = 0;
//         let mut failure_count_2 = 0;

//         task::spawn(async move {
//             let rx_content_2: &mut Receiver<SubscriptionContent>;
//             if let Ok(rxc ) = subscriber_2.subscribe_with_auto_ack().await {
//                 rx_content_2 = rxc;
//             } else {
//                 assert!(false);
//                 return;
//             }
//             loop {
//                 const TIMEOUT_SECS: u64 = 3;
//                 match timeout(Duration::from_secs(TIMEOUT_SECS), rx_content_2.recv()).await {
//                     Ok(timeout_result) => {
//                         match timeout_result {
//                             Some(_) => {
//                                 received_count_2 += 1;
//                             },
//                             None => {
//                                 error!("didn't receive proper subscription response");
//                                 failure_count_2 += 1;
//                             },
//                         }
//                     },
//                     Err(_) => {
//                         // timeout
//                         error!("didn't receive subscription response in timeout ({} s)", TIMEOUT_SECS);
//                         failure_count_2 += 1;
//                     },
//                 }
//             }
//         });

 

//         // loop {
//         //     const TIMEOUT_SECS: u64 = 3;
//         //     tokio::select! {
//         //         res_1 = rx_content_1.recv() => {
//         //             if res_1.is_some() {
//         //                 received_count_1 += 1;
//         //             } else {
//         //                 failure_count_1 += 1;
//         //             }
//         //         },
//         //         res_2 = rx_content_2.recv() => {
//         //             if res_2.is_some() {
//         //                 received_count_2 += 1;
//         //             } else {
//         //                 failure_count_2 += 1;
//         //             }
//         //         },
//         //         _ = sleep(Duration::from_secs(100)) => {
//         //             // Timeout occurred
//         //             println!("Timeout reached");
//         //             break;
//         //         }
//         //     }
//         // }
//         sleep(Duration::from_secs(100)).await;
//         assert!(received_count_1>0);
//         assert!(received_count_2>0);
//         assert_eq!(received_count_1 + received_count_2, 100);
//     });
// }