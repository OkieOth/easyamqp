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


fn extract_json_names(json_str: &str) -> Vec<String> {
    //println!("{}", conn_json_str);
    let v: Value = serde_json::from_str(&json_str).expect("Error while parse Json");
    let json_path =
        JsonPath::parse("$..name").expect("error while construct json_path");
    let node_list = json_path.query(&v);
    let mut v: Vec<String> = Vec::new();
    for node in node_list {
        let s = node.to_string();
        // this is needed because the JSON result is still enveloped in double quotes
        let r: String = s[1..s.len() - 1].to_string();
        v.push(r);
    }
    v
}

fn get_exchanges(
    rabbit_server: &str,
    user_name: &str,
    password: &str,
) -> Option<Vec<String>> {
    match std::process::Command::new("rabbitmqadmin")
        .arg("--host")
        .arg(rabbit_server)
        .arg("--username")
        .arg(user_name)
        .arg("--password")
        .arg(password)
        .arg("list")
        .arg("exchanges")
        .arg("-f")
        .arg("pretty_json")
        .output()
    {
        Ok(o) => {
            let json = o.stdout;
            let json_str = String::from_utf8_lossy(&json);
            if json_str.len() == 0 {
                return None;
            }
            return Some(extract_json_names(&json_str));
        }
        Err(_e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            return None;
        }
    };
}

fn get_queues(
    rabbit_server: &str,
    user_name: &str,
    password: &str,
) -> Option<Vec<String>> {
    match std::process::Command::new("rabbitmqadmin")
        .arg("--host")
        .arg(rabbit_server)
        .arg("--username")
        .arg(user_name)
        .arg("--password")
        .arg(password)
        .arg("list")
        .arg("queues")
        .arg("-f")
        .arg("pretty_json")
        .output()
    {
        Ok(o) => {
            let json = o.stdout;
            let json_str = String::from_utf8_lossy(&json);
            if json_str.len() == 0 {
                return None;
            }
            return Some(extract_json_names(&json_str));
        }
        Err(_e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            return None;
        }
    };
}

fn check_binding(
    rabbit_server: &str,
    user_name: &str,
    password: &str,
    binding_json_path: &str
) -> bool {
    match std::process::Command::new("rabbitmqadmin")
        .arg("--host")
        .arg(rabbit_server)
        .arg("--username")
        .arg(user_name)
        .arg("--password")
        .arg(password)
        .arg("list")
        .arg("bindings")
        .arg("-f")
        .arg("pretty_json")
        .output()
    {
        Ok(o) => {
            let json = o.stdout;
            let json_str = String::from_utf8_lossy(&json);
            if json_str.len() == 0 {
                return false;
            }
            let v: Value = serde_json::from_str(&json_str).expect("Error while parse Json");
            let json_path =
                JsonPath::parse(binding_json_path).expect("error while construct json_path");
            let node_list = json_path.query(&v);
            return node_list.len() > 0;
        }
        Err(_e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            return false;
        }
    };
}


#[test]
#[ignore]
fn create_exchange_test() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        let (mut client, params) = RabbitClient::get_default_client().await;
        client.connect().await.unwrap();
        let param1 = ExchangeDefinition::builder("first")
            .durable(true)
            .build();
        client.declare_exchange(param1).await.unwrap();

        let param2 = ExchangeDefinition::builder("second")
            .build();
        client.declare_exchange(param2).await.unwrap();

        let param3 = ExchangeDefinition::builder("third")
            .build();
        client.declare_exchange(param3).await.unwrap();

        let param4 = ExchangeDefinition::builder("second")
            .build();
        client.declare_exchange(param4).await.unwrap();

        client.close().await;

        match get_exchanges(&params.server, &params.user, &params.password) {
            Some(v) => {
                let mut found = 0;
                for s in v {
                    if s == "first" {
                        found += 1;
                    }
                    if s == "second" {
                        found += 1;
                    }
                    if s == "third" {
                        found += 1;
                    }
               }
               assert_eq!(found, 3);
            },
            None => assert!(false),

        }

    });
}

#[test]
#[ignore]
fn create_queues_test() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        let (mut client, params) = RabbitClient::get_default_client().await;
        client.connect().await.unwrap();

        let param1 = QueueDefinition::builder("first_queue")
            .durable(true)
            .build();
        client.declare_queue(param1).await.unwrap();

        let param2 = QueueDefinition::builder("second_queue")
            .exclusive(true)
            .durable(true)
            .build();
        client.declare_queue(param2).await.unwrap();

        let param3 = QueueDefinition::builder("third_queue")
            .exclusive(true)
            .durable(true)
            .build();
        client.declare_queue(param3).await.unwrap();

        let param4 = QueueDefinition::builder("second_queue")
            .exclusive(true)
            .durable(true)
            .build();
        client.declare_queue(param4).await.unwrap();

        client.close().await;

        match get_queues(&params.server, &params.user, &params.password) {
            Some(v) => {
                let mut found = 0;
                for s in v {
                    if s == "first_queue" {
                        found += 1;
                    }
                    if s == "second_queue" {
                        found += 1;
                    }
                    if s == "third_queue" {
                        found += 1;
                    }
               }
               assert_eq!(found, 3);
            },
            None => assert!(false),
        }
    });
}

#[test]
#[ignore]
fn create_bindings_test() {
    create_exchange_test();
    create_queues_test();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        let (mut client, params) = RabbitClient::get_default_client().await;
        client.connect().await.unwrap();
        let param1 = QueueBindingDefinition::new(
            "first_queue", "first", "*");
        client.declare_queue_binding(param1).await.unwrap();

        let param2 = QueueBindingDefinition::new(
            "second_queue", "second", "second.#");
        client.declare_queue_binding(param2).await.unwrap();

        let param3 = QueueBindingDefinition::new(
            "third_queue", "third", "third.*");
        client.declare_queue_binding(param3).await.unwrap();

        let json_path1 = "$[?(@.source == 'first' && @.destination == 'first_queue')]";
        assert!(check_binding(&params.server, &params.user, &params.password, json_path1));
        let json_path2 = "$[?(@.source == 'second' && @.destination == 'second_queue')]";
        assert!(check_binding(&params.server, &params.user, &params.password, json_path2));
        let json_path3 = "$[?(@.source == 'third' && @.destination == 'third_queue')]";
        assert!(check_binding(&params.server, &params.user, &params.password, json_path3));

        let json_path1_2 = "$[?(@.source == 'first' && @.destination == 'first_queue' && @.routing_key == '*')]";
        assert!(check_binding(&params.server, &params.user, &params.password, json_path1_2));
        let json_path2_2 = "$[?(@.source == 'second' && @.destination == 'second_queue' && @.routing_key == 'second.#')]";
        assert!(check_binding(&params.server, &params.user, &params.password, json_path2_2));
        let json_path3_2 = "$[?(@.source == 'third' && @.destination == 'third_queue' && @.routing_key == 'third.*')]";
        assert!(check_binding(&params.server, &params.user, &params.password, json_path3_2));
    });
}


/// This function test the deregistration of publisher workers in case
/// that publisher run out of scope or are droped
#[test]
#[ignore]
fn test_deregister_of_deleted_publishers() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        let (mut client, _) = RabbitClient::get_default_client().await;
        client.connect().await.unwrap();

        let _p1: Publisher = client.new_publisher().await.unwrap();
        assert_eq!(1, client.get_worker_count().await);
        let _p2: Publisher = client.new_publisher().await.unwrap();
        assert_eq!(2, client.get_worker_count().await);
        let _p3: Publisher = client.new_publisher().await.unwrap();
        assert_eq!(3, client.get_worker_count().await);
        let sleep_time = Duration::from_millis(500);
        drop(_p2);
        sleep( sleep_time ).await;
        assert_eq!(2, client.get_worker_count().await);
        drop(_p1);
        sleep( sleep_time ).await;
        assert_eq!(1, client.get_worker_count().await);
        drop(_p3);
        sleep( sleep_time ).await;
        assert_eq!(0, client.get_worker_count().await);
    });
}

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
        let exchange_def = ExchangeDefinition::builder("test_simple_pub_sub").build();
        let _ = client.declare_exchange(exchange_def).await;
        
        let queue_def = QueueDefinition::builder("test_simple_pub_sub.queue")
            .durable(true)
            .exclusive(true)
            .auto_delete(true)
            .build();
        let _ = client.declare_queue(queue_def).await;
        let binding_def = QueueBindingDefinition::new("test_simple_pub_sub.queue", "test_simple_pub_sub", "test"); 
        let _ = client.declare_queue_binding(binding_def).await;

        let pub_params = PublisherParams::builder()
            .exchange("test_simple_pub_sub")
            .routing_key("test")
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

        let sub_params = SubscribeParams::builder("test_simple_pub_sub.queue", "test_simple_pub_sub")
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

#[test]
#[ignore]
fn test_multiple_subscribers() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        // let user_name = get_env_var_str("RABBIT_USER", "guest");
        // let password = get_env_var_str("RABBIT_PASSWORD", "guest");
        // let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");

        // let params = RabbitConParams::builder()
        //     .server(&rabbit_server)
        //     .user(&user_name)
        //     .password(&password)
        //     .build();

        // let mut client = RabbitClient::new(params).await;
        let (mut client, _) = RabbitClient::get_default_client().await;
        client.connect().await.unwrap();
    //     let exchange_def = ExchangeDefinition::builder("test_simple_pub_sub").build();
    //     let _ = client.declare_exchange(exchange_def).await;
        
    //     let queue_def = QueueDefinition::builder("test_simple_pub_sub.queue")
    //         .durable(true)
    //         .exclusive(true)
    //         .auto_delete(true)
    //         .build();
    //     let _ = client.declare_queue(queue_def).await;
    //     let binding_def = QueueBindingDefinition::new("test_simple_pub_sub.queue", "test_simple_pub_sub", "test"); 
    //     let _ = client.declare_queue_binding(binding_def).await;

    //     let mut pub_params = PublisherParams::builder()
    //         .exchange("test_simple_pub_sub")
    //         .routing_key("test")
    //         .build();

    //     let p1: Publisher = client.new_publisher_from_params(pub_params).await.unwrap();
    //     assert_eq!(1, client.get_worker_count().await);

    //     task::spawn(async move {
    //         let content = String::from(
    //             r#"
    //                 {
    //                     "publisher": "example"
    //                     "data": "Hello, amqprs!"
    //                 }
    //             "#,
    //         )
    //         .into_bytes();
    //         for i in 0 .. 10 {
    //             if let Err(e) = p1.publish(content.clone()).await {
    //                 error!("error while publishing {i}: {}", e.to_string());
    //             }
    //         }
    //     });

    //     let sub_params = SubscribeParams::builder("test_simple_pub_sub.queue", "test_simple_pub_sub")
    //         .auto_ack(true)
    //         .exclusive(true)
    //         .build();

    //     let mut subscriber: Subscriber;
    //     if let Ok(s) = client.new_subscriber(sub_params).await {
    //         subscriber = s;
    //     } else {
    //         assert!(false);
    //         return;
    //     }
    //     let rx_content: &mut Receiver<SubscriptionContent>;
    //     if let Ok(rxc ) = subscriber.subscribe_with_auto_ack().await {
    //         rx_content = rxc;
    //     } else {
    //         assert!(false);
    //         return;
    //     }
    //     let mut received_count = 0;
    //     let mut failure_count = 0;
    //     loop {
    //         const TIMEOUT_SECS: u64 = 3;
    //         match timeout(Duration::from_secs(TIMEOUT_SECS), rx_content.recv()).await {
    //             Ok(timeout_result) => {
    //                 match timeout_result {
    //                     Some(_) => {
    //                         received_count += 1;
    //                     },
    //                     None => {
    //                         error!("didn't receive proper subscription response");
    //                         failure_count += 1;
    //                     },
    //                 }
    //             },
    //             Err(_) => {
    //                 // timeout
    //                 error!("didn't receive subscription response in timeout ({} s)", TIMEOUT_SECS);
    //                 failure_count += 1;
    //             },
    //         }

    //         if (received_count == 10) || (failure_count == 10) {
    //             break;
    //         }
    //     }
    //     assert_eq!(10, received_count);
    });
}