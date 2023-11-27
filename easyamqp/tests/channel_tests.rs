use easyamqp::{RabbitClient, ExchangeDefinition,
    QueueDefinition, QueueBindingDefinition, Publisher};
use serde_json::Value;
use serde_json_path::JsonPath;
use tokio::time::{sleep, Duration};

fn extract_json_names(json_str: &str) -> Vec<String> {
    //println!("{}", conn_json_str);
    let v: Value = serde_json::from_str(json_str).expect("Error while parse Json");
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
            Some(extract_json_names(&json_str))
        }
        Err(_e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            None
        }
    }
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
            Some(extract_json_names(&json_str))
        }
        Err(_e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            None
        }
    }
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
            !node_list.is_empty()
        }
        Err(_e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            false
        }
    }
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
