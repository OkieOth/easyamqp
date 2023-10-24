use easyamqp::{RabbitClient, RabbitConParams, 
    ExchangeDefinition, ExchangeType,
    QueueDefinition, QueueBindingDefinition};
use easyamqp::utils::get_env_var_str;
use serde_json::Value;
use serde_json_path::JsonPath;

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
        let user_name = get_env_var_str("RABBIT_USER", "guest");
        let password = get_env_var_str("RABBIT_PASSWORD", "guest");
        let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");
        
        let params = RabbitConParams::builder()
            .server(&rabbit_server)
            .user(&user_name)
            .password(&password)
            .build();

        let mut client = RabbitClient::new(params).await;
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

        match get_exchanges(&rabbit_server, &user_name, &password) {
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
        let user_name = get_env_var_str("RABBIT_USER", "guest");
        let password = get_env_var_str("RABBIT_PASSWORD", "guest");
        let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");

        let params = RabbitConParams::builder()
            .server(&rabbit_server)
            .user(&user_name)
            .password(&password)
            .build();

        let mut client = RabbitClient::new(params).await;
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

        match get_queues(&rabbit_server, &user_name, &password) {
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
        let user_name = get_env_var_str("RABBIT_USER", "guest");
        let password = get_env_var_str("RABBIT_PASSWORD", "guest");
        let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");

        let params = RabbitConParams::builder()
            .server(&rabbit_server)
            .user(&user_name)
            .password(&password)
            .build();

        let mut client = RabbitClient::new(params).await;
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
        assert!(check_binding(&rabbit_server, &user_name, &password, json_path1));
        let json_path2 = "$[?(@.source == 'second' && @.destination == 'second_queue')]";
        assert!(check_binding(&rabbit_server, &user_name, &password, json_path2));
        let json_path3 = "$[?(@.source == 'third' && @.destination == 'third_queue')]";
        assert!(check_binding(&rabbit_server, &user_name, &password, json_path3));

        let json_path1_2 = "$[?(@.source == 'first' && @.destination == 'first_queue' && @.routing_key == '*')]";
        assert!(check_binding(&rabbit_server, &user_name, &password, json_path1_2));
        let json_path2_2 = "$[?(@.source == 'second' && @.destination == 'second_queue' && @.routing_key == 'second.#')]";
        assert!(check_binding(&rabbit_server, &user_name, &password, json_path2_2));
        let json_path3_2 = "$[?(@.source == 'third' && @.destination == 'third_queue' && @.routing_key == 'third.*')]";
        assert!(check_binding(&rabbit_server, &user_name, &password, json_path3_2));
    });
}