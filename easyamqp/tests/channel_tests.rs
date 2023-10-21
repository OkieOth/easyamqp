use easyamqp::rabbitclient;
use easyamqp::utils::get_env_var_str;
use serde_json::Value;
use serde_json_path::JsonPath;

fn extract_exchange_names(json_str: &str) -> Vec<String> {
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
            return Some(extract_exchange_names(&json_str));
        }
        Err(_e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            return None;
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
        
        let params = rabbitclient::RabbitConParams {
            con_name: None,
            server: rabbit_server.clone(),
            port: 5672,
            user: user_name.clone(),
            password: password.clone(),
        };

        let mut client = rabbitclient::RabbitClient::new(params).await;
        client.connect().await.unwrap();
        client.create_exchange("first", rabbitclient::ExchangeType::Topic, true, false).await.unwrap();
        client.create_exchange("second", rabbitclient::ExchangeType::Topic, false, false).await.unwrap();
        client.create_exchange("third", rabbitclient::ExchangeType::Topic, false, false).await.unwrap();
        client.create_exchange("second", rabbitclient::ExchangeType::Topic, false, false).await.unwrap();
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