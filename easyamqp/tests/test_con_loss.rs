use serde_json::{Result, Value};
use serde_json_path::JsonPath;
use easyamqp::utils::{get_env_var_str, get_env_var_int};
use std::{time, thread};


fn extract_conn_name_from_json(conn_json_str: &str, connection_name: &str) -> Option<String> {
    //println!("{}", conn_json_str);
    let v: Value = serde_json::from_str(&conn_json_str).expect("Error while parse Json");
    let json_path_str = format!("$[?(@.client_properties.connection_name == '{}')].name", connection_name);
    let json_path = JsonPath::parse(json_path_str.as_str()).expect("error while construct json_path");
    let node_list = json_path.query(&v);
    match node_list.get(0) {
        Some(e) => {
            let s = e.to_string();
            let r = &s[1..s.len()-1].to_string();
            Some(r.to_string())
        },
        None => {
            println!("no connection found in rabbitmq response");
            None
        }
    }
}

fn get_conn_name(rabbit_server: &str, user_name: &str, password: &str, connection_name: &str) -> Option<String> {
    match std::process::Command::new("rabbitmqadmin")
        .arg("--host")
        .arg(rabbit_server)
        .arg("--username")
        .arg(user_name)
        .arg("--password")
        .arg(password)
        .arg("list")
        .arg("connections")
        .arg("-f")
        .arg("pretty_json")
        .output() {
        Ok(o) => {
            let conn_json = o.stdout;
            let conn_json_str = String::from_utf8_lossy(&conn_json);
            if conn_json_str.len() == 0 {
                return None;
            }
            return extract_conn_name_from_json(&conn_json_str, connection_name);
        },
        Err(e) => {
            println!("failed to execute rabbitmqadmin to list connections");
            return None;
        }
    };
}

fn close_conn(rabbit_server: &str, user_name: &str, password: &str, connection_name: &str) -> Option<String> {
    println!("conn_name: '{}'", connection_name);
    match std::process::Command::new("rabbitmqadmin")
        .arg("--host")
        .arg(rabbit_server)
        .arg("--username")
        .arg(user_name)
        .arg("--password")
        .arg(password)
        .arg("close")
        .arg("connection")
        .arg(format!("name={}", connection_name))
        .output() {
        Ok(o) => {
            let e_vec = o.stderr;
            let e_str = String::from_utf8_lossy(&e_vec);
            println!("{}", e_str);
            let o_vec = o.stdout;
            let o_str = String::from_utf8_lossy(&o_vec);
            return Some(o_str.to_string());
        },
        Err(_) => {
            println!("failed to close connection");
            return None;
        }
    };
}


#[test]
#[ignore]
fn connection_loss_test() {
    println!("I am only used in docker compose based integration tests");
    let user_name = get_env_var_str("RABBIT_USER", "guest");
    let password = get_env_var_str("RABBIT_PASSWORD", "guest");
    let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");
    let connection_name = get_env_var_str("CONN_NAME", "con_test");
    let count_of_kills = get_env_var_int("KILL_COUNT", 10);
    let expected_conn_count = get_env_var_int("CONN_COUNT", 1);

    println!("rabbit_server={}, connection_name={}, count_of_kills={}, expected_conn_count={}",
        rabbit_server, connection_name, count_of_kills, expected_conn_count);

    let max_reconnect_attempts = 10;
    let mut last_conn_name = "".to_string();
    for i in 0..count_of_kills {
        let mut reconnect_seconds = 1;
        let mut reconnect_attempts = 0;
            loop {
            match get_conn_name(rabbit_server.as_str(), user_name.as_str(), password.as_str(), connection_name.as_str()) {
                Some(conn_name) => {
                    assert_ne!(last_conn_name, conn_name);
                    if i == count_of_kills -1 {
                        println!("reached maximum number of connection kills, and found new connection :)")
                    } else {
                        let s = close_conn(rabbit_server.as_str(),
                        user_name.as_str(), 
                        password.as_str(), 
                        conn_name.as_str()).expect("couldn't close connection");
                        assert_eq!("connection closed\n".to_string(),s);
                        last_conn_name = conn_name;
                    }
                    break;
                },
                None => {
                    println!("sorry, can't find connection to close ... sleep for {}s, attempt {}/{}", reconnect_seconds, reconnect_attempts, max_reconnect_attempts);
                    let sleep_time = time::Duration::from_secs(reconnect_seconds);
                    thread::sleep(sleep_time);
                    reconnect_seconds = reconnect_seconds * 2;
                    reconnect_attempts += 1;
                    if reconnect_attempts > max_reconnect_attempts {
                        assert!(false);
                    }
                }
            }
        }
        println!("connection closed {}/{}, waiting for next try ...", i, count_of_kills);
        let sleep_time = time::Duration::from_secs(1);
        thread::sleep(sleep_time);

    }
}