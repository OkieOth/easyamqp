use base64::Engine;
use reqwest::header;
use easyamqp::utils::get_env_var_str;
use serde_json_path::JsonPath;
use tokio::time::{sleep, Duration};


use urlencoding::encode;

pub async fn get_connection_name(conn_name: &str) -> Result<String, String> {
    // because sometime the admin api is to slow
    let mut try_count = 0;
    let path_string = format!("$[?(@.client_properties.connection_name == '{conn_name}')].name");
    loop {
        match list_from_rabbitmqadmin("connections").await {
            Ok(s) => {
                let j = serde_json::from_str(&s).unwrap();
                //let path_string = format!("$[?(@.client_properties.connection_name == '{conn_name}')].name')]");
                match JsonPath::parse(&path_string) {
                    Ok(path) => {
                        match path.query(&j).first() {
                            Some(v) => {
                                let s = v.as_str().unwrap();
                                return Ok(s.to_string());
                            },
                            None => try_count += 1,
                        }
                    },
                    Err(e) => {
                        return Err(e.to_string());
                    },
                }
            },
            Err(msg) => {
                return Err(msg);
            },
        }
        if try_count > 20 {
            let s = format!("reached maximun tries to retrieve con name, conn_name={}", conn_name);
            return Err(s);
        }
        sleep(Duration::from_millis(500)).await;
    }
}


pub async fn test_connection_count(conn_name: &str, expected: usize) {
    match list_from_rabbitmqadmin("connections").await {
        Ok(s) => {
            let con_count = get_connection_count(&s, &conn_name).await.unwrap();
            assert_eq!(expected, con_count, "wrong connection count for: {}: {}", conn_name, s);
        },
        Err(msg) => {
            assert!(false, "{}", msg);
        },
    }
}

pub async fn close_connection(user_con_name: &str) -> Result<(), String> {
    let user_name = get_env_var_str("RABBIT_USER", "guest");
    let password = get_env_var_str("RABBIT_PASSWORD", "guest");
    let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");


    let con_name = get_connection_name(&user_con_name).await.unwrap();


    // Build the URL
    let encoded_str = encode(con_name.as_str());
    let url = format!("http://{rabbit_server}:15672/api/connections/{encoded_str}");

    // Create reqwest client with Basic Auth credentials
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // Use this only for testing, remove in production
        .build()
        .map_err(|e| e.to_string())?;

    // Create a request with Basic Auth
    let request = client.delete(&url)
        .header(header::AUTHORIZATION, reqwest::header::HeaderValue::from_str(&format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(&format!("{}:{}", user_name, password))
        )).map_err(|e| e.to_string())?);

    // Send the request asynchronously
    if let Err(e) = request.send().await {
        print!("error while del con: {}", e.to_string());
    }

    Ok(())

}

pub async fn del_queue(queue_name: &str) -> Result<(), String> {
    let user_name = get_env_var_str("RABBIT_USER", "guest");
    let password = get_env_var_str("RABBIT_PASSWORD", "guest");
    let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");

    // Build the URL
    let encoded_str = encode(queue_name);
    let encoded_vhost = encode("/");
    let url = format!("http://{rabbit_server}:15672/api/queues/{encoded_vhost}/{encoded_str}");

    // Create reqwest client with Basic Auth credentials
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // Use this only for testing, remove in production
        .build()
        .map_err(|e| e.to_string())?;

    // Create a request with Basic Auth
    let request = client.delete(&url)
        .header(header::AUTHORIZATION, reqwest::header::HeaderValue::from_str(&format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(&format!("{}:{}", user_name, password))
        )).map_err(|e| e.to_string())?);

    // Send the request asynchronously
    if let Err(e) = request.send().await {
        Err(e.to_string())
    } else {
        Ok(())
    }
}


pub async fn get_connection_count(connection_resp_json: &String, conn_name: &str) -> Result<usize, String> {
    let j = serde_json::from_str(connection_resp_json).unwrap();
    let path_string = format!("$[?(@.client_properties.connection_name == '{conn_name}')]");
    match JsonPath::parse(&path_string) {
        Ok(path) => {
            let r = path.query(&j);
            Ok(r.len())
        },
        Err(e) => Err(e.to_string()),
    }
}

pub async fn list_from_rabbitmqadmin(obj_type: &str) -> Result<String, String> {
    // Replace 'your_username' and 'your_password' with your RabbitMQ username and password
    let user_name = get_env_var_str("RABBIT_USER", "guest");
    let password = get_env_var_str("RABBIT_PASSWORD", "guest");
    let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");


    // Build the URL
    let url = format!("http://{rabbit_server}:15672/api/{obj_type}");

    // Create reqwest client with Basic Auth credentials
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // Use this only for testing, remove in production
        .build()
        .map_err(|e| e.to_string())?;

    // Create a request with Basic Auth
    let request = client.get(&url)
        .header(header::AUTHORIZATION, reqwest::header::HeaderValue::from_str(&format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(&format!("{}:{}", user_name, password))
        )).map_err(|e| e.to_string())?);

    // Send the request asynchronously
    let response = request.send().await.map_err(|e| e.to_string())?;

    // Check if the request was successful (status code 2xx)
    if response.status().is_success() {
        // Return the response body
        match response.text().await {
            Ok(body) => Ok(body),
            Err(e) => Err(e.to_string()),
        }
    } else {
        // Return an error message if the request was not successful
        let error_msg = format!("Error: {}", response.status());
        Err(error_msg)
    }
}
