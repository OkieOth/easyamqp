use easyamqp::rabbitclient;

fn main() {
    let params = rabbitclient::RabbitConParams {
        server: "127.0.0.1".to_string(),
        port: 5672,
        user: "guest".to_string(),
        password: "guest".to_string(),
    };
    println!("Hello, world ...");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let client = rabbitclient::RabbitClient::new(params).await;
            if let Ok(s) = client.dummy().await {
                println!("Received for dummy: {}", s);
            } else {
                println!("Got error for dummy call");
            }
        });
}
