use easyamqp::rabbitclient;

fn main() {
    let params = rabbitclient::RabbitConParams {
        server: "127.0.0.1".to_string(),
        port: 5672,
        user: "guest".to_string(),
        password: "guest".to_string(),
    };
    let client = rabbitclient::RabbitClient::new(params);
    println!("Hello, world ...");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            client.dummy().await;
        });
}
