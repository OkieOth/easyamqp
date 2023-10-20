use easyamqp::rabbitclient;


#[test]
#[ignore]
fn create_exchange_test() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        let params = rabbitclient::RabbitConParams {
            con_name: None,
            server: "127.0.0.1".to_string(),
            port: 5672,
            user: "guest".to_string(),
            password: "guest".to_string(),
        };
    
    
        let mut client = rabbitclient::RabbitClient::new(params).await;
        client.connect().await.unwrap();
        client.create_exchange("bulli", rabbitclient::ExchangeType::Topic, false, false).await.unwrap();
        client.close().await;
        println!("done")
    });
}