use serde::de::DeserializeOwned;
use tokio::sync::mpsc::Receiver;
use bson;
use std::io::Cursor;

use crate::rabbitclient::RabbitClient;
use crate::subscriber::{Subscriber, SubscribeParams, SubscriptionContent};
use crate::publisher::{Publisher, PublisherParams};

pub struct BsonRpcClient<'a> {
    rabbitclient: &'a RabbitClient,
    exchange: String,
    routing_key: String,
}

impl<'a> BsonRpcClient<'a> {
    pub async fn new(rabbitclient: &'a RabbitClient, exchange: &str, routing_key: &str) -> Result<BsonRpcClient<'a>, String> {
        let r = BsonRpcClient {
            rabbitclient,
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
        };
        Ok(r)
    }

    async fn init_publisher(&self) -> Result<Publisher, String> {
        let pub_params = PublisherParams::builder()
            .exchange(&self.exchange)
            .routing_key(&self.routing_key)
            .content_type("BSON")
            .build();
        match self.rabbitclient.new_publisher_from_params(pub_params).await {
            Ok(r) => return Ok(r),
            Err(e) => return Err(e),
        }
    }

    async fn init_subscriber(&self) -> Result<Subscriber, String> {
        let sub_params = SubscribeParams::builder("amq.rabbitmq.reply-to", "BsonRpcClient")
            .auto_ack(true)
            .exclusive(true)
            .build();
        match self.rabbitclient.new_subscriber(sub_params).await {
            Ok(r) => return Ok(r),
            Err(e) => return Err(e),
        }
    }

    pub async fn call<S: serde::ser::Serialize, T: DeserializeOwned> (&self, content: &S) -> Result<T, String> {
        let p: Publisher = self.init_publisher().await?;
        let mut s: Subscriber = self.init_subscriber().await?;
        let rx: &mut Receiver<SubscriptionContent>;
        match s.subscribe_with_auto_ack().await {
            Ok(r) => rx = r,
            Err(e) => return Err(e.to_string()),
        }
        let v = bson::to_vec::<S>(content).unwrap();
        if let Err(e) = p.publish(v).await {
            return Err(e.to_string());
        }

        match rx.recv().await {
            Some(c) => {
                let mut reader = Cursor::new(c.data);
                match bson::from_reader::<_, T>(&mut reader) {
                    Ok(r) => return Ok(r),
                    Err(e) => return Err(e.to_string()),
                }
            },
            None => {
                return Err("received no content".to_string());
            }
        }
    }
}

