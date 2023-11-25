use tokio::sync::mpsc::Receiver;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::rabbitclient::RabbitClient;
use crate::subscriber::{Subscriber, SubscribeParams, SubscriptionContent};
use crate::publisher::{Publisher, PublisherParams, PublishingParams};
use crate::worker::Worker;
pub struct BasicRpcClient<'a> {
    rabbitclient: &'a RabbitClient,
    exchange: String,
    routing_key: String,
}

impl<'a> BasicRpcClient<'a> {
    pub async fn new(rabbitclient: &'a RabbitClient, exchange: &str, routing_key: &str) -> Result<BasicRpcClient<'a>, String> {
        let r = BasicRpcClient {
            rabbitclient,
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
        };
        Ok(r)
    }

    async fn init_publisher(&self, worker: &Arc<Mutex<Worker>>) -> Result<Publisher, String> {

        let pub_params = PublisherParams::builder()
            .exchange(&self.exchange)
            .routing_key(&self.routing_key)
            .reply_to("amq.rabbitmq.reply-to")
            .build();

        match self.rabbitclient.new_publisher_from_worker(pub_params, worker).await {
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

    pub async fn call(&self, content: Vec<u8>,params: Option<PublishingParams>) -> Result<SubscriptionContent, String> {
        let mut s: Subscriber = self.init_subscriber().await?;
        let p: Publisher = self.init_publisher(&s.worker).await?;
        let rx: &mut Receiver<SubscriptionContent>;
        match s.subscribe_with_auto_ack().await {
            Ok(r) => rx = r,
            Err(e) => return Err(e.to_string()),
        }
        let r = match params {
            Some(pp) => {
                p.publish_with_params(content, &pp).await
            },
            None => p.publish(content).await,
        };
        if let Err(e) = r {
            return Err(e.to_string());
        }

        match rx.recv().await {
            Some(c) => {
                return Ok(c);
            },
            None => {
                return Err("received no content".to_string());
            }
        }
    }
}

