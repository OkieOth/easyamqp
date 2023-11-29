use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::rabbitclient::RabbitClient;
use crate::subscriber::{Subscriber, SubscribeParams, SubscriptionContent, SubscriptionResponse};
use crate::publisher::{Publisher, PublisherParams, PublishingParams};
use crate::worker::Worker;
use std::collections::HashMap;
/// Simple AMQP base RPC server that works over topic exchanges
pub struct BasicRpcServer<'a> {
    rabbitclient: &'a RabbitClient,
    publisher: Option<Publisher>,
    subscriber: Option<Subscriber>,
    params: RpcServerParams,
}

#[derive(Debug, Clone, Default)]
pub struct RpcServerParams {
    pub sub_exchange: String,
    pub sub_routing_key: String,
    pub sub_queue_name: Option<String>,
    pub pub_content_type: Option<String>,
    pub pub_content_encoding: Option<String>,
    pub app_id: Option<String>,
    pub pub_headers: Option<HashMap<String, String>>,
    pub timeout_secs: Option<u64>,
}

impl RpcServerParams {
    pub fn builder(sub_exchange: &str, sub_routing_key: &str) -> RpcServerParamsBuilder {
        let mut ret = RpcServerParamsBuilder::default();
        ret.sub_exchange = sub_exchange.to_string();
        ret.sub_routing_key = sub_routing_key.to_string();
        ret
    }
}

#[derive(Debug, Clone, Default)]
pub struct RpcServerParamsBuilder {
    pub sub_exchange: String,
    pub sub_routing_key: String,
    sub_queue_name: Option<String>,
    pub_content_type: Option<String>,
    pub_content_encoding: Option<String>,
    app_id: Option<String>,
    pub_headers: Option<HashMap<String, String>>,
}

impl RpcServerParamsBuilder {
    pub fn pub_content_type(mut self, v: &str) -> RpcServerParamsBuilder {
        self.pub_content_type = Some(v.to_string());
        self
    }

    pub fn pub_content_encoding(mut self, v: &str) -> RpcServerParamsBuilder {
        self.pub_content_encoding = Some(v.to_string());
        self
    }

    pub fn app_id(mut self, v: &str) -> RpcServerParamsBuilder {
        self.app_id = Some(v.to_string());
        self
    }

    pub fn sub_queue_name(mut self, v: &str) -> RpcServerParamsBuilder {
        self.sub_queue_name = Some(v.to_string());
        self
    }

    pub fn pub_headers(mut self, v: &HashMap<String, String>) -> RpcServerParamsBuilder {
        self.pub_headers = Some(v.clone());
        self
    }

    pub fn build(self) -> RpcServerParams {
        let mut ret = RpcServerParams::default();
        if self.pub_content_type.is_some() {
            ret.pub_content_type = self.pub_content_type;
        }
        if self.pub_content_encoding.is_some() {
            ret.pub_content_encoding = self.pub_content_encoding;
        }
        if self.sub_queue_name.is_some() {
            ret.sub_queue_name = self.sub_queue_name;
        }
        if self.app_id.is_some() {
            ret.app_id = self.app_id;
        }
        if self.pub_headers.is_some() {
            ret.pub_headers = self.pub_headers;
        }
        ret
    }
}


#[derive(Debug, Clone, Default)]
pub struct RpcResponseParams {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub correlation_id: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}

impl RpcResponseParams {
    pub fn builder() -> RpcResponseParamsBuilder {
        RpcResponseParamsBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RpcResponseParamsBuilder {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub correlation_id: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}

impl RpcResponseParamsBuilder {
    pub fn content_type(mut self, v: &str) -> RpcResponseParamsBuilder {
        self.content_type = Some(v.to_string());
        self
    }
    pub fn content_encoding(mut self, v: &str) -> RpcResponseParamsBuilder {
        self.content_encoding = Some(v.to_string());
        self
    }
    pub fn correlation_id(mut self, v: &str) -> RpcResponseParamsBuilder {
        self.correlation_id = Some(v.to_string());
        self
    }
    pub fn message_id(mut self, v: &str) -> RpcResponseParamsBuilder {
        self.message_id = Some(v.to_string());
        self
    }
    pub fn timestamp(mut self, v: u64) -> RpcResponseParamsBuilder {
        self.timestamp = Some(v);
        self
    }
    pub fn message_type(mut self, v: &str) -> RpcResponseParamsBuilder {
        self.message_type = Some(v.to_string());
        self
    }
    pub fn user_id(mut self, v: &str) -> RpcResponseParamsBuilder {
        self.user_id = Some(v.to_string());
        self
    }
    pub fn headers(mut self, v: &HashMap<String, String>) -> RpcResponseParamsBuilder {
        self.headers = Some(v.clone());
        self
    }
    pub fn build(self) -> RpcResponseParams {
        RpcResponseParams {
            content_type: self.content_type,
            content_encoding: self.content_encoding,
            correlation_id: self.correlation_id,
            message_id: self.message_id,
            timestamp: self.timestamp,
            message_type: self.message_type,
            user_id: self.user_id,
            headers: self.headers,
        }
    }
}


impl<'a> BasicRpcServer<'a> {
    pub async fn new(rabbitclient: &'a RabbitClient, params: RpcServerParams) -> Result<BasicRpcServer<'a>, String> {
        let r = BasicRpcServer {
            rabbitclient,
            params,
            subscriber: None,
            publisher: None,
        };
        Ok(r)
    }

    async fn init_subscriber(&self) -> Result<(), String> {
        todo!();
    }

    async fn init_publisher(&self) -> Result<(), String> {
        todo!();
    }

    pub async fn start(&mut self) -> Result<&mut Receiver<SubscriptionContent>, String> {
        if self.subscriber.is_none() {
            if let Err(e) = self.init_subscriber().await {
                return Err(e);
            }
        }
        if self.publisher.is_none() {
            if let Err(e) = self.init_publisher().await {
                return Err(e);
            }
        }

        if let Some(s) = &mut self.subscriber {
            match s.subscribe_with_auto_ack().await {
                Ok(rx) => return Ok(rx),
                Err(e) => return Err(e.to_string()),
            };
        } else {
            Err("no subscriber object available".to_string())
        }
    }

    pub async fn send_respond(&self, content: Vec<u8>, reply_to_queue: &str, params: &RpcResponseParams) -> Result<(), String> {
        match &self.publisher {
            Some(p) => {
                let mut pp = PublishingParams::default();
                pp.exchange = Some("".to_string());
                pp.routing_key = Some(reply_to_queue.to_string());
                pp.app_id = Some(self.params.app_id.as_ref().unwrap().clone());

                if let Some(v) = &params.content_type {
                    pp.content_type = Some(v.clone());
                }
                if let Some(v) = &params.content_encoding {
                    pp.content_encoding = Some(v.clone());
                }
                if let Some(v) = &params.correlation_id {
                    pp.correlation_id = Some(v.clone());
                }
                if let Some(v) = &params.message_id {
                    pp.message_id = Some(v.clone());
                }
                if let Some(v) = &params.timestamp {
                    pp.timestamp = Some(*v);
                }
                if let Some(v) = &params.message_type {
                    pp.message_type = Some(v.clone());
                }
                if let Some(v) = &params.user_id {
                    pp.user_id = Some(v.clone());
                }
                if let Some(v) = &params.headers {
                    pp.headers = Some(v.clone());
                }

                if let Err(x) = p.publish_with_params(content, &pp).await {
                    return Err(x.to_string());
                } else {
                    return Ok(());
                }
            },
            None => return Err("no publisher object available".to_string()),
        }
    }
}

