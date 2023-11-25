use crate::{worker::Worker, rabbitclient::ClientCommand};
use tokio::sync::Mutex;
use std::{sync::Arc, collections::HashMap};
use amqprs::{channel::{Channel, BasicPublishArguments}, BasicProperties};
use amqprs::{FieldTable, FieldName, FieldValue, ShortStr, LongStr};
use log::{debug, error, info, warn};
use tokio::time::{sleep, Duration};
use tokio::task;

use tokio::sync::mpsc::{Receiver, Sender};
use crate::callbacks::RabbitChannelCallback;

pub struct Publisher {
    pub params: PublisherParams,
    pub worker: Arc<Mutex<Worker>>,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let w = self.worker.clone();
        task::spawn(async move {
            let mut worker_guard = w.lock().await;
            let worker: &mut Worker = &mut *worker_guard;
            debug!("worker (id={}) will be deleted", worker.id);
            if let Err(e) = worker.callback.tx_req.send(ClientCommand::RemoveWorker(worker.id)).await {
                error!("error while sending request to delete worker (id={}): {}",
                worker.id, e.to_string());
            }
        });
    }
}

impl Publisher {
    pub async fn new(id: u32, params: PublisherParams, tx_req: Sender<ClientCommand>) -> Result<Publisher, String> {
        let callback = RabbitChannelCallback {
            tx_req,
            id,
        };
        let worker_cont = Worker {
            id,
            channel: None,
            callback,
            tx_inform_about_new_channel: None,
        };
        Ok(Publisher {
            params,
            worker: Arc::new(Mutex::new(worker_cont)),
        })
    }

    pub async fn new_from_worker(worker: &Arc<Mutex<Worker>>, params: PublisherParams) -> Result<Publisher, String> {
        let w = worker.clone();
        Ok(Publisher {
            params,
            worker: w,
        })
    }

    pub async fn publish(&self, content: Vec<u8>) -> Result<(), PublishError> {
        let mut params = PublishingParams::default();
        if self.params.exchange.is_some() {
            params.exchange = self.params.exchange.clone();
        }
        if self.params.routing_key.is_some() {
            params.routing_key = self.params.routing_key.clone();
        }
        if self.params.content_type.is_some() {
            params.content_type = self.params.content_type.clone();
        }
        if self.params.content_encoding.is_some() {
            params.content_encoding = self.params.content_encoding.clone();
        }
        if self.params.priority.is_some() {
            params.priority = self.params.priority.clone();
        }
        if self.params.mandatory.is_some() {
            params.mandatory = self.params.mandatory.clone();
        }
        if self.params.expiration.is_some() {
            params.expiration = self.params.expiration.clone();
        }
        if self.params.message_type.is_some() {
            params.message_type = self.params.message_type.clone();
        }
        if self.params.user_id.is_some() {
            params.user_id = self.params.user_id.clone();
        }
        if self.params.reply_to.is_some() {
            params.reply_to = self.params.reply_to.clone();
        }
        params.headers = self.params.headers.clone();
        self.publish_with_params(content, &params).await
    }

    async fn create_headers(&self, params: &PublishingParams) -> Option<FieldTable> {
        if let Some(h) = &params.headers {
            let mut f = FieldTable::new();
            for k in h.keys() {
                let v = h.get(k);
                if let Some(c) = v {
                    let ks = ShortStr::try_from(k.as_str()).unwrap();
                    let key = FieldName::from(ks);
                    let hk = LongStr::try_from(c.as_str()).unwrap();
                    let header_cont = FieldValue::S(hk);
                    f.insert(key, header_cont);
                }
            }
            Some(f)
        } else {
            None
        }

    }

    async fn create_basic_props(&self, params: &PublishingParams) -> Result<BasicProperties, String> {
        let prio: Option<u8> = match &params.priority {
            Some(p) => {
                let v:u8 = match p {
                    MessagePriority::NormalLowest => 0,
                    MessagePriority::NormalLower => 1,
                    MessagePriority::NormalLow => 2,
                    MessagePriority::Normal => 3,
                    MessagePriority::NormalHigh => 4,
                    MessagePriority::HigherThanNormal => 5,
                    MessagePriority::HighLow => 6,
                    MessagePriority::High => 7,
                    MessagePriority::Higher => 8,
                    MessagePriority::Highest => 9,
                };
                Some(v)
            },
            None => None,
        };
        let headers = self.create_headers(params).await;
        let mut reply_to: Option<String> = None;
        if self.params.reply_to.is_some() {
            reply_to = self.params.reply_to.clone();
        } else {
            if params.reply_to.is_some() {
                reply_to = params.reply_to.clone();
            }
        }
        Ok(BasicProperties::new(
            params.content_type.clone(),
            params.content_encoding.clone(),
            headers,
            None,
            prio,
            params.correlation_id.clone(),
            reply_to,
            params.expiration.clone(),
            params.message_id.clone(),
            params.timestamp.clone(),
            params.message_type.clone(),
            params.user_id.clone(),
            params.app_id.clone(),
            None
        ))
    }

    async fn create_publish_args(&self, params: &PublishingParams) -> Result<BasicPublishArguments, String> {
        let mut pa = BasicPublishArguments::default();
        let exchange: &String;
        if let Some(e) = self.params.exchange.as_ref() {
            exchange = e;
        } else {
            if let Some(e) = params.exchange.as_ref() {
                exchange = e;
            } else {
                return Err("exchange is a needed parameter".to_string());
            }
        };
        let routing_key: &String;
        if let Some(r) = self.params.routing_key.as_ref() {
            routing_key = r;
        } else {
            if let Some(r) = params.routing_key.as_ref() {
                routing_key = r;
            } else {
                return Err("routing_key is a needed parameter".to_string());
            }
        };
        pa.exchange = exchange.clone();
        pa.routing_key = routing_key.clone();
        if params.mandatory.is_some() {
            pa.mandatory = params.mandatory.unwrap();
        }
        return Ok(pa);
    }


    async fn validate_and_process_params(&self, params: &PublishingParams) -> Result<(BasicProperties, BasicPublishArguments), String> {
        let ret: Result<(BasicProperties, BasicPublishArguments), String> = match self.create_basic_props(params).await {
            Ok(bp) => {
                match self.create_publish_args(params).await {
                    Ok(pa) => return Ok((bp, pa)),
                    Err(msg) => Err(msg),
                }
            },
            Err(msg) => Err(msg),
        };
        return ret;
    }

    async fn publish_with_params_impl(&self, content: Vec<u8>, params: &PublishingParams, channel: &Channel) -> Result<(), PublishError> {
        match self.validate_and_process_params(params).await {
            Ok((basic_props, publish_args)) => {
                match channel.basic_publish(basic_props, content, publish_args).await {
                    Ok(_) => {
                        return Ok(());
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        error!("error while publishing: {}", msg);
                        return Err(PublishError::PublishError(msg));
                    }
                }
            },
            Err(msg) => {
                return Err(PublishError::ParameterError(msg));
            }
        }
    }

    pub async fn publish_with_params(&self, content: Vec<u8>, params: &PublishingParams) -> Result<(), PublishError> {
        let mut reconnect_millis = 500;
        let mut reconnect_attempts: u8 = 0;
        let max_reconnect_attempts = 5;
        loop {
            let mut worker_guard = self.worker.lock().await;
            let worker: &mut Worker = &mut *worker_guard;
            match &worker.channel {
                Some(c) => {
                    info!("publish to channel={}", c.channel_id().to_string());
                    return self.publish_with_params_impl(content, &params, &c).await;
                },
                None => {
                    if reconnect_attempts > max_reconnect_attempts {
                        let msg = format!("reached maximum attempts ({}), but channel not open to publish",
                        reconnect_attempts);
                        error!("{}", msg);
                        return Err(PublishError::ChannelNotOpen(worker.id));
                    } else {
                        let sleep_time = Duration::from_millis(reconnect_millis);
                        debug!("sleep for {} seconds before try to reestablish topology ...",reconnect_millis);
                        sleep( sleep_time ).await;
                        reconnect_millis = reconnect_millis * 2;
                        reconnect_attempts += 1;
                    }
                },
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PublisherParams {
    pub exchange: Option<String>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub priority: Option<MessagePriority>,
    pub message_type: Option<String>,
    pub routing_key: Option<String>,
    pub expiration: Option<String>,
    pub mandatory: Option<bool>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub reply_to: Option<String>,
}

impl PublisherParams {
    pub fn builder() -> PublisherParamsBuilder {
        PublisherParamsBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PublisherParamsBuilder {
    exchange: Option<String>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    priority: Option<MessagePriority>,
    message_type: Option<String>,
    routing_key: Option<String>,
    expiration: Option<String>,
    mandatory: Option<bool>,
    user_id: Option<String>,
    app_id: Option<String>,
    headers: Option<HashMap<String, String>>,
    reply_to: Option<String>,
}

impl PublisherParamsBuilder {
    pub fn new() -> PublisherParamsBuilder {
        PublisherParamsBuilder::default()
    }

    pub fn exchange(mut self,v: &str) -> PublisherParamsBuilder {
        self.exchange = Some(v.to_string());
        self
    }
    pub fn content_type(mut self,v: &str) -> PublisherParamsBuilder {
        self.content_type = Some(v.to_string());
        self
    }
    pub fn content_encoding(mut self,v: &str) -> PublisherParamsBuilder {
        self.content_encoding = Some(v.to_string());
        self
    }
    pub fn priority(mut self,v: MessagePriority) -> PublisherParamsBuilder {
        self.priority = Some(v);
        self
    }
    pub fn message_type(mut self,v: &str) -> PublisherParamsBuilder {
        self.message_type = Some(v.to_string());
        self
    }
    pub fn routing_key(mut self,v: &str) -> PublisherParamsBuilder {
        self.routing_key = Some(v.to_string());
        self
    }
    pub fn mandatory(mut self,v: bool) -> PublisherParamsBuilder {
        self.mandatory = Some(v);
        self
    }
    pub fn expiration(mut self,v: &str) -> PublisherParamsBuilder {
        self.expiration = Some(v.to_string());
        self
    }
    pub fn user_id(mut self,v: &str) -> PublisherParamsBuilder {
        self.user_id = Some(v.to_string());
        self
    }
    pub fn app_id(mut self,v: &str) -> PublisherParamsBuilder {
        self.app_id = Some(v.to_string());
        self
    }
    pub fn reply_to(mut self, v: &str) -> PublisherParamsBuilder {
        self.reply_to = Some(v.to_string());
        self
    }
    pub fn headers(mut self,v: &HashMap<String, String>) -> PublisherParamsBuilder {
        self.headers = Some(v.clone());
        self
    }

    pub fn build(self) -> PublisherParams {
        PublisherParams {
            exchange: self.exchange,
            content_type: self.content_type,
            content_encoding: self.content_encoding,
            priority: self.priority,
            message_type: self.message_type,
            routing_key: self.routing_key,
            expiration: self.expiration,
            mandatory: self.mandatory,
            user_id: self.user_id,
            app_id: self.app_id,
            headers: self.headers,
            reply_to: self.reply_to,
        }
    }
}


#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum MessagePriority {
    /// AMQP value 0
    NormalLowest,
    /// AMQP value 1
    NormalLower,
    /// AMQP value 2
    NormalLow,
    /// AMQP value 3
    Normal,
    /// AMQP value 4
    #[default]
    NormalHigh,
    /// AMQP value 5
    HigherThanNormal,
    /// AMQP value 6
    HighLow,
    /// AMQP value 7
    High,
    /// AMQP value 8
    Higher,
    /// AMQP value 9
    Highest,
}

#[derive(Debug, Clone, Default)]
pub struct PublishingParams {
    pub exchange: Option<String>,
    pub routing_key: Option<String>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub priority: Option<MessagePriority>,
    pub mandatory: Option<bool>,
    pub correlation_id: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub reply_to: Option<String>,
}

impl PublishingParams {
    pub fn builder() -> PublishingParamsBuilder {
        PublishingParamsBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PublishingParamsBuilder {
    exchange: Option<String>,
    routing_key: Option<String>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    priority: Option<MessagePriority>,
    mandatory: Option<bool>,
    correlation_id: Option<String>,
    expiration: Option<String>,
    message_id: Option<String>,
    timestamp: Option<u64>,
    message_type: Option<String>,
    user_id: Option<String>,
    app_id: Option<String>,
    headers: Option<HashMap<String, String>>,
    reply_to: Option<String>,
}

impl PublishingParamsBuilder {
    pub fn new() -> PublishingParamsBuilder {
        PublishingParamsBuilder::default()
    }

    pub fn exchange(mut self,v: &str) -> PublishingParamsBuilder {
        self.exchange = Some(v.to_string());
        self
    }
    pub fn routing_key(mut self,v: &str) -> PublishingParamsBuilder {
        self.routing_key = Some(v.to_string());
        self
    }
    pub fn content_type(mut self,v: &str) -> PublishingParamsBuilder {
        self.content_type = Some(v.to_string());
        self
    }
    pub fn content_encoding(mut self,v: &str) -> PublishingParamsBuilder {
        self.content_encoding = Some(v.to_string());
        self
    }
    pub fn priority(mut self,v: MessagePriority) -> PublishingParamsBuilder {
        self.priority = Some(v);
        self
    }
    pub fn mandatory(mut self,v: bool) -> PublishingParamsBuilder {
        self.mandatory = Some(v);
        self
    }
    pub fn correlation_id(mut self,v: &str) -> PublishingParamsBuilder {
        self.correlation_id = Some(v.to_string());
        self
    }
    pub fn expiration(mut self,v: &str) -> PublishingParamsBuilder {
        self.expiration = Some(v.to_string());
        self
    }
    pub fn message_id(mut self,v: &str) -> PublishingParamsBuilder {
        self.message_id = Some(v.to_string());
        self
    }
    pub fn timestamp(mut self,v: u64) -> PublishingParamsBuilder {
        self.timestamp = Some(v);
        self
    }
    pub fn message_type(mut self,v: &str) -> PublishingParamsBuilder {
        self.message_type = Some(v.to_string());
        self
    }
    pub fn user_id(mut self,v: &str) -> PublishingParamsBuilder {
        self.user_id = Some(v.to_string());
        self
    }
    pub fn app_id(mut self,v: &str) -> PublishingParamsBuilder {
        self.app_id = Some(v.to_string());
        self
    }
    pub fn reply_to(mut self,v: &str) -> PublishingParamsBuilder {
        self.reply_to = Some(v.to_string());
        self
    }
    pub fn headers(mut self,v: &HashMap<String, String>) -> PublishingParamsBuilder {
        self.headers = Some(v.clone());
        self
    }
    pub fn build(self) -> PublishingParams {
        PublishingParams {
            exchange: self.exchange,
            routing_key: self.routing_key,
            content_type: self.content_type,
            content_encoding: self.content_encoding,
            priority: self.priority,
            mandatory: self.mandatory,
            correlation_id: self.correlation_id,
            expiration: self.expiration,
            message_id: self.message_id,
            timestamp: self.timestamp,
            message_type: self.message_type,
            user_id: self.user_id,
            app_id: self.app_id,
            headers: self.headers,
            reply_to: self.reply_to
        }
    }
}


pub enum PublishError {
    Todo,
    ChannelNotOpen(u32),
    ParameterError(String),
    PublishError(String),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PublishError::Todo => write!(f, "Todo error occurred"),
            PublishError::ChannelNotOpen(id) => write!(f, "Channel is not open, worker id={}", id),
            PublishError::ParameterError(msg) => write!(f, "Parameter error: {}", msg),
            PublishError::PublishError(msg) => write!(f, "Publish error: {}", msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::publisher::{PublisherParams, PublishingParams, MessagePriority};

    #[test]
    fn publisher_builder_test() {
        let p0 = PublisherParams::builder().build();
        assert_eq!(None, p0.exchange);
        assert_eq!(None, p0.content_type);
        assert_eq!(None, p0.content_encoding);
        assert_eq!(None, p0.priority);
        assert_eq!(None, p0.message_type);
        assert_eq!(None, p0.routing_key);
        assert_eq!(None, p0.mandatory);
 
 
        let p1 = PublisherParams::builder()
        .exchange("test_exchange")
        .content_type("test_content_type")
        .content_encoding("test_content_encoding")
        .priority(MessagePriority::HighLow)
        .message_type("test_message_type")
        .routing_key("test_routing_key")
        .mandatory(true)
        .build();

        assert_eq!("test_exchange", p1.exchange.unwrap());
        assert_eq!("test_content_type", p1.content_type.unwrap());
        assert_eq!("test_content_encoding", p1.content_encoding.unwrap());
        assert_eq!(MessagePriority::HighLow, p1.priority.unwrap());
        assert_eq!("test_message_type", p1.message_type.unwrap());
        assert_eq!("test_routing_key", p1.routing_key.unwrap());
        assert_eq!(true, p1.mandatory.unwrap());

        let p2 = PublisherParams::builder()
        .priority(MessagePriority::NormalLower)
        .mandatory(false)
        .build();

        assert_eq!(None, p2.exchange);
        assert_eq!(None, p2.content_type);
        assert_eq!(None, p2.content_encoding);
        assert_eq!(MessagePriority::NormalLower, p2.priority.unwrap());
        assert_eq!(None, p2.message_type);
        assert_eq!(None, p2.routing_key);
        assert_eq!(false, p2.mandatory.unwrap());

    }

    #[test]
    fn publishingparams_builder_test() {
        // TODO
    }


}