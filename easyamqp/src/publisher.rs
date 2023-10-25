use crate::{worker::Worker, rabbitclient::ClientCommand};
use tokio::sync::Mutex;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};
use crate::callbacks::RabbitChannelCallback;

pub struct Publisher {
    pub params: PublisherParams,
    pub worker: Arc<Mutex<Worker>>,
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
        };
        Ok(Publisher {
            params,
            worker: Arc::new(Mutex::new(worker_cont)),
        })
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
    pub mandatory: Option<bool>,
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
    mandatory: Option<bool>,
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

    pub fn build(self) -> PublisherParams {
        PublisherParams {
            exchange: self.exchange,
            content_type: self.content_type,
            content_encoding: self.content_encoding,
            priority: self.priority,
            message_type: self.message_type,
            routing_key: self.routing_key,
            mandatory: self.mandatory,
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
    pub timestamp: Option<String>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
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
    timestamp: Option<String>,
    message_type: Option<String>,
    user_id: Option<String>,
}

impl PublishingParamsBuilder {
    pub fn new() -> PublishingParamsBuilder {
        PublishingParamsBuilder::default()
    }

    pub fn exchange(mut self,v: &str) -> PublishingParamsBuilder {
        self.exchange = Some(v.to_string());
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
    pub fn message_type(mut self,v: &str) -> PublishingParamsBuilder {
        self.message_type = Some(v.to_string());
        self
    }
    pub fn routing_key(mut self,v: &str) -> PublishingParamsBuilder {
        self.routing_key = Some(v.to_string());
        self
    }
    pub fn mandatory(mut self,v: bool) -> PublishingParamsBuilder {
        self.mandatory = Some(v);
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
        }
    }
}


pub enum PublishError {
    Todo,
}

impl Publisher {
    pub fn publish(content: Vec<u8>, params: PublishingParams) -> Result<(), PublishError> {
        Err(PublishError::Todo)
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