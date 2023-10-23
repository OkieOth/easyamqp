//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{error, warn};
use std::default;
use std::result::Result;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::client_impl::ClientImpl;
use crate::publisher::Publisher;
use crate::subscriber::Subscriber;

/// Container for the connection parameters for the broker connection
#[derive(Debug, Clone, Default)]
pub struct RabbitConParams {
    /// Connection name
    pub con_name: Option<String>,
    /// Server name or IP address to connect to
    pub server: String,
    /// Port of the RabbitMq server
    pub port: u16,
    /// User used for authentication
    pub user: String,
    /// Password used for authentication
    pub password: String,
}

#[derive(Debug, Clone, Default)]
/// Represents parameters for configuring a message exchange.
pub struct ExchangeParams {
    /// The name of the exchange. It is a string that identifies the exchange.
    pub name: String,

    /// The type of the exchange, indicating how it routes messages to queues.
    pub exhange_type: ExchangeType,

    /// Specifies whether the exchange should survive server restarts.
    pub durable: bool,

    /// Indicates whether the exchange should be deleted when it's no longer in use.
    pub auto_delete: bool,
}

#[derive(Debug, Clone, Default)]
/// Supported types of Exchanges
pub enum ExchangeType {
    /// Fanout exchange
    Fanout,
    /// Topic exchange
    #[default]
    Topic,
    /// Direct exchange
    Direct,
    /// Headers exchange
    Headers,
}

#[derive(Debug, Clone, Default)]
/// Represents parameters for configuring an AMQP queue.
pub struct QueueParams {
    /// The name of the queue.
    pub name: String,

    /// Specifies whether the queue should survive server restarts.
    /// Defaults to `false`.
    pub durable: bool,

    /// Indicates whether the queue can only be accessed by the current connection.
    /// Defaults to `false`.
    pub exclusive: bool,

    /// Indicates whether the queue should be deleted when it's no longer in use.
    /// Defaults to `false`.
    pub auto_delete: bool,
}

const EXCHANGE_TYPE_FANOUT: &str = "fanout";
const EXCHANGE_TYPE_TOPIC:  &str = "topic";
const EXCHANGE_TYPE_DIRECT:  &str = "direct";
const EXCHANGE_TYPE_HEADERS:  &str = "headers";

impl From<ExchangeType> for String {
    fn from(value: ExchangeType) -> String {
        match value {
            ExchangeType::Fanout => EXCHANGE_TYPE_FANOUT.to_owned(),
            ExchangeType::Topic => EXCHANGE_TYPE_TOPIC.to_owned(),
            ExchangeType::Direct => EXCHANGE_TYPE_DIRECT.to_owned(),
            ExchangeType::Headers => EXCHANGE_TYPE_HEADERS.to_owned(),
        }
    }
}


pub struct RabbitClient {
    client_impl: ClientImpl,
}

impl RabbitClient {
    pub async fn new(con_params: RabbitConParams) -> Self {
        let client_impl = ClientImpl::new(con_params).await;
        RabbitClient {
            client_impl,
        }
    }

    pub async fn connect(&mut self) -> Result<(), String> {
        return self.client_impl.connect().await;
    }

    pub async fn close(&self) {
        // TODO
    }

    pub async fn dummy(&self, id: u32) -> Result<String, ()> {
        let r = self.client_impl.dummy(id).await;
        return Ok(r);
    }

    pub async fn set_panic_sender(&self, tx_panic: Sender<u32>) {
        self.client_impl.set_panic_sender(tx_panic).await;
    }

    pub async fn create_exchange(&self, name: &str, exhange_type: ExchangeType, durable: bool, auto_delete: bool) -> Result<(), String> {
        return self.client_impl.create_exchange(name, exhange_type, durable, auto_delete).await;
    }

    pub async fn new_publisher(&self, exchange: &str) -> Result<Publisher, String> {
        Err("TODO".to_string())
    }

    pub async fn new_subscriber(&self, exchange: &str, routing_key: &str, queue: &str) -> Result<Subscriber, String> {
        Err("TODO".to_string())
    }

}


#[cfg(test)]
mod tests {
    use crate::rabbitclient;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn dummy() {
        let params = rabbitclient::RabbitConParams {
            con_name: None,
            server: "127.0.0.1".to_string(),
            port: 5672,
            user: "guest".to_string(),
            password: "guest".to_string(),
        };


        let client = rabbitclient::RabbitClient::new(params).await;

        for _ in 0 .. 1000 {
            for id in 0 .. 10 {
                let dummy_result = client.dummy(id).await.unwrap();
                assert_eq!(format!("hello from dummy to id={}", id), dummy_result);
            }
        }

        //client.close().await;
    }
}
