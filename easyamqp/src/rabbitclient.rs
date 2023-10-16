//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{error, warn};
use std::result::Result;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::client_impl::ClientImpl;

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

pub enum ExchangeType {
    /// Fanout exchange
    Fanout,
    /// Topic exchange
    Topic,
    /// Direct exchange
    Direct,
    /// Headers exchange
    Headers,
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

pub enum ExchangeExistsResult {
    Yes,
    WrongParameters,
    No,
    
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

    pub async fn create_exchange(&self, name: String, exhange_type: ExchangeType, durable: bool, auto_delete: bool) -> Result<(), String> {
        return self.client_impl.create_exchange(name, exhange_type, durable, auto_delete).await;
    }

    pub async fn does_exchange_exist(&self,name: String, exhange_type: ExchangeType, durable: bool, auto_delete: bool) -> Result<ExchangeExistsResult, String> {
        return self.client_impl.does_exchange_exist(name, exhange_type, durable, auto_delete).await;
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
