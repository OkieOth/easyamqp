use std::sync::Arc;
use tokio::sync::Mutex;


use crate::rabbitclient::ClientImplCont;

use amqprs::{
    callbacks::{ChannelCallback, ConnectionCallback},
    channel::{Channel, ExchangeDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
    Ack, BasicProperties, Cancel, Close, CloseChannel, Nack, Return,
};


#[derive(Debug, Clone, Default)]
/// Represents parameters for configuring a message exchange.
pub struct ExchangeDefinition {
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

#[derive(Debug, Clone, Default)]
/// Represents parameters for configuring an AMQP queue.
pub struct QueueDefinition {
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

#[derive(Debug, Clone, Default)]
pub struct QueueBindingDefinition {
    /// Queue name. Default: "".
    pub queue: String,
    /// Exchange name. Default: "".
    pub exchange: String,
    /// Default: "".
    pub routing_key: String,
}

pub struct Topology {
    pub exchanges: Vec<ExchangeDefinition>,
    pub queues: Vec<QueueDefinition>,
    pub bindings: Vec<QueueBindingDefinition>,
    pub cont: Arc<Mutex<ClientImplCont>>,
}

impl Topology {
    pub async fn declare_exchange(&self,params: ExchangeDefinition) -> Result<(), String> {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        match &client_cont.connection {
            Some(con) => {
                if con.is_open() {
                    return self.do_declare_exchange(&con, params).await;
                } else {
                    return Err("broker connection isn't open".to_string());
                }
            },
            None => {
                return Err("no broker connection available".to_string());
            }
        }
    }

    pub async fn do_declare_exchange(&self,con: &Connection,params: ExchangeDefinition) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let type_str: String = params.exhange_type.into();
        let mut args = ExchangeDeclareArguments::new(params.name.as_str(), type_str.as_str());
        args.auto_delete = params.auto_delete;
        args.durable = params.durable;
        if let Err(e) = channel.exchange_declare(args).await {
            return Err(e.to_string());
        };
        Ok(())
    }
}