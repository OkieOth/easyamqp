
use amqprs::{
    channel::{ExchangeDeclareArguments, QueueDeclareArguments, QueueBindArguments},
    connection::Connection,
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

impl ToString for ExchangeType {
    fn to_string(&self) -> String {
        match self {
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
}


impl Topology {
    pub async fn declare_all_exchanges(&self) -> Result<(), String> {
        Ok(())
    }
    pub async fn declare_all_queues(&self) -> Result<(), String> {
        Ok(())
    }
    pub async fn declare_all_bindings(&self) -> Result<(), String> {
        Ok(())
    }

    pub async fn declare_exchange(&mut self,exchange_def: ExchangeDefinition, con: &Connection) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let type_str: String = exchange_def.exhange_type.to_string();
        let mut args = ExchangeDeclareArguments::new(
            exchange_def.name.as_str(), type_str.as_str());
        args.auto_delete = exchange_def.auto_delete;
        args.durable = exchange_def.durable;
        if let Err(e) = channel.exchange_declare(args).await {
            return Err(e.to_string());
        };
        // if the exchange is of type auto_delete, maybe the topology needs to be restored
        // after a connection loss
        if exchange_def.auto_delete {
            self.exchanges.push(exchange_def);
        }
        Ok(())
    }

    pub async fn declare_queue(&mut self, queue_def: QueueDefinition, con: &Connection) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let queue_name = queue_def.name.as_str();
        let mut args = QueueDeclareArguments::new(queue_name);
        args.auto_delete(queue_def.auto_delete);
        args.durable(queue_def.durable);
        if let Err(e) = channel.queue_declare(args).await {
            return Err(e.to_string());
        };
        // if the exchange is of type auto_delete, maybe the topology needs to be restored
        // after a connection loss
        if queue_def.auto_delete {
            self.queues.push(queue_def);
        }
        Ok(())
    }

    pub async fn declare_queue_binding(&mut self, binding_def: QueueBindingDefinition, con: &Connection) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let queue_name = binding_def.queue.clone();
        let exchange_name = binding_def.exchange.clone();
        let args = QueueBindArguments::new(
            &binding_def.queue.as_str(),
            &binding_def.exchange.as_str(),
            &binding_def.routing_key.as_str());
        if let Err(e) = channel.queue_bind(args).await {
            return Err(e.to_string());
        };
        // if the exchange is of type auto_delete, maybe the topology needs to be restored
        // after a connection loss
        {
            let exchange_result = self.exchanges
                .iter()
                .filter(|item| (item.name == exchange_name) && (item.auto_delete == true))
                .next();
            let queue_result = self.queues
                .iter()
                .filter(|item| (item.name == queue_name) && (item.auto_delete == true))
                .next();
            if exchange_result.is_some() || queue_result.is_some() {
                self.bindings.push(binding_def);
            }
        }
        Ok(())
    }

}