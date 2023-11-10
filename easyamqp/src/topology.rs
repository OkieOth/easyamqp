
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

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
    pub exchange_type: ExchangeType,

    /// Specifies whether the exchange should survive server restarts.
    pub durable: bool,

    /// Indicates whether the exchange should be deleted when it's no longer in use.
    pub auto_delete: bool,
}

impl ExchangeDefinition {
    pub fn builder(exchange_name: &str) -> ExchangeDefinitionBuilder {
        ExchangeDefinitionBuilder::default().name(exchange_name)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExchangeDefinitionBuilder {
    name: String,

    /// The type of the exchange, indicating how it routes messages to queues.
    exchange_type: ExchangeType,

    /// Specifies whether the exchange should survive server restarts.
    durable: bool,

    /// Indicates whether the exchange should be deleted when it's no longer in use.
    auto_delete: bool,
}

impl ExchangeDefinitionBuilder {
    pub fn new(exchange_name: &str) -> ExchangeDefinitionBuilder {
        ExchangeDefinitionBuilder::default().name(exchange_name)
    }
    pub fn name(mut self, exchange_name: &str) -> ExchangeDefinitionBuilder {
        self.name = exchange_name.to_string();
        self
    }
    pub fn exchange_type(mut self, exchange_type: ExchangeType) -> ExchangeDefinitionBuilder {
        self.exchange_type = exchange_type;
        self
    }
    pub fn durable(mut self, durable: bool) -> ExchangeDefinitionBuilder {
        self.durable = durable;
        self
    }
    pub fn auto_delete(mut self, auto_delete: bool) -> ExchangeDefinitionBuilder {
        self.auto_delete = auto_delete;
        self
    }
    pub fn build(self) -> ExchangeDefinition {
        ExchangeDefinition {
            name: self.name,
            exchange_type: self.exchange_type,
            durable: self.durable,
            auto_delete: self.auto_delete,
        }
    } 
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

impl QueueDefinition {
    pub fn builder(queue_name: &str) -> QueueDefinitionBuilder {
        QueueDefinitionBuilder::default().name(queue_name)
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueueDefinitionBuilder {
    name: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
}

impl QueueDefinitionBuilder {
    pub fn new(queue_name: &str) -> QueueDefinitionBuilder {
        QueueDefinitionBuilder::default().name(queue_name)
    }
    pub fn name(mut self, queue_name: &str) -> QueueDefinitionBuilder {
        self.name = queue_name.to_string();
        self
    }
    pub fn durable(mut self, durable: bool) -> QueueDefinitionBuilder {
        self.durable = durable;
        self
    }
    pub fn exclusive(mut self, exclusive: bool) -> QueueDefinitionBuilder {
        self.exclusive = exclusive;
        self
    }
    pub fn auto_delete(mut self, auto_delete: bool) -> QueueDefinitionBuilder {
        self.auto_delete = auto_delete;
        self
    }
    pub fn build(self) -> QueueDefinition {
        QueueDefinition {
            name: self.name,
            durable: self.durable,
            exclusive: self.exclusive,
            auto_delete: self.auto_delete,
        }
    }
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

impl QueueBindingDefinition {
    pub fn new(queue: &str, exchange: &str, routing_key: &str) -> QueueBindingDefinition {
        QueueBindingDefinition { 
            queue: queue.to_string(), 
            exchange: exchange.to_string(), 
            routing_key: routing_key.to_string() }
    }
}

struct Subscriber {
    id: u32,
    queue_and_sender: Option<(String, Sender<u32>)>,
}


pub struct Topology {
    pub exchanges: Vec<ExchangeDefinition>,
    pub queues: Vec<QueueDefinition>,
    pub bindings: Vec<QueueBindingDefinition>,
    subscribers: Vec<Subscriber>,
}


impl Topology {
    pub fn new() -> Topology {
        Topology { 
            exchanges: Vec::new(),
            queues: Vec::new(),
            bindings: Vec::new(),
            subscribers: Vec::new(),
        }
    }

    pub async fn declare_all_exchanges(&self, con: &Connection) -> Result<(), String> {
        for e in self.exchanges.iter() {
            if let Err(e) = self.declare_exchange_base(e, con).await {
                return Err(e);
            }
        }
        Ok(())
    }
    pub async fn declare_all_queues(&self, con: &Connection) -> Result<(), String> {
        for q in self.queues.iter() {
            if let Err(e) = self.declare_queue_base(q, con).await {
                return Err(e);
            }
        }
        Ok(())
    }
    pub async fn declare_all_bindings(&self, con: &Connection) -> Result<(), String> {
        for b in self.bindings.iter() {
            if let Err(e) = self.declare_queue_binding_base(b, con).await {
                return Err(e);
            }
        }
        Ok(())
    }

    async fn declare_exchange_base(&self,exchange_def: &ExchangeDefinition, con: &Connection) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let type_str: String = exchange_def.exchange_type.to_string();
        let mut args = ExchangeDeclareArguments::new(
            exchange_def.name.as_str(), type_str.as_str());
        args.auto_delete = exchange_def.auto_delete;
        args.durable = exchange_def.durable;
        if let Err(e) = channel.exchange_declare(args).await {
            return Err(e.to_string());
        };
        Ok(())
    }


    pub async fn declare_exchange(&mut self,exchange_def: ExchangeDefinition, con: &Connection) -> Result<(), String> {
        match self.declare_exchange_base(&exchange_def, con).await {
            Ok(_) => {
                if exchange_def.auto_delete {
                    self.exchanges.push(exchange_def);
                }
                Ok(())
            },
            Err(e) => {
                return Err(e);
            }
        }
    }

    async fn declare_queue_base(&self, queue_def: &QueueDefinition, con: &Connection) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let queue_name = queue_def.name.as_str();
        let mut args = QueueDeclareArguments::new(queue_name);
        args.auto_delete(queue_def.auto_delete);
        args.durable(queue_def.durable);
        if let Err(e) = channel.queue_declare(args).await {
            return Err(e.to_string());
        };
        Ok(())
    }


    pub async fn declare_queue(&mut self, queue_def: QueueDefinition, con: &Connection) -> Result<(), String> {
        match self.declare_queue_base(&queue_def, con).await {
            Ok(_) => {
                // if the exchange is of type auto_delete, maybe the topology needs to be restored
                // after a connection loss
                if queue_def.auto_delete {
                    self.queues.push(queue_def);
                }
                Ok(())
            },
            Err(e) => {
                return Err(e);
            }
        }
    }

    async fn declare_queue_binding_base(&self, binding_def: &QueueBindingDefinition, con: &Connection) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let args = QueueBindArguments::new(
            &binding_def.queue.as_str(),
            &binding_def.exchange.as_str(),
            &binding_def.routing_key.as_str());
        if let Err(e) = channel.queue_bind(args).await {
            return Err(e.to_string());
        };
        Ok(())
    }


    pub async fn declare_queue_binding(&mut self, binding_def: QueueBindingDefinition, con: &Connection) -> Result<(), String> {
        match self.declare_queue_binding_base(&binding_def, con).await {
            Ok(_) => {
                // if the exchange is of type auto_delete, maybe the topology needs to be restored
                // after a connection loss
                {
                    let exchange_name = &binding_def.exchange;
                    let queue_name = &binding_def.queue;
                    let exchange_result = self.exchanges
                        .iter()
                        .filter(|item| (item.name == *exchange_name) && (item.auto_delete == true))
                        .next();
                    let queue_result = self.queues
                        .iter()
                        .filter(|item| (item.name == *queue_name) && (item.auto_delete == true))
                        .next();
                    if exchange_result.is_some() || queue_result.is_some() {
                        self.bindings.push(binding_def);
                    }
                }
                Ok(())
            },
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub async fn register_subscriber(&mut self, id: u32) {
        let s = Subscriber {
            id,
            queue_and_sender: None,
        };
        self.subscribers.push(s);
    }

    pub async fn remove_subscriber(&mut self, id_to_remove: u32) {
        self.subscribers.retain(|s| s.id != id_to_remove );
    }

    pub async fn get_subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    pub async fn init_queue(&mut self, id_to_init: u32, queue_name: &str, tx_infrom_about_new_channel: &Sender<u32>) {
        for s in self.subscribers.iter_mut() {
            if s.id == id_to_init {
                s.queue_and_sender = Some((queue_name.to_string(), tx_infrom_about_new_channel.clone()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::topology;

    #[test]
    fn exchangedefinition_builder_test() {
        let param1 = topology::ExchangeDefinition::builder("first")
            .durable(true)
            .build();
        assert_eq!("first", param1.name);
        assert_eq!(true, param1.durable);
        assert_eq!(topology::ExchangeType::Topic, param1.exchange_type);
        assert_eq!(false, param1.auto_delete);

        let param2 = topology::ExchangeDefinition::builder("x")
            .exchange_type(topology::ExchangeType::Fanout)
            .auto_delete(true)
            .build();
        assert_eq!("x", param2.name);
        assert_eq!(false, param2.durable);
        assert_eq!(topology::ExchangeType::Fanout, param2.exchange_type);
        assert_eq!(true, param2.auto_delete);
    }

    #[test]
    fn queuedefinition_builder_test() {
        let param1 = topology::QueueDefinition::builder("first_queue")
            .durable(true)
            .build();
        assert_eq!("first_queue", param1.name);
        assert_eq!(true, param1.durable);
        assert_eq!(false, param1.exclusive);
        assert_eq!(false, param1.auto_delete);

        let param2 = topology::QueueDefinition::builder("second_queue")
            .exclusive(true)
            .durable(true)
            .build();
        assert_eq!("second_queue", param2.name);
        assert_eq!(true, param2.durable);
        assert_eq!(true, param2.exclusive);
        assert_eq!(false, param2.auto_delete);
    }

    #[test]
    fn queuebindingsdefinition_builder_test() {
        let param1 = topology::QueueBindingDefinition::new(
            "second_queue", "second", "second.#");
        assert_eq!("second_queue", param1.queue);
        assert_eq!("second", param1.exchange);
        assert_eq!("second.#", param1.routing_key);
    }

}
