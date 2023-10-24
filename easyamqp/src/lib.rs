mod callbacks;
mod publisher;
mod subscriber;
mod rabbitclient;
mod topology;
pub mod utils;

pub use rabbitclient::{RabbitConParams, RabbitConParamsBuilder, RabbitClient};
pub use topology::{ExchangeDefinition, ExchangeType, ExchangeDefinitionBuilder,
    QueueDefinition, QueueDefinitionBuilder, QueueBindingDefinition};

pub fn dummy() {
    println!("    Hello, from the lib!");
}
