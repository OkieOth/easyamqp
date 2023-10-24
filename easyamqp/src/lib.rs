mod callbacks;
mod publisher;
mod subscriber;
mod rabbitclient;
mod topology;
pub mod utils;

pub use rabbitclient::{RabbitConParams, RabbitClient};
pub use topology::{ExchangeDefinition, ExchangeType, QueueDefinition, QueueBindingDefinition};

pub fn dummy() {
    println!("    Hello, from the lib!");
}
