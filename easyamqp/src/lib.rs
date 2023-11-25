mod callbacks;
mod publisher;
mod subscriber;
mod rabbitclient;
mod topology;
mod worker;
pub mod utils;
pub mod rpc_client;

pub use rabbitclient::{RabbitConParams, RabbitConParamsBuilder, RabbitClient};
pub use topology::{ExchangeDefinition, ExchangeType, ExchangeDefinitionBuilder,
    QueueDefinition, QueueDefinitionBuilder, QueueBindingDefinition};
pub use publisher::{Publisher, PublishError, PublisherParams, PublishingParams,
    PublisherParamsBuilder, PublishingParamsBuilder};
pub use subscriber::{Subscriber, SubscribeParams, SubscriptionContent, SubscriptionResponse};


pub fn dummy() {
    println!("    Hello, from the lib!");
}
