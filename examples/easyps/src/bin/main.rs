use env_logger::Env;
use easyamqp::{RabbitClient, RabbitConParams,
    ExchangeDefinition, ExchangeType, 
    QueueDefinition, QueueBindingDefinition,
    Publisher, PublishingParams,
    Subscriber, SubscribeParams, SubscriptionContent, SubscriptionResponse};
use log::{info, error};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

use easyps_impl::subscriber::SubscribeArgs;
use easyps_impl::publisher::PublishArgs;
use clap::Parser;
use clap::{Subcommand};

/// Simple programm to publish and subscribe to an Rabbitmq instance
#[derive(Debug, Parser)]
#[clap(name = "easyps", version)]
pub struct App {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Use the program to publish to Rabbitmq
    Publish(PublishArgs),

    /// Use the program to subscribe data from Rabbitmq
    Subscribe(SubscribeArgs),
}



fn main() {
    let env = Env::default().filter_or("LOG_LEVEL", "info");
    env_logger::init_from_env(env);

    let args = <App as Parser>::parse();

    info!("started ...");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        });
}

