use crate::{worker::Worker, rabbitclient::ClientCommand};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};
use crate::callbacks::RabbitChannelCallback;
use log::{debug, error, info, warn};
use amqprs::consumer::AsyncConsumer;
use amqprs::channel::{BasicAckArguments, BasicConsumeArguments, Channel};
use amqprs::{Deliver, BasicProperties};



pub struct Subscriber {
    pub worker: Arc<Mutex<Worker>>,
    consumer: Box<dyn Fn(&Vec<u8>) -> bool>,
}

#[async_trait::async_trait]
impl AsyncConsumer for Subscriber {
    async fn consume(
        &mut self, // use `&mut self` to make trait object to be `Sync`
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        (self.consumer)(&content);
        // if (self.consumer)(&content) {
        //     let args = BasicAckArguments::new(deliver.delivery_tag(), false);
        //     channel.basic_ack(args).await.unwrap();
        // }
    }

}

impl Subscriber {
    pub async fn new(id: u32, tx_req: Sender<ClientCommand>, consumer: dyn Fn(Vec<u8>) -> bool, params: SubscribeParams) -> Result<Subscriber, String> {
        let callback = RabbitChannelCallback {
            tx_req,
            id,
        };
        let worker_cont = Worker {
            id,
            channel: None,
            callback,
        };
        Ok(Subscriber {
            worker: Arc::new(Mutex::new(worker_cont)),
            consumer: Box::new(consumer),
        })
    }

    pub async fn subscibe(&self) -> Result<(), SubscribeError> {
        let mut reconnect_millis = 500;
        let mut reconnect_attempts: u8 = 0;
        let max_reconnect_attempts = 5;
        loop {
            let mut worker_guard = self.worker.lock().await;
            let worker: &mut Worker = &mut *worker_guard;
            match &worker.channel {
                Some(c) => {
                    // TODO consume
                    //return self.publish_with_params_impl(content, &params, &c).await;
                    return Ok(());
                },
                None => {
                    if reconnect_attempts > max_reconnect_attempts {
                        let msg = format!("reached maximum attempts ({}), but channel not open to publish",
                        reconnect_attempts);
                        error!("{}", msg);
                        return Err(SubscribeError::ChannelNotOpen(worker.id));
                    } else {
                        let sleep_time = Duration::from_millis(reconnect_millis);
                        debug!("sleep for {} seconds before try to reestablish topology ...",reconnect_millis);
                        sleep( sleep_time ).await;
                        reconnect_millis = reconnect_millis * 2;
                        reconnect_attempts += 1;
                    }
                },
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubscribeParams {

}

pub enum SubscribeError {
    Todo,
    ChannelNotOpen(u32),
    ParameterError(String),
    SubscribeError(String),
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SubscribeError::Todo => write!(f, "Todo error occurred"),
            SubscribeError::ChannelNotOpen(id) => write!(f, "Channel is not open, worker id={}", id),
            SubscribeError::ParameterError(msg) => write!(f, "Parameter error: {}", msg),
            SubscribeError::SubscribeError(msg) => write!(f, "Subscribe error: {}", msg),
        }
    }
}

