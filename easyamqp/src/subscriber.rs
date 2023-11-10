use std::sync::Arc;
use log::{debug, error, info, warn};

use tokio::sync::Mutex;
use tokio::time::{Duration, sleep, timeout};
use tokio::task;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use amqprs::consumer::AsyncConsumer;
use amqprs::channel::{BasicAckArguments, BasicConsumeArguments, Channel};
use amqprs::{Deliver, BasicProperties};
use crate::topology::{QueueDefinition, QueueBindingDefinition};
use crate::callbacks::RabbitChannelCallback;
use crate::{worker::Worker, rabbitclient::ClientCommand};


pub struct Subscriber {
    pub worker: Arc<Mutex<Worker>>,
    //sub_impl: Arc<Mutex<SubscriberImpl>>,
    pub params: SubscribeParams,
    pub tx_content: Arc<Mutex<Sender<SubscriptionContent>>>,
    pub rx_content: Receiver<SubscriptionContent>,
    pub tx_response: Sender<SubscriptionResponse>,
    pub rx_response: Arc<Mutex<Receiver<SubscriptionResponse>>>,
    pub rx_inform_about_new_channel: Arc<Mutex<Receiver<u32>>>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let w = self.worker.clone();
        task::spawn(async move {
            let mut worker_guard = w.lock().await;
            let worker: &mut Worker = &mut *worker_guard;
            debug!("worker (id={}) will be deleted", worker.id);
            if let Err(e) = worker.callback.tx_req.send(ClientCommand::RemoveSubscriber(worker.id)).await {
                error!("error while sending request to delete worker (id={}): {}",
                worker.id, e.to_string());
            }
        });
    }
}


#[derive(Debug, Clone, Default)]
pub struct SubscriptionContent {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub correlation_id: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub data: Vec<u8>,
    pub delivery_tag: u64,
}

impl SubscriptionContent {
    fn new(
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>
    ) -> SubscriptionContent {
        let mut ret = SubscriptionContent{
            data: content,
            delivery_tag: deliver.delivery_tag(),
            ..Default::default()
        };

        if basic_properties.content_type().is_some() {
            ret.content_type = Some(basic_properties.content_type().unwrap().to_string());
        }
        if basic_properties.content_encoding().is_some() {
            ret.content_encoding = Some(basic_properties.content_encoding().unwrap().to_string());
        }
        if basic_properties.correlation_id().is_some() {
            ret.correlation_id = Some(basic_properties.correlation_id().unwrap().to_string());
        }
        if basic_properties.message_id().is_some() {
            ret.message_id = Some(basic_properties.message_id().unwrap().to_string());
        }
        if basic_properties.timestamp().is_some() {
            ret.timestamp = Some(basic_properties.timestamp().unwrap());
        }
        if basic_properties.message_type().is_some() {
            ret.message_type = Some(basic_properties.message_type().unwrap().to_string());
        }
        if basic_properties.user_id().is_some() {
            ret.user_id = Some(basic_properties.user_id().unwrap().to_string());
        }
        if basic_properties.app_id().is_some() {
            ret.app_id = Some(basic_properties.app_id().unwrap().to_string());
        }

        ret

    }
}

pub struct SubscriptionResponse {
    pub delivery_tag: u64,
    pub ack: bool,
}

struct SubscriberImpl {
    pub tx_content: Arc<Mutex<Sender<SubscriptionContent>>>,
    pub rx_response: Arc<Mutex<Receiver<SubscriptionResponse>>>,
    pub auto_ack: bool,
    pub queue_name: String,
    pub consumer_tag: String,
    pub exclusive: bool,
}

impl SubscriberImpl {
    async fn wait_for_subscription_response_and_ack(&mut self, delivery_tag: u64, channel: &Channel) {
        const TIMEOUT_SECS: u64 = 30;
        let mut rx_response_guard = self.rx_response.lock().await;
        let rx_response: &mut Receiver<SubscriptionResponse> = &mut *rx_response_guard;
        match timeout(Duration::from_secs(TIMEOUT_SECS), rx_response.recv()).await {
            Ok(timeout_result) => {
                match timeout_result {
                    Some(resp) => {
                        if resp.ack {
                            let args = BasicAckArguments::new(delivery_tag, false);
                            channel.basic_ack(args).await.unwrap();
                        }
                    },
                    None => {
                        error!("didn't receive proper subscription response");
                        // left the message unacknoledged
                    },
                }
            },
            Err(_) => {
                // timeout
                error!("didn't receive subscription response in timeout ({} s)", TIMEOUT_SECS);
            },
        }
    }
}


#[async_trait::async_trait]
impl AsyncConsumer for SubscriberImpl {
    async fn consume(
        &mut self, // use `&mut self` to make trait object to be `Sync`
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        debug!("consume is called");
        let delivery_tag = deliver.delivery_tag();
        let sc = SubscriptionContent::new(
            deliver,
            basic_properties,
            content
            );
        {
            let mut tx_content_guard = self.tx_content.lock().await;
            info!("receive content for channel={}", channel.channel_id().to_string());
            let tx_content: &mut Sender<SubscriptionContent> = &mut *tx_content_guard;

            if let Err(e) = tx_content.send(sc).await {
                error!("error while sending subscription content: {}", e);
                // left the message unacknoledged
                return;
            }
        }
        if ! self.auto_ack {
            info!("wait for ack delivery_tag={} ...", delivery_tag);
            self.wait_for_subscription_response_and_ack(delivery_tag, &channel).await;
        }
    }
}

impl Subscriber {
    pub async fn new(id: u32, tx_req: Sender<ClientCommand>, params: SubscribeParams) -> Result<Subscriber, String> {
        let callback = RabbitChannelCallback {
            tx_req,
            id,
        };
        let (tx_inform_about_new_channel, rx_inform_about_new_channel): (Sender<u32>, Receiver<u32>) = channel(1);
        let worker_cont = Worker {
            id,
            channel: None,
            callback,
            tx_inform_about_new_channel: Some(tx_inform_about_new_channel),
        };
        let (tx_content, rx_content): (Sender<SubscriptionContent>, Receiver<SubscriptionContent>) = channel(1);
        let (tx_response, rx_response): (Sender<SubscriptionResponse>, Receiver<SubscriptionResponse>) = channel(1);

        let ret = Subscriber {
            worker: Arc::new(Mutex::new(worker_cont)),
            params: params.clone(),
            tx_content: Arc::new(Mutex::new(tx_content)),
            rx_content: rx_content,
            tx_response: tx_response,
            rx_response: Arc::new(Mutex::new(rx_response)),
            rx_inform_about_new_channel: Arc::new(Mutex::new(rx_inform_about_new_channel)),
        };
        Ok(ret)
    }

    pub async fn subscribe_with_auto_ack(&mut self) -> Result<&mut Receiver<SubscriptionContent>, SubscribeError> {
        match self.subscribe().await {
            Ok((rx, _)) => {
                Ok(rx)
            },
            Err(e) => {
                Err(e)
            }
        }
    }

    pub async fn start_new_channel_listener(&self, tx_req: Sender<ClientCommand>) {
        let rx_inform_about_new_channel = self.rx_inform_about_new_channel.clone();
        let w = self.worker.clone();

        let tx_content = self.tx_content.clone();
        let rx_response = self.rx_response.clone();
        let auto_ack = self.params.auto_ack;
        let queue_name = self.params.queue_name.clone();
        let consumer_tag = self.params.consumer_tag.clone();
        let exclusive = self.params.exclusive;

        task::spawn(async move {
            loop {
                let rx: &mut Receiver<u32>;
                let mut guard = rx_inform_about_new_channel.lock().await;
                rx = &mut *guard;
                let _ = rx.recv().await;
                let mut worker_guard = w.lock().await;
                let worker: &mut Worker = &mut *worker_guard;
                match &worker.channel {
                    Some(c) => {
                        let sub_impl = SubscriberImpl {
                            tx_content: tx_content.clone(),
                            rx_response: rx_response.clone(),
                            auto_ack: auto_ack,
                            queue_name: queue_name.clone(),
                            consumer_tag: consumer_tag.clone(),
                            exclusive: exclusive,
                        };
                        let args = BasicConsumeArguments::new(&sub_impl.queue_name.clone(), &sub_impl.consumer_tag)
                        .manual_ack(!sub_impl.auto_ack)
                        .exclusive(sub_impl.exclusive)
                        .finish();

                        match c.basic_consume(sub_impl, args).await {
                            Ok(_) => {
                                debug!("subscription re-established");
                            },
                            Err(err) => {
                                let msg = format!("error while re-create subscription (worker: {}): {}", worker.id, err.to_string());
                                error!("{}", &msg);
                                let _ = tx_req.send(ClientCommand::Panic(msg)).await;
                            },
                        }
                    },
                    None => {
                        let msg = format!("error while re-create subscription (worker: {}): channel is None", worker.id);
                        error!("{}", &msg);
                        let _ = tx_req.send(ClientCommand::Panic(msg)).await;
                    },
                }
            }
        });
    }

    pub async fn subscribe(&mut self) -> Result<(&mut Receiver<SubscriptionContent>, &Sender<SubscriptionResponse>), SubscribeError> {
        let mut reconnect_millis = 500;
        let mut reconnect_attempts: u8 = 0;
        let max_reconnect_attempts = 5;
        loop {
            let mut worker_guard = self.worker.lock().await;
            let worker: &mut Worker = &mut *worker_guard;
            match &worker.channel {
                Some(c) => {
                    info!("subscribe for channel={}", c.channel_id().to_string());

                    let sub_impl = SubscriberImpl {
                        tx_content: self.tx_content.clone(),
                        rx_response: self.rx_response.clone(),
                        auto_ack: self.params.auto_ack,
                        queue_name: self.params.queue_name.clone(),
                        consumer_tag: self.params.consumer_tag.clone(),
                        exclusive: self.params.exclusive,
                    };

                    let args = BasicConsumeArguments::new(&sub_impl.queue_name.clone(), &sub_impl.consumer_tag)
                    .manual_ack(!sub_impl.auto_ack)
                    .exclusive(sub_impl.exclusive)
                    .finish();

                    match c.basic_consume(sub_impl, args).await {
                        Ok(_) => {
                            self.start_new_channel_listener(worker.callback.tx_req.clone()).await;
                            return Ok((&mut self.rx_content, &self.tx_response));
                        },
                        Err(err) => {
                            return Err(SubscribeError::SubscribeError(err.to_string()));
                        },
                    }
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
    pub auto_ack: bool,
    pub queue_name: String,
    pub exclusive: bool,
    pub consumer_tag: String,
}

impl SubscribeParams {
    pub fn builder(queue_name: &str, consumer_tag: &str) -> SubscribeParamsBuilder {
        SubscribeParamsBuilder::new(queue_name, consumer_tag)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubscribeParamsBuilder {
    pub auto_ack: Option<bool>,
    pub queue_name: String,
    pub exclusive: Option<bool>,
    pub consumer_tag: String,
}

impl SubscribeParamsBuilder {
    pub fn new(queue_name: &str, consumer_tag: &str) -> SubscribeParamsBuilder {
        let mut r = SubscribeParamsBuilder::default();
        r.queue_name = queue_name.to_string();
        r.consumer_tag = consumer_tag.to_string();
        r
    }
    pub fn auto_ack(mut self,v: bool) -> SubscribeParamsBuilder {
        self.auto_ack = Some(v);
        self
    }
    pub fn exclusive(mut self,v: bool) -> SubscribeParamsBuilder {
        self.exclusive = Some(v);
        self
    }
 
    pub fn build(&self) -> SubscribeParams {
        let auto_ack = match self.auto_ack {
            Some(b) => b,
            None => false,
        };
        let exclusive = match self.exclusive {
            Some(b) => b,
            None => false,
        };

        SubscribeParams {
            auto_ack,
            queue_name: self.queue_name.clone(),
            exclusive,
            consumer_tag: self.consumer_tag.clone(),
        }
    }
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

