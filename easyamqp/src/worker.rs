use amqprs::channel::Channel;
use tokio::sync::mpsc::Sender;

use crate::callbacks::RabbitChannelCallback;

/// This structure is put directly into the Publishers and Subscribers
pub struct Worker {
    pub id: u32,
    pub channel: Option<Channel>,
    pub callback: RabbitChannelCallback,
    pub tx_inform_about_new_channel: Option<Sender<u32>>
}

