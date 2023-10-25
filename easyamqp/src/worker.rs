use amqprs::channel::Channel;

use crate::callbacks::RabbitChannelCallback;

/// This structure is put directly into the Publishers and Subscribers
pub struct Worker {
    pub id: u32,
    pub channel: Option<Channel>,
    pub callback: RabbitChannelCallback,
}