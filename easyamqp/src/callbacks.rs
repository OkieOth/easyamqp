use log::{debug, error, info, warn};
use tokio::sync::mpsc::Sender;
use amqprs::{
    callbacks::{ConnectionCallback, ChannelCallback},
    connection::Connection,
    channel::Channel,
    Ack, BasicProperties, Cancel, Close, CloseChannel, Nack, Return,
};

use crate::rabbitclient::ClientCommand;


type Result<T> = std::result::Result<T, amqprs::error::Error>;

#[derive(Debug, Clone)]
pub struct RabbitConCallback {
    /// Sender to request a new connection from the RabbitMq client worker
    pub tx_cmd: Sender<ClientCommand>,
}

#[async_trait::async_trait]
impl ConnectionCallback for RabbitConCallback {
    async fn close(&mut self, _: &Connection, _: Close) -> Result<()> {
        info!("connection was closed");
        if let Err(e) = self.tx_cmd.send(ClientCommand::Connect).await {
            error!(
                "error while notify about closed connection: {}",
                e.to_string()
            )
        }
        Ok(())
    }

    async fn blocked(&mut self, _: &Connection, _: String) {
        debug!("connection is blocked")
    }
    async fn unblocked(&mut self, _: &Connection) {
        debug!("connection is unblocked")
    }
}

#[derive(Debug, Clone)]
pub struct RabbitChannelCallback {
    /// id of the worker
    pub id: u32,
    /// Sender to request a new connection from the RabbitMq client worker
    pub tx_req: Sender<ClientCommand>,
}

#[async_trait::async_trait]
impl ChannelCallback for RabbitChannelCallback {
    async fn close(&mut self, _channel: &Channel, _close: CloseChannel) -> Result<()> {
        warn!("channel was closed");
        let _ = self.tx_req.send(ClientCommand::GetChannel(self.id)).await;
        Ok(())
    }
    async fn cancel(&mut self, _channel: &Channel, _cancel: Cancel) -> Result<()> {
        Ok(())
    }
    async fn flow(&mut self, _channel: &Channel, _active: bool) -> Result<bool> {
        Ok(true)
    }
    async fn publish_ack(&mut self, _channel: &Channel, _ack: Ack) {}
    async fn publish_nack(&mut self, _channel: &Channel, _nack: Nack) {}
    async fn publish_return(
        &mut self,
        _channel: &Channel,
        _ret: Return,
        _basic_properties: BasicProperties,
        _content: Vec<u8>,
    ) {
    }
}
