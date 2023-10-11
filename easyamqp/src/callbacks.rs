use log::{debug, error, info};
use tokio::sync::mpsc::Sender;
use amqprs::{
    callbacks::ConnectionCallback,
    connection::Connection,
    Close,
};

use crate::client_impl::ClientCommand;


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
