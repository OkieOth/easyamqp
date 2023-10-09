//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use crate::client_impl::ClientImpl;

/// Container for the connection parameters for the broker connection
#[derive(Debug, Clone, Default)]
pub struct RabbitConParams {
    /// Server name or IP address to connect to
    pub server: String,
    /// Port of the RabbitMq server
    pub port: u16,
    /// User used for authentication
    pub user: String,
    /// Password used for authentication
    pub password: String,
}

pub enum ClientCommand {
    Init(RabbitConParams),
}

pub enum ClientCommandResponse {}

pub struct RabbitClient {
    con_params: RabbitConParams,
    rx_command_response: Receiver<ClientCommandResponse>,
    tx_command: Sender<ClientCommand>,
    client_impl: ClientImpl,
}

impl RabbitClient {
    pub fn new(con_params: RabbitConParams) -> Self {
        let (tx_command, rx_command): (Sender<ClientCommand>, Receiver<ClientCommand>) =
            mpsc::channel(100);
        let (tx_command_response, rx_command_response): (
            Sender<ClientCommandResponse>,
            Receiver<ClientCommandResponse>,
        ) = mpsc::channel(100);
        let client_impl = ClientImpl {
            con_params: con_params.clone(),
            rx_command,
            tx_command_response,
        };
        RabbitClient {
            con_params,
            rx_command_response,
            tx_command,
            client_impl,
        }
    }

    pub async fn connect(&self) -> std::result::Result<(), String> {
        Ok(())
    }

    pub async fn close(&self) {
        // TODO
    }

    pub async fn dummy(&self) {
        println!("rabbitclient.dummy is called :)");
    }
}
