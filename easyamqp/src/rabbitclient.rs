//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{debug, error, info, warn};
use std::result::Result;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

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
    Connect(Sender<Result<(), String>>),
    Close(Sender<Result<(), String>>),
    Dummy(Sender<Result<String, ()>>),
}

impl std::fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientCommand::Dummy(_) => write!(f, "Dummy"),
            ClientCommand::Connect(_) => write!(f, "Connect"),
            ClientCommand::Close(_) => write!(f, "Close"),
        }
    }
}

pub struct RabbitClient {
    tx_command: Sender<ClientCommand>,
    client_impl: ClientImpl,
}

impl RabbitClient {
    pub async fn new(con_params: RabbitConParams) -> Self {
        let (tx_command, mut rx_command): (Sender<ClientCommand>, Receiver<ClientCommand>) =
            mpsc::channel(100);
        let client_impl = ClientImpl::new(con_params, rx_command).await;
        RabbitClient {
            tx_command,
            client_impl,
        }
    }

    pub async fn connect(&self) -> Result<(), String> {
        let (tx_command_response, mut rx_command_response): (
            Sender<Result<(), String>>,
            Receiver<Result<(), String>>,
        ) = mpsc::channel(1);
        let cmd = ClientCommand::Connect(tx_command_response);
        if let Err(e) = self.tx_command.send(cmd).await {
            error!("error while send connection request: {}", e.to_string());
            return Err(e.to_string());
        }
        match rx_command_response.recv().await {
            Some(e) => match e {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    error!("error while connect: {}", e);
                    return Err(e);
                }
            },
            None => {
                let msg = "didn't receive proper connect response".to_string();
                warn!("{}", &msg);
                return Err(msg);
            }
        }
    }

    pub async fn close(&self) {
        let (tx_command_response, mut rx_command_response): (
            Sender<Result<(), String>>,
            Receiver<Result<(), String>>,
        ) = mpsc::channel(1);
        // TODO
    }

    pub async fn dummy(&self) -> Result<String, ()> {
        let (tx_command_response, mut rx_command_response): (
            Sender<Result<String, ()>>,
            Receiver<Result<String, ()>>,
        ) = mpsc::channel(1);
        let cmd = ClientCommand::Dummy(tx_command_response);
        if let Err(e) = self.tx_command.send(cmd).await {
            error!("error while send dummy request: {}", e.to_string());
            return Err(());
        }
        match rx_command_response.recv().await {
            Some(e) => match e {
                Ok(s) => {
                    return Ok(s);
                }
                Err(_) => {
                    let msg = "error while dummy call".to_string();
                    error!("{}", msg);
                    return Err(());
                }
            },
            None => {
                let msg = "didn't receive proper connect response".to_string();
                warn!("{}", &msg);
                return Err(());
            }
        }
    }
}
