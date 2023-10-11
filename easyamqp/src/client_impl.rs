use crate::rabbitclient::RabbitConParams;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::result::Result;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use amqprs::{
    callbacks::{ChannelCallback, ConnectionCallback},
    channel::{Channel, ExchangeDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
    Ack, BasicProperties, Cancel, Close, CloseChannel, Nack, Return,
};

use crate::callbacks::RabbitConCallback;

pub enum ClientCommand {
    Connect,
}

impl std::fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientCommand::Connect => write!(f, "Connect"),
        }
    }
}


pub struct ClientImpl {
    con_params: RabbitConParams,
    tx_cmd: Sender<ClientCommand>,
    con_callback: RabbitConCallback,
    connection: Arc<Option<Connection>>,
}

impl ClientImpl {
    pub async fn new(con_params: RabbitConParams) -> Self {
        let (tx_cmd, rx_cmd): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(100);
        let con_callback = RabbitConCallback {
            tx_cmd: tx_cmd.clone(),
        };

        let ret = ClientImpl { 
            con_params, 
            tx_cmd, 
            con_callback, 
            connection: Arc::new(None), };
        ret.start_cmd_receiver_task(rx_cmd);
        return ret;
    }

    fn start_cmd_receiver_task(&self, mut rx_command: Receiver<ClientCommand>) {
        let con_params = self.con_params.clone();
        let con_callback = self.con_callback.clone();
        let con_wrapper = self.connection.clone();
        tokio::spawn(async move {
            while let Some(cc) = rx_command.recv().await {
                debug!("receive client command: {}", cc);
                match cc {
                    ClientCommand::Connect => {
                        match ClientImpl::connect(&con_params,con_callback.clone()).await {
                            Ok(connection) => {
                                // TODO 
                            },
                            Err(s) => {
                                // TODO
                            }
                        }
                    }
                }
            }
            error!("I am leaving the management task 8-o");
        });
    }

    pub async fn connect(con_params: &RabbitConParams, con_callback: RabbitConCallback) -> Result<Connection, String> {
        debug!("do connect ...");
        let mut con_args = OpenConnectionArguments::new(
            &con_params.server,
            con_params.port,
            &con_params.user,
            &con_params.password,
        );
        if con_params.con_name.is_some() {
            let s = con_params.con_name.as_ref().unwrap().clone();
            con_args.connection_name(s.as_str());
        }
        match Connection::open(&con_args).await {
            Ok(connection) => {
                info!(
                    "connection established :), name={}",
                    connection.connection_name()
                );
                connection
                    .register_callback(con_callback)
                    .await
                    .unwrap();
                // info!("???: {}", connection.is_open());
                // c.connection = Some(connection.clone());
                // let tx_send_reconnect = c.tx_req.clone();
                // tokio::spawn(async move {
                //     if connection.listen_network_io_failure().await {
                //         error!("received network error for rabbit connection");
                //         if let Err(e) = tx_send_reconnect.send(ClientCommand::Connect).await {
                //             error!(
                //                 "error while notify about closed connection: {}",
                //                 e.to_string()
                //             );
                //         }
                //     } else {
                //         info!("no network error for rabbit connection");
                //     }
                // });
                Ok(connection)
            }
            Err(e) => {
                error!("connection failure :(");
                Err(e.to_string())
            }
        }
    }

    pub async fn dummy(&self, id: u32) -> String {
        return format!("hello from dummy to id={}", id);
    }
}
