use crate::rabbitclient::{ClientCommand, RabbitConParams};
use log::{debug, error, info, warn};
use std::result::Result;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct ClientImpl {
    con_params: RabbitConParams,
}

impl ClientImpl {
    pub async fn new(con_params: RabbitConParams, mut rx_command: Receiver<ClientCommand>) -> Self {
        let ret = ClientImpl { con_params };
        ret.start_cmd_receiver_task(rx_command);
        return ret;
    }

    fn start_cmd_receiver_task(&self, mut rx_command: Receiver<ClientCommand>) {
        tokio::spawn(async move {
            while let Some(cc) = rx_command.recv().await {
                debug!("receive client command: {}", cc);
                match cc {
                    ClientCommand::Dummy(tx) => {
                        ClientImpl::handle_dummy(tx).await;
                    }
                    ClientCommand::Connect(tx) => {}
                    ClientCommand::Close(tx) => {}
                }
            }
            error!("I am leaving the management task 8-o");
        });
    }

    async fn handle_dummy(tx: Sender<Result<String, ()>>) {
        if let Err(e) = tx.send(Ok("hello from dummy".to_string())).await {
            error!("could not send dummy response: {}", e.to_string());
        }
    }
}
