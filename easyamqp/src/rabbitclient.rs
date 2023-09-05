use log::{debug, error, info};
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;


/// Command send to the async worker that holds the connection
enum ClientCommand {
    Cancel,
    Dummy(i32),
}

impl fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientCommand::Cancel => write!(f, "Cancel"),
            ClientCommand::Dummy(i) => write!(f, "Dummy({})", i),
        }
    }
}

/// Response of the command that was send to the async worker
enum ClientCommandResponse {
    CancelResponse(std::result::Result<(), String>),
    DummyResponse(std::result::Result<i32, String>),
}


pub struct RabbitClient {
    tx_req: Sender<ClientCommand>,
    mutex_resp: Arc<Mutex<Receiver<ClientCommandResponse>>>,
}

impl RabbitClient {
    pub async fn new() -> RabbitClient {
        let (tx_req, rx_req): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(1);
        let (tx_resp, rx_resp): (Sender<ClientCommandResponse>, Receiver<ClientCommandResponse>) = mpsc::channel(1);
        RabbitClient::start_management_task(rx_req,tx_resp).await;
        RabbitClient {
            tx_req: tx_req,
            mutex_resp: Arc::new(Mutex::new(rx_resp)),
        }
    }

    async fn start_management_task(mut rx_req: Receiver<ClientCommand>, tx_resp: Sender<ClientCommandResponse>) {
        tokio::spawn(async move {
            while let Some(cc) = rx_req.recv().await {
                debug!("receive client command: {}", cc);
                match cc {
                    ClientCommand::Dummy(i) => RabbitClient::handle_dummy_cmd(i, &tx_resp).await,
                    ClientCommand::Cancel => {
                        RabbitClient::handle_cancel_cmd(&tx_resp).await;
                        break;
                    },
                }
            }
        });
    }

    async fn dummy(&self, i: i32) -> std::result::Result<i32, String> {
        let mut rx_resp = self.mutex_resp.lock().await;
        if self.tx_req.send(ClientCommand::Dummy(i)).await.is_err() {
            return Err("error while sending dummy request".to_string());
        }
        match rx_resp.recv().await {
            Some(resp) => {
                debug!("receive propper response from dummy request");
                if let ClientCommandResponse::DummyResponse(value) = resp {
                    return value;
                } else {
                    Err("Wrong response type for dummy command".to_string())
                }
            },
            None => {
                Err("didn't receive a propper response from dummy request".to_string())
            }
        }
    }

    async fn close(&mut self) {
        let mut rx_resp = self.mutex_resp.lock().await;
        if self.tx_req.send(ClientCommand::Cancel).await.is_err() {
            error!("error while sending cancel request");
        }
        match rx_resp.recv().await {
            Some(resp) => {
                debug!("receive propper response from cancel request");
            },
            None => {
                error!("didn't receive a propper response from cancel request");
            }
        } 
    }


    async fn handle_dummy_cmd(i: i32, tx_resp: &Sender<ClientCommandResponse>) {
        info!("received dummy: {}", i);
        match tx_resp.send(ClientCommandResponse::DummyResponse(Ok(i))).await {
            Ok(_) => {
                debug!("sent: ClientCommandResponse::DummyResponse");
            },
            Err(e) => {
                error!("error while sending command response: {}", e.to_string());
            }
        }
    }

    async fn handle_cancel_cmd(tx_resp: &Sender<ClientCommandResponse>) {
        match tx_resp.send(ClientCommandResponse::CancelResponse(Ok(()))).await {
            Ok(_) => {
                debug!("sent: ClientCommandResponse::DummyResponse");
            },
            Err(e) => {
                error!("error while sending command response: {}", e.to_string());
            }
        }
    }


}
