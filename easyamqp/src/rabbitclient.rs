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


struct RabbitClientCont {
    tx_req: Sender<ClientCommand>,
    rx_resp: Receiver<ClientCommandResponse>,
}

#[derive(Clone)]
pub struct RabbitClient {
    mutex: Arc<Mutex<RabbitClientCont>>,
}

impl RabbitClient {
    pub async fn new() -> RabbitClient {
        let (tx_req, rx_req): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(1);
        let (tx_resp, rx_resp): (Sender<ClientCommandResponse>, Receiver<ClientCommandResponse>) = mpsc::channel(1);
        RabbitClient::start_management_task(rx_req,tx_resp);
        let c = RabbitClientCont {
            tx_req: tx_req,
            rx_resp: rx_resp,
        };
        RabbitClient {
            mutex: Arc::new(Mutex::new(c)),
        }
    }

    fn start_management_task(mut rx_req: Receiver<ClientCommand>, tx_resp: Sender<ClientCommandResponse>) {
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

    pub async fn dummy(&self, i: i32) -> std::result::Result<i32, String> {
        let mut guard = self.mutex.lock().await;
        let c: &mut RabbitClientCont = &mut *guard;
        if c.tx_req.send(ClientCommand::Dummy(i)).await.is_err() {
            return Err("error while sending dummy request".to_string());
        }
        match c.rx_resp.recv().await {
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

    pub async fn close(&mut self) {
        let mut guard = self.mutex.lock().await;
        let c: &mut RabbitClientCont = &mut *guard;
        if c.tx_req.send(ClientCommand::Cancel).await.is_err() {
            error!("error while sending cancel request");
        }
        match c.rx_resp.recv().await {
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

#[cfg(test)]
mod tests {
    use crate::rabbitclient;



    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn first() {
        let mut client = rabbitclient::RabbitClient::new().await;
        let dummy_input = 13;
        let dummy_result = client.dummy(dummy_input).await.unwrap();

        assert_eq!(dummy_input, dummy_result);
        client.close().await;

    }
}