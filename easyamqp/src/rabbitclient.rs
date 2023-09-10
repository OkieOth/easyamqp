//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{debug, error, info};
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use amqprs::{
    callbacks::{ChannelCallback, ConnectionCallback},
    channel::Channel,
    connection::{Connection, OpenConnectionArguments},
    Close,
};

type Result<T> = std::result::Result<T, amqprs::error::Error>;

/// Command send to the async worker that holds the connection
enum ClientCommand {
    /// Cancel the async worker task
    Cancel,
    /// dummy function for test purposes
    Dummy(i32),
    /// reconnect to the Broker
    ReConnect,
}

impl fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientCommand::Cancel => write!(f, "Cancel"),
            ClientCommand::Dummy(i) => write!(f, "Dummy({})", i),
            ClientCommand::ReConnect => write!(f, "Reconnect"),
        }
    }
}

/// Response of the command that was send to the async worker. Every element of
/// this enum ia a counterpart to one ClientCommand value
enum ClientCommandResponse {
    /// sent in response of a cancel request
    CancelResponse(std::result::Result<(), String>),
    /// sent in response of a dummy function call
    DummyResponse(std::result::Result<i32, String>),
}

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

/// Internal content of the RabbitClient
struct RabbitClientCont {
    /// connection parameters
    con_params: Option<RabbitConParams>,
    /// a sender to inform the host of the client that the client wants to panic
    tx_panic: Option<Sender<String>>,
    /// maximal number of reconnects in case of connection issues
    max_reconnect_attempts: u8,
    /// internal Sender to inform the parallel RabbitCient worker, to execute a task
    tx_req: Sender<ClientCommand>,
    /// internal Receiver to read possible Response from the parallel RabbitClient worker
    rx_resp: Receiver<ClientCommandResponse>,
    /// Connection object to RabbitMq server
    connection: Option<Connection>,
}

/// Internal structure to implement the connection callback for the RabbitMq Connection
struct RabbitConCallback {
    /// Sender to request a new connection from the RabbitMq client worker
    tx_req: Sender<ClientCommand>,
}


#[async_trait::async_trait]
impl ConnectionCallback for RabbitConCallback {
    async fn close(&mut self, _: &Connection, _: Close) -> Result<()> {
        info!("connection was closed");

        if let Err(e) = self.tx_req.send(ClientCommand::ReConnect).await {
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


/// RabbitMq client wrapper
#[derive(Clone)]
pub struct RabbitClient {
    mutex: Arc<Mutex<RabbitClientCont>>,
}

impl RabbitClient {
    pub async fn new(
        con_params: Option<RabbitConParams>,
        tx_panic: Option<Sender<String>>,
        max_reconnect_attempts: u8,
    ) -> RabbitClient {
        let (tx_req, rx_req): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(1);
        let (tx_resp, rx_resp): (Sender<ClientCommandResponse>, Receiver<ClientCommandResponse>) = mpsc::channel(1);
        let c = RabbitClientCont {
            tx_req: tx_req,
            rx_resp: rx_resp,
            connection: None,
            tx_panic: tx_panic,
            max_reconnect_attempts: max_reconnect_attempts,
            con_params: con_params,
        };
        let ret = RabbitClient {
            mutex: Arc::new(Mutex::new(c)),
        };
        RabbitClient::start_management_task(rx_req,tx_resp, ret.clone());
        return ret;
    }

    fn start_management_task(mut rx_req: Receiver<ClientCommand>, tx_resp: Sender<ClientCommandResponse>,mut rabbit_client: RabbitClient) {
        tokio::spawn(async move {
            while let Some(cc) = rx_req.recv().await {
                debug!("receive client command: {}", cc);
                match cc {
                    ClientCommand::Dummy(i) => RabbitClient::handle_dummy_cmd(i, &tx_resp).await,
                    ClientCommand::Cancel => {
                        RabbitClient::handle_cancel_cmd(&tx_resp).await;
                        break;
                    },
                    ClientCommand::ReConnect => RabbitClient::handle_connect(&tx_resp, &mut rabbit_client).await,
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

    async fn handle_connect(tx_resp: &Sender<ClientCommandResponse>, rabbit_client: &mut RabbitClient) {
        let mut guard = rabbit_client.mutex.lock().await;

    }
}

#[cfg(test)]
mod tests {
    use crate::rabbitclient;
    use std::sync::mpsc::{Sender, Receiver};
    use std::sync::mpsc;
    use std::time;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn dummy() {
        let mut client = rabbitclient::RabbitClient::new(None, None, 0).await;
        let dummy_input = 13;
        let dummy_result = client.dummy(dummy_input).await.unwrap();

        assert_eq!(dummy_input, dummy_result);
        client.close().await;

    }

    async fn task(input: i32, client: rabbitclient::RabbitClient, tx_err: Sender<(i32,i32)>) {
        for _i in 0..1000 {
            let output = client.dummy(input).await.unwrap();
            if input != output {
                tx_err.send((input, output)).unwrap();
            };
            // uncomment to test that an issue is detected by the test
            // if input == 15 {
            //     tx_err.send((input, output+1)).unwrap();
            //     break;
            // }
        }
    }
    
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn multi_thread_dummy() {
        let (tx_err,rx_err): (Sender<(i32, i32)>, Receiver<(i32, i32)>) = mpsc::channel();
        let mut client: rabbitclient::RabbitClient = rabbitclient::RabbitClient::new(None, None, 0).await;

        let task1_handle = tokio::spawn(task(14, client.clone(),tx_err.clone()));
        let task2_handle = tokio::spawn(task(28, client.clone(),tx_err.clone()));
        let task3_handle = tokio::spawn(task(13, client.clone(),tx_err.clone()));
        let task4_handle = tokio::spawn(task(280, client.clone(),tx_err.clone()));
        let task5_handle = tokio::spawn(task(1, client.clone(),tx_err.clone()));
        let task6_handle = tokio::spawn(task(8, client.clone(),tx_err.clone()));
        let task7_handle = tokio::spawn(task(114, client.clone(),tx_err.clone()));
        let task8_handle = tokio::spawn(task(21, client.clone(),tx_err.clone()));
        let task9_handle = tokio::spawn(task(99, client.clone(),tx_err.clone()));
        let task10_handle = tokio::spawn(task(78, client.clone(),tx_err.clone()));
        let task11_handle = tokio::spawn(task(141, client.clone(),tx_err.clone()));
        let task12_handle = tokio::spawn(task(282, client.clone(),tx_err.clone()));
        let task13_handle = tokio::spawn(task(133, client.clone(),tx_err.clone()));
        let task14_handle = tokio::spawn(task(2804, client.clone(),tx_err.clone()));
        let task15_handle = tokio::spawn(task(15, client.clone(),tx_err.clone()));
        let task16_handle = tokio::spawn(task(86, client.clone(),tx_err.clone()));
        let task17_handle = tokio::spawn(task(1147, client.clone(),tx_err.clone()));
        let task18_handle = tokio::spawn(task(218, client.clone(),tx_err.clone()));
        let task19_handle = tokio::spawn(task(999, client.clone(),tx_err.clone()));
        let task20_handle = tokio::spawn(task(7810, client.clone(),tx_err.clone()));



        let _ = tokio::join!(task1_handle, task2_handle,
            task3_handle, task4_handle, task5_handle,
            task6_handle, task7_handle, task8_handle, task9_handle, task10_handle,
            task11_handle, task12_handle,
            task13_handle, task14_handle, task15_handle,
            task16_handle, task17_handle, task18_handle, task19_handle, task20_handle);


        let timeout = time::Duration::from_secs(20);
        let r = rx_err.recv_timeout(timeout);
        if ! r.is_err() {
            let (i, o) = r.unwrap();
            assert_eq!(i, o);
        }

        client.close().await;

    }
}