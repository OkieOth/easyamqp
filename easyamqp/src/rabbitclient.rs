//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{debug, error, info, warn};
use std::{thread, time, fmt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use amqprs::{
    callbacks::{ChannelCallback, ConnectionCallback},
    channel::Channel,
    connection::{Connection, OpenConnectionArguments},
    Ack, BasicProperties, Cancel, Close, CloseChannel, Nack, Return,
};

type Result<T> = std::result::Result<T, amqprs::error::Error>;

/// Command send to the async worker that holds the connection

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
    /// Connection object to RabbitMq server
    connection: Option<Connection>,
    // callback instance that's triggered when the connection is closed
    con_callback: RabbitConCallback,

    con_name: Option<String>,

    workers: Vec<RabbitWorkerHandle>
}


struct RabbitClientApiCont {
    /// internal Sender to inform the parallel RabbitCient worker, to execute a task
    tx_req: Sender<ClientCommand>,
    /// internal Receiver to read possible Response from the parallel RabbitClient worker
    rx_resp: Receiver<ClientCommandResponse>,
}

struct RabbitWorkerCont {
    channel: Option<Channel>,
    callback: RabbitChannelCallback,
}

pub struct RabbitWorker {
    pub id: i32,
    cont_mutex: Arc<Mutex<RabbitWorkerCont>>,
}

impl RabbitWorker {
    pub async fn get_state(&self) -> (bool, bool, bool) {
        let mut guard = self.cont_mutex.lock().await;
        let worker_cont: &mut RabbitWorkerCont =&mut *guard;
        let o = worker_cont.channel.as_ref();
        match o {
            Some(x) => {
                (true, x.is_connection_open(), x.is_open())
            },
            None => {
                (false, false, false)
            }
        }
    }
}

struct RabbitWorkerHandle {
    id: i32,
    tx_req: Sender<ClientCommand>,
    cont_mutex: Arc<Mutex<RabbitWorkerCont>>,
}


enum ClientCommand {
    /// Cancel the async worker task
    Cancel,
    /// dummy function for test purposes
    Dummy(i32),
    /// reconnect to the Broker
    Connect,
    /// request a new worker on rabbitMq ... it provides basically the connection channel
    GetWorker(i32),
    /// request the recreation of a specific channel
    GetChannel(i32),
}

impl fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientCommand::Cancel => write!(f, "Cancel"),
            ClientCommand::Dummy(i) => write!(f, "Dummy({})", i),
            ClientCommand::Connect => write!(f, "Connect"),
            ClientCommand::GetWorker(i) => write!(f, "GetWorker-{}", i),
            ClientCommand::GetChannel(i) => write!(f, "GetChannel-{}", i),
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
    /// send the created worker back to the specific RabbitClient function
    GetWorkerRespone(std::result::Result<RabbitWorker, String>),
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


/// Internal structure to implement the connection callback for the RabbitMq Connection
#[derive(Debug, Clone)]
struct RabbitConCallback {
    /// Sender to request a new connection from the RabbitMq client worker
    tx_req: Sender<ClientCommand>,
}


#[async_trait::async_trait]
impl ConnectionCallback for RabbitConCallback {
    async fn close(&mut self, _: &Connection, _: Close) -> Result<()> {
        info!("connection was closed");

        if let Err(e) = self.tx_req.send(ClientCommand::Connect).await {
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
struct RabbitChannelCallback {
    /// id of the worker
    id: i32,
    /// Sender to request a new connection from the RabbitMq client worker
    tx_req: Sender<ClientCommand>,
}

#[async_trait::async_trait]
impl ChannelCallback for RabbitChannelCallback {
    async fn close(&mut self, channel: &Channel, close: CloseChannel) -> Result<()> {
        warn!("channel was closed");
        let _ = self.tx_req.send(ClientCommand::GetChannel(self.id)).await;
        Ok(())
    }
    async fn cancel(&mut self, channel: &Channel, cancel: Cancel) -> Result<()> {
        Ok(())
    }
    async fn flow(&mut self, channel: &Channel, active: bool) -> Result<bool> {
        Ok(true)
    }
    async fn publish_ack(&mut self, channel: &Channel, ack: Ack) {}
    async fn publish_nack(&mut self, channel: &Channel, nack: Nack) {}
    async fn publish_return(
        &mut self,
        channel: &Channel,
        ret: Return,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
    }
}



/// RabbitMq client wrapper
#[derive(Clone)]
pub struct RabbitClient {
    mutex_handler: Arc<Mutex<RabbitClientCont>>,
    mutex_api: Arc<Mutex<RabbitClientApiCont>>,
}

impl RabbitClient {
    pub async fn new_with_name(
        name: String,
        con_params: Option<RabbitConParams>,
        tx_panic: Option<Sender<String>>,
        max_reconnect_attempts: u8,
    ) -> RabbitClient {
        let client = RabbitClient::new_impl(con_params, tx_panic, max_reconnect_attempts).await;
        let mut guard = client.mutex_handler.lock().await;
        let cont = &mut *guard;
        cont.con_name = Some(name);
        drop(guard);
        return client;
    }

    pub async fn new(
        con_params: Option<RabbitConParams>,
        tx_panic: Option<Sender<String>>,
        max_reconnect_attempts: u8,
    ) -> RabbitClient {
        RabbitClient::new_impl(con_params, tx_panic, max_reconnect_attempts).await
    }
    
    async fn new_impl(
        con_params: Option<RabbitConParams>,
        tx_panic: Option<Sender<String>>,
        max_reconnect_attempts: u8,
    ) -> RabbitClient {
        let (tx_req, rx_req): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(100);
        let (tx_resp, rx_resp): (Sender<ClientCommandResponse>, Receiver<ClientCommandResponse>) = mpsc::channel(100);
        let c = RabbitClientCont {
            tx_req: tx_req.clone(),
            connection: None,
            tx_panic: tx_panic,
            max_reconnect_attempts: max_reconnect_attempts,
            con_params: con_params,
            con_callback: RabbitConCallback { tx_req: tx_req.clone() },
            workers: Vec::new(),
            con_name: None,
        };
        let ac = RabbitClientApiCont {
            tx_req: tx_req.clone(),
            rx_resp: rx_resp,
        };
        let ret = RabbitClient {
            mutex_handler: Arc::new(Mutex::new(c)),
            mutex_api: Arc::new(Mutex::new(ac)),
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
                    ClientCommand::Connect => RabbitClient::handle_connect(&tx_resp, &mut rabbit_client).await,
                    ClientCommand::GetWorker(i) => RabbitClient::handle_get_worker(i, &tx_resp, &mut rabbit_client).await,
                    ClientCommand::GetChannel(i) => RabbitClient::handle_get_channel(i, &mut rabbit_client).await
                }
            }
            error!("I am leaving the management task 8-o");
        });
    }

    pub async fn dummy(&self, i: i32) -> std::result::Result<i32, String> {
        let mut guard = self.mutex_api.lock().await;
        let ac: &mut RabbitClientApiCont = &mut *guard;
        if ac.tx_req.send(ClientCommand::Dummy(i)).await.is_err() {
            return Err("error while sending dummy request".to_string());
        }
        match ac.rx_resp.recv().await {
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
        let mut guard = self.mutex_api.lock().await;
        let ac: &mut RabbitClientApiCont = &mut *guard;
        if ac.tx_req.send(ClientCommand::Cancel).await.is_err() {
            error!("error while sending cancel request");
        }
        match ac.rx_resp.recv().await {
            Some(_) => {
                debug!("receive propper response from cancel request");
            },
            None => {
                error!("didn't receive a propper response from cancel request");
            }
        } 
    }

    pub async fn connect(&mut self) -> std::result::Result<(), String>{
        let mut guard = self.mutex_api.lock().await;
        let ac: &mut RabbitClientApiCont = &mut *guard;
        if ac.tx_req.send(ClientCommand::Connect).await.is_err() {
            let msg = "error while sending cancel request";
            error!("{}", msg);
            return Err(msg.to_string());
        }
        Ok(())
    }

    pub async fn new_worker(&mut self, id: i32) -> std::result::Result<RabbitWorker, String> {
        debug!("request api lock: id={}", id);
        let mut guard = self.mutex_api.lock().await;
        debug!("got api lock: id={}", id);
        let ac: &mut RabbitClientApiCont = &mut *guard;
        if ac.tx_req.send(ClientCommand::GetWorker(id)).await.is_err() {
            return Err("error while sending getWorker request".to_string());
        }
        debug!("waiting for worker request response: id={}", id);
        match ac.rx_resp.recv().await {
            Some(resp) => {
                debug!("receive propper response from getWorker request");
                if let ClientCommandResponse::GetWorkerRespone(result) = resp {
                    return result;
                } else {
                    Err("Wrong response type for getWorker command".to_string())
                }
            },
            None => {
                debug!("didn't receive response for worker request: id={}", id);
                Err("didn't receive a propper response from getWorker request".to_string())
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

    async fn do_connect(c: &mut RabbitClientCont) -> std::result::Result<(), String> {
        let con_params: &RabbitConParams = c.con_params.as_mut().unwrap();
            let mut con_args = OpenConnectionArguments::new(
                &con_params.server,
                con_params.port,
                &con_params.user,
                &con_params.password,
            );
            if c.con_name.is_some() {
                let s = c.con_name.as_ref().unwrap().clone();
                con_args.connection_name(s.as_str());
            }
            match Connection::open(&con_args)
            .await
            {
                Ok(connection) => {
                    info!("connection established :), name={}", connection.connection_name());
                    connection
                        .register_callback(c.con_callback.clone())
                        .await
                        .unwrap();
                    info!("???: {}", connection.is_open());
                    c.connection = Some(connection.clone());
                    let tx_send_reconnect = c.tx_req.clone();
                    tokio::spawn(async move {
                        if connection.listen_network_io_failure().await {
                            error!("received network error for rabbit connection");
                            if let Err(e) = tx_send_reconnect.send(ClientCommand::Connect).await {
                                error!(
                                    "error while notify about closed connection: {}",
                                    e.to_string()
                                );
                            }
                        } else {
                            info!("no network error for rabbit connection");
                        }
                    });
                    Ok(())
                }
                Err(e) => {
                    error!("connection failure :(");
                    Err(e.to_string())
                }
            }
    }

    async fn do_panic(c: &mut RabbitClientCont, msg: &str) {
        if c.tx_panic.is_some() {
            let pannic_channel: Sender<String> = c.tx_panic.clone().unwrap();
            if let Err(se) = pannic_channel.send(msg.to_string()).await {
                error!("error while sending panic request: {}", se.to_string());
                panic!("{}", msg);
            }
        } else {
            panic!("{}", msg);
        };
    }

    async fn reset_channels_in_workers(workers: &mut Vec<RabbitWorkerHandle>) {
        for w in workers.iter() {
            let mut guard = w.cont_mutex.lock().await;
            let worker_cont: &mut RabbitWorkerCont =&mut *guard;
            worker_cont.channel = None;
        }
    }

    async fn restore_channels_in_workers(connection: &Option<Connection>, workers: &mut Vec<RabbitWorkerHandle>) {
        if connection.is_none() {
            warn!("should restore channel on None connection object");
            return;
        }
        let conn = connection.as_ref().unwrap();
        for w in workers.iter() {
            let mut guard = w.cont_mutex.lock().await;
            let worker_cont: &mut RabbitWorkerCont =&mut *guard;
            if worker_cont.channel.is_none() {
                match conn.open_channel(None).await {
                    Ok(channel) => {
                        channel
                            .register_callback(worker_cont.callback.clone())
                            .await
                            .unwrap();
                        worker_cont.channel = Some(channel);
                    },
                    Err(e) => {
                        error!("error while creating channel for worker={}: {}", w.id, e.to_string());
                    }
                }
            }
        }
    }

    async fn handle_connect(tx_resp: &Sender<ClientCommandResponse>, rabbit_client: &mut RabbitClient) {
        let mut guard = rabbit_client.mutex_handler.lock().await;
        let c: &mut RabbitClientCont = &mut *guard;
        let mut reconnect_seconds = 1;
        let mut reconnect_attempts: u8 = 0;
        if c.con_params.is_none() {
            let msg = "RabbitClient object isn't proper initialized. Missing rabbit connection params";
            error!("{}", msg);
            RabbitClient::do_panic(c, msg).await;
            return;
        }
        if c.connection.is_some() && c.connection.as_ref().unwrap().is_open() {
            info!("should connect, but connection is already open");
            return;
        } else {
            RabbitClient::reset_channels_in_workers(&mut c.workers).await;
        }

        loop {
            if let Err(e) = RabbitClient::do_connect(c).await {
                error!("error while trying to connect: {}", e.to_string());
                let sleep_time = time::Duration::from_secs(reconnect_seconds);
                info!("sleep for {} seconds before try to reconnect ...", reconnect_seconds);
                thread::sleep(sleep_time);
                reconnect_seconds = reconnect_seconds * 2;
                reconnect_attempts += 1;
                if reconnect_attempts > c.max_reconnect_attempts {
                    error!("reached maximum reconnection attempts ({}), and stop trying", reconnect_attempts);
                    let msg = "reached maximum reconnection attempts";
                    error!("{}", msg);
                    RabbitClient::do_panic(c, msg).await;
                    return;
                }
            } else {
                RabbitClient::restore_channels_in_workers(&c.connection, &mut c.workers).await;
                break;
            }
        }
    }

    async fn handle_get_channel(i: i32, rabbit_client: &mut RabbitClient) {
        debug!("should get channel for worker-{} ... request lock ...", i);
        let mut guard = rabbit_client.mutex_handler.lock().await;
        debug!("should get channel for worker-{} ... got lock", i);
        let c: &mut RabbitClientCont = &mut *guard;
        if c.connection.is_some() && c.connection.as_ref().unwrap().is_open() {
            let conn = c.connection.as_ref().unwrap();
            for w in c.workers.iter() {
                if w.id == i {
                    let mut guard = w.cont_mutex.lock().await;
                    let worker_cont: &mut RabbitWorkerCont =&mut *guard;
                    match conn.open_channel(None).await {
                        Ok(channel) => {
                            channel
                                .register_callback(worker_cont.callback.clone())
                                .await
                                .unwrap();
                            worker_cont.channel = Some(channel);
                        },
                        Err(e) => {
                            error!("error while creating channel for worker={}: {}", w.id, e.to_string());
                        }
                    }
                }
            }    
        } else {
            error!("can't proceed because connection isn't open");
        }

    }

    async fn handle_get_worker(i: i32, tx_resp: &Sender<ClientCommandResponse>, rabbit_client: &mut RabbitClient) {
        let mut guard = rabbit_client.mutex_handler.lock().await;
        let c: &mut RabbitClientCont = &mut *guard;
        let mut reconnect_seconds = 1;
        let mut reconnect_attempts: u8 = 0;

        loop {

            if c.connection.is_none() || (!c.connection.as_ref().unwrap().is_open()) {
                info!("connection isn't ready ...");
            } else {
                // try to create the channel
                let conn = c.connection.as_ref().unwrap();

                let callback = RabbitChannelCallback { 
                        tx_req: c.tx_req.clone(),
                        id: i,
                    };

                let worker_cont = RabbitWorkerCont {
                    channel: None,
                    callback: callback,
                };
                let worker = RabbitWorker {
                    id: i,
                    cont_mutex: Arc::new(Mutex::new(worker_cont)),
                };

                let worker_handle = RabbitWorkerHandle {
                    id: i,
                    tx_req: c.tx_req.clone(),
                    cont_mutex: worker.cont_mutex.clone(),
                };

                c.workers.push(worker_handle);

                match conn.open_channel(None).await {
                    Ok(channel) => {
                        {
                            let mut guard = worker.cont_mutex.lock().await;
                            let worker_cont: &mut RabbitWorkerCont =&mut *guard;
                            channel
                                .register_callback(worker_cont.callback.clone())
                                .await
                                .unwrap();
                            worker_cont.channel = Some(channel);
                        }
                        match tx_resp.send(ClientCommandResponse::GetWorkerRespone(Ok(worker))).await {
                            Ok(_) => {
                                debug!("sent to id={}: ClientCommandResponse::GetWorkerResponse", i);
                            },
                            Err(e) => {
                                error!("error while sending getChannel response to {}: {}", i,  e.to_string());
                            }
                        };
                        return;
                    },
                    Err(e) => {
                        error!("error while creating channel for i={}: {}", i, e.to_string());
                    }
                }
            }

            let sleep_time = time::Duration::from_secs(reconnect_seconds);
            info!("sleep for {} seconds before try to reconnect ...", reconnect_seconds);
            thread::sleep(sleep_time);
            reconnect_seconds = reconnect_seconds * 2;
            reconnect_attempts += 1;
            if reconnect_attempts > c.max_reconnect_attempts {
                error!("reached maximum reconnection attempts ({}), and stop trying", reconnect_attempts);
                let msg = "reached maximum reconnection attempts";
                error!("{}", msg);
                RabbitClient::do_panic(c, msg).await;
                let _ = tx_resp.send(ClientCommandResponse::GetWorkerRespone(Err(msg.to_string()))).await;
                return;
            }
        }
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