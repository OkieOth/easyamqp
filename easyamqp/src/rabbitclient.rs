//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time;
use std::thread;
use std::result::Result;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};

use amqprs::{
    callbacks::{ChannelCallback, ConnectionCallback},
    channel::{Channel, ExchangeDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
    Ack, BasicProperties, Cancel, Close, CloseChannel, Nack, Return,
};


use crate::publisher::Publisher;
use crate::subscriber::Subscriber;
use crate::topology::Topology;
use crate::topology::{ExchangeDefinition, ExchangeType, QueueDefinition, QueueBindingDefinition};
use crate::callbacks::RabbitConCallback;

/// Container for the connection parameters for the broker connection
#[derive(Debug, Clone, Default)]
pub struct RabbitConParams {
    /// Connection name
    pub con_name: Option<String>,
    /// Server name or IP address to connect to
    pub server: String,
    /// Port of the RabbitMq server
    pub port: u16,
    /// User used for authentication
    pub user: String,
    /// Password used for authentication
    pub password: String,
}



pub struct RabbitClient {
    /// Optional Application identifier, when set used a part of BasicProperties
    app_id: Option<String>,

    con_params: RabbitConParams,
    tx_cmd: Sender<ClientCommand>,
    con_callback: RabbitConCallback,
    cont: Arc<Mutex<ClientImplCont>>,

    topology: Topology,
}

impl RabbitClient {
    pub async fn new(con_params: RabbitConParams) -> Self {
        let (tx_cmd, rx_cmd): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(100);
        let con_callback = RabbitConCallback {
            tx_cmd: tx_cmd.clone(),
        };
        let contImpl = ClientImplCont {
            connection: None,
            tx_panic: None,
        };
        let cont = Arc::new(Mutex::new(contImpl));
        let ret = RabbitClient {
            app_id: None,
            con_params,
            tx_cmd,
            con_callback,
            cont: cont.clone(),
            topology: Topology {
                exchanges: Vec::new(),
                queues: Vec::new(),
                bindings: Vec::new(),
                cont,
            }
        };
        ret.start_cmd_receiver_task(rx_cmd);
        return ret;
    }

    pub async fn connect(&mut self) -> Result<(), String> {
        return RabbitClient::do_connect(&self.con_params, self.con_callback.clone(), &self.cont, 4).await;
    }

    pub async fn close(&self) {
        // TODO
    }

    pub async fn dummy(&self, id: u32) -> Result<String, ()> {
        let r = format!("hello from dummy to id={}", id);
        return Ok(r);
    }

    pub async fn set_panic_sender(&self, tx_panic: Sender<u32>) {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.tx_panic = Some(tx_panic);
    }

    pub async fn declare_exchange(&self, params: ExchangeDefinition) -> Result<(), String> {
        return self.topology.declare_exchange(params).await;
    }

    pub async fn declare_queue(&self, params: QueueDefinition) -> Result<(), String> {
        // TODO
        Ok(())
        // return self.client_impl.create_exchange(params).await;
    }

    pub async fn declare_queue_binding(&self, params: QueueBindingDefinition) -> Result<(), String> {
        // TODO
        Ok(())
        // return self.client_impl.create_exchange(params).await;
    }


    pub async fn new_publisher(&self, exchange_params: ExchangeDefinition) -> Result<Publisher, String> {
        Err("TODO".to_string())
    }

    pub async fn new_subscriber(&self, exchange_params: ExchangeDefinition, queue_params: QueueDefinition) -> Result<Subscriber, String> {
        Err("TODO".to_string())
    }


    fn start_cmd_receiver_task(&self, mut rx_command: Receiver<ClientCommand>) {
        let con_params = self.con_params.clone();
        let con_callback = self.con_callback.clone();
        let cont = self.cont.clone();
        tokio::spawn(async move {
            while let Some(cc) = rx_command.recv().await {
                debug!("receive client command: {}", cc);
                match cc {
                    ClientCommand::Connect => {
                        debug!("try to reset connection object ...");
                        {
                            let mut guard = cont.lock().await;
                            let client_cont: &mut ClientImplCont = &mut *guard;
                            client_cont.connection = None;
                        };
                        debug!("connection object reseted");
                        if let Err(_) = RabbitClient::do_connect(&con_params,con_callback.clone(),&cont,4).await {
                            let mut guard = cont.lock().await;
                            let client_cont: &mut ClientImplCont = &mut *guard;
                            match &client_cont.tx_panic {
                                Some(tx) => {
                                    debug!("send panic over channel");
                                    let _ = tx.send(0).await;
                                },
                                None => {
                                    warn!("would like to panic, but no panic channel sender is set");
                                }
                            }
                        }
                    }
                }
            }
            error!("I am leaving the management task 8-o");
        });
    }

    async fn do_connect(
        con_params: &RabbitConParams,
        con_callback: RabbitConCallback, 
        cont: &Arc<Mutex<ClientImplCont>>,
        max_reconnect_attempts: u8) -> Result<(), String> {
        let mut reconnect_seconds = 1;
        let mut reconnect_attempts: u8 = 0;
        loop {
            debug!("call try_to_connect");
            match RabbitClient::try_to_connect(&con_params,con_callback.clone()).await {
                Ok(connection) => {
                    let mut guard = cont.lock().await;
                    let client_cont: &mut ClientImplCont = &mut *guard;
                    client_cont.connection = Some(connection);
                    return Ok(());
                },
                Err(s) => {
                    warn!("error to connect: {}", s);
                    let sleep_time = time::Duration::from_secs(reconnect_seconds);
                    debug!("sleep for {} seconds before try to reconnect ...",reconnect_seconds);
                    thread::sleep(sleep_time);
                    reconnect_seconds = reconnect_seconds * 2;
                    reconnect_attempts += 1;
                    if reconnect_attempts > max_reconnect_attempts {
                        return Err(format!(
                            "reached maximum reconnection attempts ({}), and stop trying",
                            reconnect_attempts));
                    }
                }
            }
        }
    }

    async fn try_to_connect(con_params: &RabbitConParams, con_callback: RabbitConCallback) -> Result<Connection, String> {
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
                info!("connection established :), name={}",connection.connection_name());
                connection
                    .register_callback(con_callback)
                    .await
                    .unwrap();
                Ok(connection)
            }
            Err(e) => {
                error!("connection failure :(");
                Err(e.to_string())
            }
        }
    }

}

pub struct ClientImplCont {
    pub connection: Option<Connection>,
    pub tx_panic: Option<Sender<u32>>,
}

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


#[cfg(test)]
mod tests {
    use crate::rabbitclient;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn dummy() {
        let params = rabbitclient::RabbitConParams {
            con_name: None,
            server: "127.0.0.1".to_string(),
            port: 5672,
            user: "guest".to_string(),
            password: "guest".to_string(),
        };


        let client = rabbitclient::RabbitClient::new(params).await;

        for _ in 0 .. 1000 {
            for id in 0 .. 10 {
                let dummy_result = client.dummy(id).await.unwrap();
                assert_eq!(format!("hello from dummy to id={}", id), dummy_result);
            }
        }

        //client.close().await;
    }
}
