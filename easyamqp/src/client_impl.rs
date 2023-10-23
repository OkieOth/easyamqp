use crate::rabbitclient::{RabbitConParams, ExchangeType, ExchangeParams};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::{thread, time};
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

/// This is a helper container to allow switching e.g. connection instances
struct ClientImplCont {
    connection: Option<Connection>,
    tx_panic: Option<Sender<u32>>,
}


pub struct ClientImpl {
    con_params: RabbitConParams,
    tx_cmd: Sender<ClientCommand>,
    con_callback: RabbitConCallback,
    cont: Arc<Mutex<ClientImplCont>>,
}

impl ClientImpl {
    pub async fn new(con_params: RabbitConParams) -> Self {
        let (tx_cmd, rx_cmd): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(100);
        let con_callback = RabbitConCallback {
            tx_cmd: tx_cmd.clone(),
        };
        let cont = ClientImplCont {
            connection: None,
            tx_panic: None,
        };
        let ret = ClientImpl { 
            con_params, 
            tx_cmd, 
            con_callback, 
            cont: Arc::new(Mutex::new(cont)), };
        ret.start_cmd_receiver_task(rx_cmd);
        return ret;
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
                        if let Err(_) = ClientImpl::do_connect(&con_params,con_callback.clone(),&cont,4).await {
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

    pub async fn set_panic_sender(&self, tx_panic: Sender<u32>) {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.tx_panic = Some(tx_panic);
    }

    pub async fn connect(&self) -> Result<(), String> {
        return ClientImpl::do_connect(&self.con_params, self.con_callback.clone(), &self.cont, 4).await;
    }

    pub async fn dummy(&self, id: u32) -> String {
        return format!("hello from dummy to id={}", id);
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
            match ClientImpl::try_to_connect(&con_params,con_callback.clone()).await {
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

    pub async fn create_exchange(&self,params: ExchangeParams) -> Result<(), String> {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        match &client_cont.connection {
            Some(con) => {
                if con.is_open() {
                    return self.do_create_exchange(&con, params).await;
                } else {
                    return Err("broker connection isn't open".to_string());
                }
            },
            None => {
                return Err("no broker connection available".to_string());
            }
        }
    }

    pub async fn do_create_exchange(&self,con: &Connection,params: ExchangeParams) -> Result<(), String> {
        let channel = con.open_channel(None).await.unwrap();
        let type_str: String = params.exhange_type.into();
        let mut args = ExchangeDeclareArguments::new(params.name.as_str(), type_str.as_str());
        args.auto_delete = params.auto_delete;
        args.durable = params.durable;
        if let Err(e) = channel.exchange_declare(args).await {
            return Err(e.to_string());
        };
        Ok(())
    }
}