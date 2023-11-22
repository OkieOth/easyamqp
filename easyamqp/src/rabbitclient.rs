//! Simple RabbitMq Client implementation. It utilized amqprs (https://github.com/gftea/amqprs)
//! The main handle to the client is a thread safe RabbitCient instance, that works as
//! factory to create internally the needed connection objects. In addition it is used the
//! create workers on the connection that can be used for publishing and subscribing of data.
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use std::result::Result;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};

use amqprs::connection::{Connection, OpenConnectionArguments};


use crate::publisher::Publisher;
use crate::publisher::PublisherParams;
use crate::subscriber::{Subscriber, SubscribeParams};
use crate::topology::Topology;
use crate::topology::{ExchangeDefinition, QueueDefinition, QueueBindingDefinition};
use crate::callbacks::RabbitConCallback;
use crate::worker::Worker;
use crate::utils::get_env_var_str;

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

impl RabbitConParams {
    pub fn builder() -> RabbitConParamsBuilder {
        RabbitConParamsBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RabbitConParamsBuilder {
    /// Connection name
    con_name: Option<String>,
    /// Server name or IP address to connect to
    server: Option<String>,
    /// Port of the RabbitMq server
    port: u16,
    /// User used for authentication
    user: Option<String>,
    /// Password used for authentication
    password: Option<String>,
}

impl RabbitConParamsBuilder {
    pub fn new() -> RabbitConParamsBuilder {
        RabbitConParamsBuilder::default()
    }

    pub fn con_name(mut self, con_name: &str) -> RabbitConParamsBuilder {
        self.con_name = Some(con_name.to_string());
        self
    }
    pub fn server<'a> (mut self, server: &str) -> RabbitConParamsBuilder {
        self.server = Some(server.to_string());
        self
    }
    pub fn user<'a> (mut self, user: &str) -> RabbitConParamsBuilder {
        self.user = Some(user.to_string());
        self
    }
    pub fn password<'a> (mut self, password: &str) -> RabbitConParamsBuilder {
        self.password = Some(password.to_string());
        self
    }
    pub fn port(mut self, port: u16) -> RabbitConParamsBuilder {
        self.port = port;
        self
    }

    pub fn build(&self) -> RabbitConParams {
        RabbitConParams {
            con_name: self.con_name.as_deref().map(|s| Some(s.to_string())).unwrap_or_default(),
            server: self.server.as_deref().map(|s| s.to_string()).unwrap_or_default(),
            user: self.user.as_deref().map(|s| s.to_string()).unwrap_or_default(),
            password: self.password.as_deref().map(|s| s.to_string()).unwrap_or_default(),
            port: if self.port == 0 { 5672 } else { self.port },
        }
    }
}

pub struct RabbitClient {
    con_params: RabbitConParams,
    tx_cmd: Sender<ClientCommand>,
    con_callback: RabbitConCallback,
    cont: Arc<Mutex<ClientImplCont>>,
}

impl RabbitClient {
    pub async fn new(con_params: &RabbitConParams) -> Self {
        let (tx_cmd, rx_cmd): (Sender<ClientCommand>, Receiver<ClientCommand>) = mpsc::channel(100);
        let con_callback = RabbitConCallback {
            tx_cmd: tx_cmd.clone(),
        };
        let top = Topology::new();
        let cont_impl = ClientImplCont {
            connection: None,
            tx_panic: None,
            topology: top,
            max_reconnect_attempts: 3,
            workers: Vec::new(),
            max_worker_id: 0,
            reconnect_count: 0,
        };
        let c = Arc::new(Mutex::new(cont_impl));
        let ret = RabbitClient {
            con_params: con_params.clone(),
            tx_cmd,
            con_callback,
            cont: c.clone(),
        };
        ret.start_cmd_receiver_task(rx_cmd);
        return ret;
    }

    /// This function is mostly used to reduce the code for tests
    pub async fn get_default_client() -> (RabbitClient, RabbitConParams) {
        let user_name = get_env_var_str("RABBIT_USER", "guest");
        let password = get_env_var_str("RABBIT_PASSWORD", "guest");
        let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");
    
        let params = RabbitConParams::builder()
            .server(&rabbit_server)
            .user(&user_name)
            .password(&password)
            .build();
    
        (RabbitClient::new(&params).await, params)
    }
    
    pub async fn get_default_client_with_name(con_name: &str) -> (RabbitClient, RabbitConParams) {
        let user_name = get_env_var_str("RABBIT_USER", "guest");
        let password = get_env_var_str("RABBIT_PASSWORD", "guest");
        let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");
    
        let params = RabbitConParams::builder()
            .server(&rabbit_server)
            .user(&user_name)
            .password(&password)
            .con_name(con_name)
            .build();
    
        (RabbitClient::new(&params).await, params)
    }

    pub async fn connect(&mut self) -> Result<(), String> {
        return RabbitClient::do_connect(&self.con_params, self.con_callback.clone(), &self.cont, 4).await;
    }

    pub async fn close(&self) {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        if client_cont.connection.is_some() {
            {
                let c = client_cont.connection.as_mut().unwrap().clone();
                if let Err(e) = c.close().await {
                    error!("error while close connection: {}", e.to_string())
                }
            }
            client_cont.connection = None;
        } else {
            warn!("connection is None ... nothing to close");
        }
    }

    pub async fn dummy(&self, id: u32) -> Result<String, ()> {
        let r = format!("hello from dummy to id={}", id);
        return Ok(r);
    }

    pub async fn set_panic_sender(&self, tx_panic: Sender<String>) {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.tx_panic = Some(tx_panic);
    }

    pub async fn declare_exchange(&self, exchange_def: ExchangeDefinition) -> Result<(), String> {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        if client_cont.connection.is_some() {
            client_cont.topology.declare_exchange(exchange_def, &client_cont.connection.as_ref().unwrap()).await
        } else {
            Err("Connection isn't ready".to_string())
        }
    }

    pub async fn declare_queue(&self, queue_def: QueueDefinition) -> Result<(), String> {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        if client_cont.connection.is_some() {
            client_cont.topology.declare_queue(queue_def, &client_cont.connection.as_ref().unwrap()).await
        } else {
            Err("Connection isn't ready".to_string())
        }
    }

    pub async fn declare_queue_binding(&self, binding_def: QueueBindingDefinition) -> Result<(), String> {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        if client_cont.connection.is_some() {
            client_cont.topology.declare_queue_binding(binding_def, &client_cont.connection.as_ref().unwrap()).await
        } else {
            Err("Connection isn't ready".to_string())
        }
    }

    pub async fn new_publisher_from_params(&self, params: PublisherParams) -> Result<Publisher, String> {
        debug!("new_publisher_from_params is wating for lock ...");
        let mut guard = self.cont.lock().await;
        debug!("new_publisher_from_params got lock");
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.max_worker_id += 1;

        match Publisher::new(client_cont.max_worker_id, params, self.tx_cmd.clone()).await {
            Ok(publisher) => {
                {
                    let mut worker_guard = publisher.worker.lock().await;
                    let worker: &mut Worker = &mut *worker_guard;
                    match Self::set_channel_to_worker(&client_cont.connection,worker, false).await {
                        Ok(_) => {
                            client_cont.workers.push(publisher.worker.clone());
                        },
                        Err(msg) => {
                            return Err(msg);
                        },
                    }
                }
                return Ok(publisher);
            },
            Err(msg) => {
                return Err(msg);
            },
        }
    }

    pub async fn new_publisher(&self) -> Result<Publisher, String> {
        let params = PublisherParams::builder().build();
        self.new_publisher_from_params(params).await
    }

    pub async fn new_subscriber(&self, params: SubscribeParams) -> Result<Subscriber, String> {
        debug!("new_publisher_from_params is wating for lock ...");
        let mut guard = self.cont.lock().await;
        debug!("new_publisher_from_params got lock");
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.max_worker_id += 1;
        match Subscriber::new(client_cont.max_worker_id, self.tx_cmd.clone() , params).await {
            Ok(subscriber) => {
                {
                    let mut worker_guard = subscriber.worker.lock().await;
                    let worker: &mut Worker = &mut *worker_guard;
                    match Self::set_channel_to_worker(&client_cont.connection,worker, false).await {
                        Ok(_) => {
                            client_cont.workers.push(subscriber.worker.clone());
                            client_cont.topology.register_subscriber(
                                worker.id, 
                                &subscriber.params.queue_name,
                                worker.tx_inform_about_new_channel.as_ref().unwrap().clone()).await;
                        },
                        Err(msg) => {
                            return Err(msg);
                        },
                    }
                }
                return Ok(subscriber);
            },
            Err(msg) => {
                return Err(msg);
            },
        }
    }

    
    /// Sends a panic message to the client host
    async fn send_panic(panic_msg: String, cont: &Arc<Mutex<ClientImplCont>>) {
        let mut guard = cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        match &client_cont.tx_panic {
            Some(tx) => {
                debug!("send panic over channel");
                let _ = tx.send(panic_msg).await;
            },
            None => {
                warn!("would like to panic, but no panic channel sender is set");
            }
        }
    }


    pub async fn set_channel_to_worker(connection: &Option<Connection>, worker: &mut Worker, inform_worker: bool) -> Result<(), String> {
        if connection.is_some() {
            let connection = connection.as_ref().unwrap();
            match connection.open_channel(None).await {
                Ok(channel) => {
                    channel
                        .register_callback(worker.callback.clone())
                        .await
                        .unwrap();
                    debug!("set channel for worker (id={})", worker.id);
                    worker.channel = Some(channel);
                    if inform_worker && worker.tx_inform_about_new_channel.is_some() {
                        debug!("inform worker (id={}) about new channel", worker.id);
                        let _ = worker.tx_inform_about_new_channel.as_ref().unwrap().send(worker.id).await;
                    }
                    Ok(())
                }
                Err(e) => {
                    let msg = format!(
                        "error while creating channel for worker (id={}): {}",
                        worker.id,
                        e.to_string());
                    error!("{}", msg);
                    Err(msg)
                }
            }

        } else {
            let msg = format!("no connection, unable to provide channel for worker (id={})", worker.id);
            warn!("{}", msg);
            Err(msg)
        }
    }

    async fn remove_worker(cont: &Arc<Mutex<ClientImplCont>>, id_to_remove: u32) {
        let mut guard = cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;

        let mut new_workers = vec![];

        for worker in &client_cont.workers {
            let worker_guard = worker.lock().await;
            let w: &Worker = &*worker_guard;
            if w.id != id_to_remove {
                new_workers.push(worker.clone());
            }
        }
    
        client_cont.workers = new_workers;
    }

    async fn remove_subscriber(cont: &Arc<Mutex<ClientImplCont>>, id_to_remove: u32) {
        let mut guard = cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.topology.remove_subscriber(id_to_remove).await;
    }

    async fn check_queue(cont: &Arc<Mutex<ClientImplCont>>, id: u32) {
        let mut guard = cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        // TODO maybe multiple tries???
        if client_cont.connection.is_some() {
            client_cont.topology.check_queue(id, &client_cont.connection.as_ref().unwrap()).await;
        }
    }

    async fn provide_channel(cont: &Arc<Mutex<ClientImplCont>>, id: u32) {
        let mut guard = cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        let mut found = false;
        for w in client_cont.workers.iter() {
            let mut worker_guard = w.lock().await;
            let worker: &mut Worker = &mut *worker_guard;
            if worker.id == id {
                found = true;
                let _ = RabbitClient::set_channel_to_worker(&client_cont.connection, worker, true).await;
            }
        }
        if ! found {
            warn!("didn't find worker (id={}) to provide a channel", id);
        }
    }

    async fn recreate_topology(cont: &Arc<Mutex<ClientImplCont>>) {
        let mut guard = cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;

        let mut reconnect_seconds = 1;
        let mut reconnect_attempts: u8 = 0;
        let mut success: bool;
        loop {
            success = client_cont.topology.declare_all_exchanges(client_cont.connection.as_ref().unwrap()).await.is_ok() &&
            client_cont.topology.declare_all_queues(client_cont.connection.as_ref().unwrap()).await.is_ok() &&
            client_cont.topology.declare_all_bindings(client_cont.connection.as_ref().unwrap()).await.is_ok();
            if ! success {
                if reconnect_attempts > client_cont.max_reconnect_attempts {
                    let msg = format!("reached maximum attempts ({}) to reestablish topology, and stop trying",
                    reconnect_attempts);
                    error!("{}", msg);
                    RabbitClient::send_panic(msg, cont).await;
                    break;
                } else {
                    let sleep_time = Duration::from_secs(reconnect_seconds);
                    debug!("sleep for {} seconds before try to reestablish topology ...",reconnect_seconds);
                    sleep(sleep_time).await;
                    reconnect_seconds = reconnect_seconds * 2;
                    reconnect_attempts += 1;
                }
            } else {
                break;
            }
        }
    }

    async fn recreate_channel(cont: &Arc<Mutex<ClientImplCont>>) {
        let mut guard = cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;

        for w in client_cont.workers.iter() {
            let mut worker_guard = w.lock().await;
            let worker: &mut Worker = &mut *worker_guard;
            if worker.channel.is_some() {
                worker.channel = None;
            }
            if let Err(e) = worker.callback.tx_req.send(ClientCommand::GetChannel(worker.id)).await {
                error!("error while requesting channel for worker (id={})", worker.id);
            }
        }
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
                        if let Err(s) = RabbitClient::do_connect(&con_params,con_callback.clone(),&cont,4).await {
                            RabbitClient::send_panic(s, &cont).await;
                        } else {
                            {
                                let mut guard = cont.lock().await;
                                let client_cont: &mut ClientImplCont = &mut *guard;
                                client_cont.reconnect_count += 1;
                            }
                            RabbitClient::recreate_topology(&cont).await;
                            RabbitClient::recreate_channel(&cont).await;
                        }
                    },
                    ClientCommand::GetChannel(id) => {
                        debug!("received a get channel request for id={}", id);
                        RabbitClient::provide_channel(&cont, id).await
                    },
                    ClientCommand::RemoveWorker(id) => {
                        RabbitClient::remove_worker(&cont, id).await;
                    },
                    ClientCommand::RemoveSubscriber(id) => {
                        RabbitClient::remove_worker(&cont, id).await;
                        RabbitClient::remove_subscriber(&cont, id).await;
                    },
                    ClientCommand::CheckQueue(id) => {
                        RabbitClient::check_queue(&cont, id).await;
                    }
                    ClientCommand::Panic(msg) => {
                        RabbitClient::send_panic(msg, &cont).await;
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
                    if reconnect_attempts > max_reconnect_attempts {
                        return Err(format!(
                            "reached maximum reconnection attempts ({}), and stop trying",
                            reconnect_attempts));
                    } else {
                        warn!("error to connect: {}", s);
                        let sleep_time = Duration::from_secs(reconnect_seconds);
                        debug!("sleep for {} seconds before try to reconnect ...",reconnect_seconds);
                        sleep(sleep_time).await;
                        reconnect_seconds = reconnect_seconds * 2;
                        reconnect_attempts += 1;
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

    pub async fn get_worker_count(&self) -> usize {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.workers.len()
    }

    pub async fn get_reconnect_count(&self) -> usize {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.reconnect_count
    }

    pub async fn get_subscriber_count(&self) -> usize {
        let mut guard = self.cont.lock().await;
        let client_cont: &mut ClientImplCont = &mut *guard;
        client_cont.topology.get_subscriber_count().await
    }
}

pub struct ClientImplCont {
    topology: Topology,
    pub connection: Option<Connection>,
    pub tx_panic: Option<Sender<String>>,
    max_reconnect_attempts: u8,
    workers: Vec<Arc<Mutex<Worker>>>,
    max_worker_id: u32,
    reconnect_count: usize,
}

pub enum ClientCommand {
    Connect,
    GetChannel(u32),
    RemoveWorker(u32),
    RemoveSubscriber(u32),
    CheckQueue(u32),
    Panic(String),
}

impl std::fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientCommand::Connect => write!(f, "Connect"),
            ClientCommand::GetChannel(id) => write!(f, "GetChannel(id={id})"),
            ClientCommand::RemoveWorker(id) => write!(f, "RemoveWorker(id={id})"),
            ClientCommand::RemoveSubscriber(id) => write!(f, "RemoveSubscriber(id={id})"),
            ClientCommand::CheckQueue(id) => write!(f, "CheckQueue(id={id}"),
            ClientCommand::Panic(msg) => write!(f, "Panic(msg={msg})"),
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::rabbitclient;

    #[test]
    fn rabbitconparam_builder_test() {
        let params = rabbitclient::RabbitConParams::builder()
            .server("test_server")
            .user("test_user")
            .password("test_pwd")
            .build();
        assert_eq!("test_server", params.server);
        assert_eq!("test_user", params.user);
        assert_eq!("test_pwd", params.password);
        assert_eq!(5672, params.port);
        assert_eq!(None, params.con_name);

        let params2 = rabbitclient::RabbitConParams::builder()
            .server("test_server2")
            .user("test_user2")
            .password("test_pwd2")
            .con_name("test_con")
            .port(4242)
            .build();

        assert_eq!("test_server2", params2.server);
        assert_eq!("test_user2", params2.user);
        assert_eq!("test_pwd2", params2.password);
        assert_eq!(4242, params2.port);
        assert_eq!(Some("test_con".to_string()), params2.con_name);

    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn dummy() {
        let (client, _) = rabbitclient::RabbitClient::get_default_client().await;

        for _ in 0 .. 1000 {
            for id in 0 .. 10 {
                let dummy_result = client.dummy(id).await.unwrap();
                assert_eq!(format!("hello from dummy to id={}", id), dummy_result);
            }
        }

        //client.close().await;
    }
}
