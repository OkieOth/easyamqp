use crate::rabbitclient::{ClientCommand, ClientCommandResponse, RabbitConParams};
use tokio::sync::mpsc::{Receiver, Sender};

pub struct ClientImpl {
    pub con_params: RabbitConParams,
    pub rx_command: Receiver<ClientCommand>,
    pub tx_command_response: Sender<ClientCommandResponse>,
}
