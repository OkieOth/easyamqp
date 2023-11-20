use clap::Args;

#[derive(Debug, Args)]
pub struct PublishArgs {
    /// Exchange to bind the queue
    #[arg(short, long)]
    pub exchange: Option<String>,

    /// Routing key to bind the queue with the exchange
    #[arg(short, long)]
    pub routing_key: Option<String>,

    /// Name of the input file with the content to publish
    #[arg(short, long)]
    pub input_file: Option<String>,

    /// Miliseconds to delay the publishing
    #[arg(short, long)]
    pub delay_millis: Option<usize>,

    /// Address of the rabbitmq server
    #[arg(long, default_value="127.0.0.1")]
    pub server: String,

    /// Port of the rabbitmq server
    #[arg(long, default_value_t=5672)]
    pub port: usize,

    /// User name
    #[arg(long, default_value="guest")]
    pub user: String,

    /// Password
    #[arg(long, default_value="guest")]
    pub passwort: String,
}
