use clap::Args;

#[derive(Debug, Args)]
pub struct SubscribeArgs {
    /// Queue name to subscribe to
    #[clap(long, short)]
    pub queue: Option<String>,

    /// Exchange to bind the queue
    #[arg(short, long)]
    pub exchange: Option<String>,

    /// Routing key to bind the queue with the exchange
    #[arg(short, long)]
    pub routing_key: Option<String>,

    /// Name of the output file
    #[arg(short, long)]
    pub output_file: Option<String>,

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
