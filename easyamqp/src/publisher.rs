use crate::topology::ExchangeDefinition;

pub struct Publisher {
    pub exchange_params: ExchangeDefinition,
    content_type: Option<String>,
    content_encoding: Option<String>,
    priority: Option<MessagePriority>,
    message_type: Option<String>,

}

#[derive(Debug, Clone, Default)]
pub enum MessagePriority {
    /// AMQP value 0
    NormalLowest,
    /// AMQP value 1
    NormalLower,
    /// AMQP value 2
    NormalLow,
    /// AMQP value 3
    Normal,
    /// AMQP value 4
    #[default]
    NormalHigh,
    /// AMQP value 5
    HigherThanNormal,
    /// AMQP value 6
    HighLow,
    /// AMQP value 7
    High,
    /// AMQP value 8
    Higher,
    /// AMQP value 9
    Highest,
}

pub struct PublishingParams {
    pub routing_key: String,
    pub mandatory: bool,
    correlation_id: Option<String>,
    expiration: Option<String>,
    message_id: Option<String>,
    timestamp: Option<String>,
    message_type: Option<String>,
    user_id: Option<String>,
}

pub enum PublishError {
    Todo,
}

impl Publisher {
    pub fn publish(content: Vec<u8>, params: PublishingParams) -> Result<(), PublishError> {
        Err(PublishError::Todo)
    }
}