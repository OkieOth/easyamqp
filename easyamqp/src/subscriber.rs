
#[async_trait::async_trait]
pub trait SubscriptionConsumer {

}

pub struct Subscriber {

}

impl Subscriber {
    pub fn subscibe<F: SubscriptionConsumer>() -> Result<(), String> {
        Ok(())
    }
}

