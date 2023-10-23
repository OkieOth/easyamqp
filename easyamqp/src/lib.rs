mod callbacks;
mod client_impl;
mod publisher;
mod subscriber;
pub mod rabbitclient;
pub mod topology;
pub mod utils;

pub fn dummy() {
    println!("    Hello, from the lib!");
}
