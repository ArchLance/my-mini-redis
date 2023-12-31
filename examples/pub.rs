//! Publish to a redis channel example
//! 
//! A simple client that connects to a mini-reids server, and
//! publishes a message on `foo` channel
//! 
//! You can test this out by running:
//! 
//!     cargo run --bin my-mini-redis-server
//! 
//! Then in another terminal run:
//! 
//!     cargo run --example sub
//! 
//! And then in another terminal run:
//! 
//!     cargo run --example pub

#![warn(rust_2018_idioms)]

use my_mini_redis::{clients::Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Open a connection to the mini-redis address.
    let mut client = Client::connect("127.0.0.1:6379").await?;

    client.publish("foo", "bar".into()).await?;

    Ok(())
}