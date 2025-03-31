pub mod clients;
pub use clients::{BlockingClient, BufferedClient, Client};

pub mod cmd;
pub use cmd::Command;

pub mod frame;
pub use frame::Frame;

pub mod connection;
pub use connection::Connection;

pub mod shutdown;   
use shutdown::Shutdown;

pub mod parse;
use parse::{Parse, ParseError};

pub mod db;
use db::{Db, DbDropGuard};

pub mod server;
/// Default port that a redis server listens on
///
/// Used if no port is specified
pub const DEFAULT_PORT: u16 = 6379;

/// Error returned by most functions.
///
/// When writing a rea application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for this example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handld during normal execution when a partial frame is received on a socket.
/// `std::error::Error` is implemented for `parse::Error` which allows it to be
/// converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for my-mini-redis operation
/// This is defined as a convenience
pub type Result<T> = std::result::Result<T, Error>;
