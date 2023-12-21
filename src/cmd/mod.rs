mod get;
pub use get::Get;

mod ping;
pub use ping::Ping;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscibe;
pub use subscibe::{Subcribe, Unsubscribe};

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subcribe(Subcribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown)
}

impl Command {
    
}