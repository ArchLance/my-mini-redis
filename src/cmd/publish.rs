use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;

/// Posts a message to the given channel.
/// 
/// Send a message into a channel without any knowledge of individual comsumers
/// Consumers may subscribe to channels in order to receive the messages.
/// 
/// Channel names have no relation to the key-value namespace. Publishing on a
/// channel named "foo" has no relation to setting the "foo" key.
#[derive(Debug)]
pub struct Publish {
    /// Name of the channel on which the message should be published.
    channel: String,

    /// The message to publish.
    message: Bytes,
}

impl Publish {
    /// Create a new `Publish` command which sends `message` on `channel`.
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }
    /// Parse a `Publish` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `PUBLISH` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Publish` value is returned. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing three entries.
    ///
    /// ```text
    /// PUBLISH channel message
    /// ``` 
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;

        Ok(Publish {channel, message})
    }
    /// Apply the `Publish` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let num_subscribers = db.publish(&self.channel, self.message);

        let response = Frame::Integer(num_subscribers as u64);

        dst.write_frame(&response).await?;

        Ok(())
    } 
    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Publish` command to send
    /// to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }
}