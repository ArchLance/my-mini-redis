use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;

/// 向指定channel中发送一条信息
///
/// 向频道发送信息，而无需了解每个消费者的情况 消费者可以订阅频道，以便接收信息。
///
///  通道名称与key-value的map无关。在名为 “foo”的频道上发布与设置 “foo” 键没有任何关系。
#[derive(Debug)]
pub struct Publish {
    /// 应发布信息的频道名称。
    channel: String,

    /// 要发布的信息。
    message: Bytes,
}

impl Publish {
    /// 创建一个新的在 `channel` 上发送 `message`的 `Publish` 命令。
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }
    /// 将收到的frame解析为一个Publish实例
    ///
    /// Parse参数提供了一个类似于游标的 API，用于从`Frame`中读取字段。
    /// 此时，整个帧已经从socket中收到。
    ///
    ///  `publish`字符串已经被消费了
    ///
    /// # Returns
    ///
    /// 成功返回Publish实例，不完整则返回Err
    ///
    /// # Format
    ///
    /// 数组frame应该包含3个entries
    ///
    /// ```text
    /// publish channel message
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }
    /// 应用Ping命令并返回信息
    ///
    /// 回复被写回dst，这个函数被server调用，目的是为了执行收到的命令
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let num_subscribers = db.publish(&self.channel, self.message);

        let response = Frame::Integer(num_subscribers as u64);

        dst.write_frame(&response).await?;

        Ok(())
    }
    /// 将命令转换为对等的Frame
    ///
    /// 当需要把Publish命令发送给server时，这个函数被client调用
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }
}
