use crate::cmd::Unknown;
use crate::{Command, Connection, Db, Frame, Shutdown, Parse, ParseError};

use bytes::Bytes;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// Subcribes the client to one or more channels.
/// 
/// Once the client enters the subcribed state, it is not supposed to issue any
/// other commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
/// PUNSUBSCRIBE, PING and QUIT commands.
#[derive(Debug)]
pub struct Subcribe {
    channels: Vec<String>,
}

/// Unsubscribes the client from one or more channels.
/// 
/// When no channels are specified, the client is unsubscribed from all the
/// previously subscribed channels.
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// Stream of messages. The stream receives messages from the
/// `broadcast::Receiver`. We use `stream!` to create a `Stream` that consumes
/// messages. Because `stream!` values cannot be named, we box the stream using
/// a trait object
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subcribe {
    /// Create a new `Subscribe` command to listen on the specified channels.
    pub(crate) fn new(channels: Vec<String>) -> Subcribe {
        Subcribe { channels }
    }

    /// Parse a `Subscribe` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SUBSCRIBE` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Subscribe` value is returned. If the frame is
    /// malformed, `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two or more entries.
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subcribe> {
        use ParseError::EndOfStream;

        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subcribe { channels })
    }

    /// Apply the `Subscribe` command to the specified `Db` instance.
    /// 
    /// This function is the entry point and includes the initial list of
    /// channels to subscribe to. Additional `subscribe` and `unsubscribe`
    /// commands may be received from the client and the list of subscriptions
    /// are updated accordingly.
    /// 
    /// [here]: https://redis.io/topics/pubsub
    pub(crate) async fn apply (
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // 每个单独的channel订阅都使用`sync::broadcast` channel被处理。
        // 消息被发送给所有当前订阅channels的客户端。
        //
        // 一个单独的客户端可能订阅多个channels 可能动态从他们的subscription set中
        // 添加或者移除channel。 为了处理这个，`StreamMap` 被用来跟踪有效订阅。
        // `StreamMap` 会在接收到来自各个channels的messages时将其合并.
        let mut subscriptions = StreamMap::new();

        loop {
            // `self.channels` 被用来跟踪要订阅的其他频道
            // 当一个新的 `SUBSCRIBE` 命令在执行`apply`的过程中被收到，
            // 新的channels 被放到这个vec中
            // 这个表达式使用 drain 方法来移除 self.channels 中的所有元素
            //并返回一个迭代器，该迭代器允许你遍历被移除的元素。
            for channel_name in self.channels.drain(..) {
                subscibe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // 等待下面其中的一个事件发生：
            //
            // - 从其中一个subscribed channels中收到一个消息
            // - 从客户端收到一个 subscribe 或者 unsubscribe 命令
            // - 服务端关闭信号
            select!{
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        None => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(());
                }

            };
        }
    }
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscibe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection
) -> crate::Result<()> {
    let mut rx = db.subscibe(channel_name.clone());
    //async_stream::stream! 是一个宏，用于方便地创建一个实现 Stream trait 的异步流。
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                //如果接收操作成功（即 Ok(msg)），
                //则使用 yield 关键字将消息放入流中。yield 用于生成流中的下一个值。
                Ok(msg) => yield msg,
                // 如果消费消息之后，请继续
                Err(broadcast::error::RecvError::Lagged(_)) => {},
                Err(_) => break,
            }
        }
    });

    subscriptions.insert(channel_name.clone(), rx);

    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}
/// Handle a command received while inside `Subscribe::apply`. Only subscribe
/// and unsubscribe commands are permitted in this context.
/// 
/// Any new subscriptions are appended to `subscribe_to` instead of modifying
/// `subscriptions`
async fn handle_command (
    frame: Frame,
    subscibe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection
) -> crate::Result<()> {
    // 一个指令从客户端收到
    // 只有`SUBSCRIBE`和`UNSUBSCRIBE`命令允许被处理
    match Command::from_frame(frame)? {
        Command::Subcribe(subscibe) => {
            subscibe_to.extend(subscibe.channels.into_iter())
        },
        Command::Unsubscribe(mut unsubscribe) => {
            // 如果没有channels被指定，会请求所有channels取消订阅。
            // 为了实现增功能，`unsubscribe.channels`容器将填充
            // 当前订阅的列表
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        },
        other => {
            let cmd = Unknown::new(other.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}


/// Creates the response to a subscribe request
/// 
/// All of these functions take the `channel_name` as a `String` instead of
/// a `&str` since `Bytes::from` can reuse the allocation in the `String`,
/// and taking a `&str` would require copying the data. This allows the caller
/// to decide whether to clone the channel name or not.
/// 重要的是，这个过程可以重用 String 中的内存分配。这意味着在将 String 转换为 Bytes 时，
/// 不需要分配新的内存来存储字符串数据，从而提高效率。而使用`&str`会拷贝数据。
/// 这允许调用者是否需要clone channel name
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_unsubscribe_frame(channel_name: String, num_subs:usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

        /// Parse an `Unsubscribe` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `UNSUBSCRIBE` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Unsubscribe` value is returned. If the frame is
    /// malformed, `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least one entry.
    ///
    /// ```text
    /// UNSUBSCRIBE [channel [channel ...]]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),

                Err(EndOfStream) => break,

                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe{ channels })
    }
    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding an `Unsubscribe` command to
    /// send to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}