use crate::cmd::Unknown;
use crate::{Command, Connection, Db, Frame, Parse, ParseError, Shutdown};

use bytes::Bytes;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// 客户端订阅一个或多个channel。
///
/// 一旦客户端进入子订阅状态，除了附加的 SUBSCRIBE、PSUBSCRIBE、UNSUBSCRIBE 命令外，
/// 它不应再发出任何其他命令、 PUNSUBSCRIBE、PING 和 QUIT 命令。
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// 客户端退订一个或者多个channels
///
/// 如果没有指定频道，客户端将取消订阅所有已订阅的频道。
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// 信息流。信息流从 `broadcast::Receiver` 接收信息
/// 使用`stream!`创建一个消耗信息的stream，由于stream!值不能命名
/// 使用一个trait对象
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// Create a new `Subscribe` command to listen on the specified channels.
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// 从接收到的frame中解析一个Subscribe实例
    ///
    /// Parse "参数提供了一个类似于游标的 API，用于从`Frame`中读取字段。
    /// 此时，整个帧已经从socket 收到。
    ///
    /// "subscribe"字符串在这之前已经被消耗
    ///
    /// # Returns
    ///
    /// 成功返回Subscribe实例，如果不完整返回Err
    ///
    /// # Format
    ///
    /// 接受一个数组frame包含2个及以上entries
    ///
    /// ```text
    /// subscribe channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /// 将Subscribe命令应用到Db实例中
    ///
    /// 该函数是入口点，包含要订阅的频道的初始列表。
    /// 服务端从客户端可能会接收到额外的 `subscribe` 和 `unsubscribe` 命令，
    ///
    /// [here]: https://redis.io/topics/pubsub
    pub(crate) async fn apply(
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
            // 并返回一个迭代器，该迭代器允许你遍历被移除的元素。
            for channel_name in self.channels.drain(..) {
                subscibe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // 等待下面其中的一个事件发生：
            //
            // - 从其中一个subscribed channels中收到一个消息
            // - 从客户端收到一个 subscribe 或者 unsubscribe 命令
            // - 服务端关闭信号
            select! {
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
    dst: &mut Connection,
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
/// 处理在 `Subscribe::apply` 中收到的命令。在此上下文中只允许订阅 和取消订阅命令。
///
/// 任何新subscriptions都会附加到 `subscribe_to` 中，而不是修改`subscriptions`
async fn handle_command(
    frame: Frame,
    subscibe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {
    // 一个指令从客户端收到
    // 只有`SUBSCRIBE`和`UNSUBSCRIBE`命令允许被处理
    match Command::from_frame(frame)? {
        Command::Subcribe(subscibe) => subscibe_to.extend(subscibe.channels.into_iter()),
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
        }
        other => {
            let cmd = Unknown::new(other.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

/// 创建对订阅请求的响应
///
/// 所有这些函数都将 `channel_name` 作为 `String` 而不是 `&str`，
/// 因为 `Bytes::from` 可以重用 `String` 中的分配，而 `&str` 则需要复制数据。
/// 这允许调用者决定是否克隆通道名。
///
/// 这个的总结如下
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

fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
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
    /// 从接收到的frame中解析一个Unsubscribe实例
    ///
    /// Parse参数提供了一个类似于游标的 API，用于从`Frame`中读取字段。
    /// 此时，整个帧已经从socket 收到。
    ///
    /// "unsubscribe"字符串已经被消耗
    ///
    /// # Returns
    ///
    /// 成功返回Unsubscribe实例，frame不完全返回 Err
    ///
    /// # Format
    ///
    /// 接收一个数组Frame至少含有一个entry
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

        Ok(Unsubscribe { channels })
    }

    /// 将命令转换为对等的Frame
    ///
    /// 当需要把unsubscribe命令发送给server时，这个函数被client调用
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
