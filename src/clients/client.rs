//! Minimal Redis client implementation
//!  
//! Provides an async connect and methods for issuing the supported commands.


use crate::cmd::{Get, Ping, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

use async_stream::try_stream;
use bytes::Bytes;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::{debug, instrument};

/// Established connection with a Redis server.
/// 
/// Backed by a single `TcpStream`, `Client` provides basic network client
/// functionality (no pooling, retrying, ...). Connections are established using
/// the [`connect`](fn@connect) function.
/// 
/// Requests are issued using the various methods of `Client`.
pub struct Client {
    /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    /// 
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connect: Connection,
}

/// A client that has entered pub/sub mode
/// 
/// Once clients subscribe to a channel, they may only perform pub/sub related
/// commands. The `Client` type is transitioned to a `Subscriber` type in order to
/// prevent non-pub/sub methods from being called.
struct Subscriber {
    client: Client,

    subscribed_channels: Vec<String>,
}

#[derive(Deubg, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    /// Establish a connection with the Redis server located at `addr`.
    /// 
    /// `addr` may be any type that can be asynchronously converted to a 
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSokcetAddrs`
    /// trait is the Tokio version and not the `std` version.
    /// 
    /// # Examples
    /// 
    /// ```no_run
    /// use mini_redis::clients::Client;
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = match Client::connect("localhost:6379").await {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        // `addr`变量直接被传递给`TcpStream::connect`. 这将执行任何异步 DNS 查找，
        //并尝试建立 TCP 连接。无论哪一步出错，都会返回错误信息，
        //并向 `mini_redis` connect 的调用者通报。
        let socket = TcpStream::connect(addr).await?;

        // 初始化连接状态。为read/write buffers开辟空间，来执行redis协议中frame的解析
        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    /// Ping to the server.
    /// 
    /// Returns PONG if no argument is provided, otherwise
    /// return a copy of the argument as a bulk.
    /// 
    /// This command is often used to test if a connection
    /// is still alive, or to measure latency.
    /// 
    /// # Example
    /// 
    /// Demonstrates basic usage
    /// ```no_run
    /// use mini_redis::clients::Client;
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///     
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error())
        }
    }

    /// Get the value of key
    /// 
    /// If the key does not exist the special value `None` is returned.
    /// 
    /// # Examples
    /// 
    /// Demonstrates basic usage.
    /// 
    /// ```no_run
    /// use my_mini_redis::clients::Client;
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///     
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// Set `key` to hold the given `value`.
    /// 
    /// The `value` is associated with `key` until it is overwritten by the next
    /// call to `set` or it is removed.
    /// 
    /// If key already holds a value, it is overwritten. Any previous time to live
    /// associated with the key is discarded on successful SET operation.
    /// 
    /// # Examples
    /// 
    /// Demonstrates basic usage.
    /// 
    /// ```no_run
    /// use my_mini_redis::clients::Client;
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///     client.set("foo", "bar".into()).await.unwrap();
    /// 
    ///     // Getting the value immediately works
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, None)).await
    }
    /// Set `key` to hold the given `value`. The value expires after `expiration`
    ///
    /// The `value` is associated with `key` until one of the following:
    /// - it expires.
    /// - it is overwritten by the next call to `set`.
    /// - it is removed.
    ///
    /// If key already holds a value, it is overwritten. Any previous time to
    /// live associated with the key is discarded on a successful SET operation.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage. This example is not **guaranteed** to always
    /// work as it relies on time based logic and assumes the client and server
    /// stay relatively synchronized in time. The real world tends to not be so
    /// favorable.
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    /// use tokio::time;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set_expires("foo", "bar".into(), ttl).await.unwrap();
    ///
    ///     // Getting the value immediately works
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // Wait for the TTL to expire
    ///     time::sleep(ttl).await;
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    pub async fn set_expires(&mut self, key: &str, value: Bytes, expiration: Duration) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        let frame = cmd.into_frame();

        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error())
        }
    }

    /// Posts `message` to the given `channel`.
    ///
    /// Returns the number of subscribers currently listening on the channel.
    /// There is no guarantee that these subscribers receive the message as they
    /// may disconnect at any time.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.publish("foo", "bar".into()).await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        let frame = Publish::new(channel, message).into_frame();

        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /// Subscribes the client to the specified channels.
    ///
    /// Once a client issues a subscribe command, it may no longer issue any
    /// non-pub/sub commands. The function consumes `self` and returns a `Subscriber`.
    ///
    /// The `Subscriber` value is used to receive messages as well as manage the
    /// list of channels the client is subscribed to.
    #[instrument(skip(self))]
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        self.subscribe_cmd(&channels).await?;

        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Subscribe::new(channels.to_vec()).into_frame();

        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        // 对于订阅的每个频道，服务器都会回复一条确认订阅该频道的信息。
        for channel in channels {
            let response = self.read_response().await?;

            match response {
                // as_slice()返回不可变切片
                Frame::Array(ref frame) => match frame.as_slice() {
                    // 服务端用一个frame数组回复，回复格式如下：
                    //
                    // ```
                    // [ "subscribe", channel, num-subscribed ]
                    // ```
                    //
                    // 当频道名是所订阅频道名并且num-subscribed为当前订阅
                    // 这里能直接比较是因为实现了PartialEq<&str>特征
                    [subscribe, schannel, ..] if *subscribe == "subscribe"  && *schannel == channel => {},
                    _ => return Err(frame.to_error()),
                },
                frame => return Err(frame.to_error())
            };
        }

        Ok(())
    }
    /// Read a response frame from the socket.
    /// 
    /// If an `Error` frame is receive, it is converted to `Err`
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(?response);

        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // 收到`None`表示服务器已经关闭连接，并且没有发送frame。
                // 这是不可预测的并且代表”连接被对端重置“错误
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");

                Err(err.into())
            }
        }
    }
}

impl Subscriber {
    /// Returns the set of channels currently subscribed to.
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// Receive the next message published on a subscribed channel, waiting if
    /// necessary.
    /// 
    /// `None` indicates the subscription has been terminated.
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(mframe) => {
                debug!(?mframe);

                match mframe {
                    Frame::Array(ref frame) => match frame.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message{
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_stirng()),
                        })),
                        _ => Err(frame.to_error()),
                    },
                    frame => Err(frame.to_error()),
                }
            }
            None => Ok(None)
        }
    }

    /// Convert the subscriber into a `Stream` yielding new messages published
    /// on subscribed channels
    /// 将订阅者转换为 "流"，在订阅频道上发布新消息
    /// `Subscriber` does not implement stream itself as doing so with safe code
    /// is non trivial. The usage of async/await would require a manual Stream
    /// implementation to use `unsafe` code. Instead, a conversion function is 
    /// provided and the returned stream is implemented with the help of the 
    /// `async-stream` crate.
    /// 订阅者 "本身并不实现流，因为使用安全代码实现流并非易事。如果使用 async/await，
    /// 则需要手动实现流以使用`不安全`代码。取而代之的是提供一个转换函数，
    /// 并在 `async-stream` crate 的帮助下实现返回的流。
    fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        // 使用`async-stream`包中的`try_stream`宏。在Rust中
        // 生成器并不稳定。该板块使用宏来模拟 async/await 上的生成器。
        // 该宏有一些限制，请阅读相关文档。
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// Subscribe to a list of new channels
    #[instrument(skip(self))]
    pub async fn subscibe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.client.subscribe_cmd(channels).await?;
        // channels.iter().map(Clone::clone) 创建了一个新的迭代器，
        // 这个迭代器在每次迭代时都会返回 channels 中元素的一个克隆。
        self.subscribed_channels.extend(channels.iter().map(Clone::clone));
        
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Unsubscribe::new(channels).into_frame();

        debug!(request = ?frame);

        self.client.connection.write_frame(&frame).await?;

        // 如果输入channel list为空，服务器确认取消订阅所有频道
        // 所以我们断言收到的取消订阅列表和客户端订阅列表一致
        let num = if channels.is_empty() {
            self.subscribed_channels.len()
        } else {
            channels.len()
        };

        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len =  self.subscribed_channels.len();

                        if len == 0 {
                            return Err(response.to_error());
                        }

                        self.subscribed_channels.retain(|c| *channel != &c[..]);

                        if self.subscribed_channels.len() != len - 1 {
                            return Err(response.to_error());
                        }
                    }
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }
        Ok(())
    }
}