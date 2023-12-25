//! Minimal blocking Redis client implementation
//! 
//! Provides a blocking connection and methods for issuing the supported commands.

use bytes::Bytes;
use std::time::Duration;
// ToSocketAddrs trait
// 为对象提供了将自身转换为一系列 socket 地址的能力。
// 这在处理网络编程时非常有用，因为它允许使用各种类型来表示网络地址，
// 如字符串或 (host, port) 对。例如，一个实现了 ToSocketAddrs 的字符串
// 可以直接用于指定网络连接的目标地址。
use tokio::net::ToSocketAddrs;
use tokio::runtime::Runtime;

pub use crate::clients::Message;

/// Established connection with a Redis server.
/// 
/// Backed by a single `TcpStream`, `BlockingClient` provides basic network
/// client functionality (no pooling, retrying, ..). Connections are 
/// established using the [`connect`](fn@connect) function.
/// 
/// Requests are issued using the various methods of `Client`.
pub struct BlockingClient {
    /// The asynchronous `Client`,
    inner: crate::clients::Client,

    /// A `current_thread` runtime for executing operations on the asynchronous
    /// client in a blocking manner.
    /// 用于在异步客户端上以阻塞方式执行操作的 "current_thread runtime".
    rt: Runtime,
}

/// A client that has entered pub/sub mode.
/// 
/// Once client subscribe to a channel, they may only perform pub/sub related
/// commands. The `BlockingClient` type is transitioned to a `BlockingSubscriber`
/// type in order to prevent non-pub/sub methods from being called.
pub struct BlockingSubscriber {
    /// The asynchronous `Subscriber`,
    inner: crate::clients::Subscriber,

    /// A `current_thread` runtime for executing operatioins on the asynchronous
    /// `Subscriber` in a blocking manner.
    rt: Runtime,
}

/// The iterator returned by `Subscriber::into_iter`.
struct SubscriberIterator {
    /// The asynchronous `Subscriber`,
    inner: crate::client::Subscriber,

    /// A `current_thread` runtime for executing operations on the asynchronous
    /// `Subscriber` in a blocking manner.
    rt: Runtime,
}

impl BlockingClient {
    /// Establish a connection with the Redis server located at `adder`
    /// 
    /// `addr` may be any type that can be asynchronously converted to a
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
    /// traits is the Tokio version and not the `std` version
    /// 
    /// # Examples
    /// 
    /// Demonstrates basic usage.
    /// 
    /// ```no_run
    /// use my_mini_redis::clients::BlockingClient;
    /// 
    /// fn main() {
    ///     let mut client = match BlockingClient::connect("localhost:6379") {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);  
    /// }
    /// ```
    pub fn connect<T: ToSocketAddr>(addr: T) -> crate::Result<BlockingClient> {
        // 这里，tokio::runtime::Builder::new_current_thread() 创建了一个 Builder 实例，
        //用于构造一个新的运行时。new_current_thread 表示这个运行时是基于当前线程的。
        //这意味着所有由这个运行时驱动的异步任务都将在创建它的同一线程上执行。
        //这种类型的运行时通常用于应用程序中的单线程场景，其中任务不会被跨多个线程分配。
        let rt = tokio::runtime::Builder::new_current_thread()
            // 这一行调用 enable_all() 函数来启用 tokio 运行时的所有特性。
            //这包括各种 I/O 和时间驱动的功能，例如网络和定时器等。
            .enable_all()
            //build() 方法尝试创建运行时。如果成功，它返回 Ok(Runtime)，
            //否则返回一个 Err。这里使用 ? 后缀，它是 Rust 中的错误传播运算符。
            //如果 build() 返回 Err，那么错误将从当前函数返回，
            //并提早结束函数执行。如果返回 Ok，则 rt 变量将包含运行时实例。
            .build()?;
        //block_on 是运行时上的一个方法，用于运行一个 Future 并等待它完成。
        //这个调用会阻塞当前线程，直到 Future 完成为止。
        //在这个例子中，block_on 方法被用来执行 Client::connect 函数并等待其结果。
        let inner = rt.block_on(crate::clients::Client::connect(addr))?;

        Ok(BlockingClient { inner, rt })
    }
    /// Get the value of key
    /// 
    /// If the key does not exist the special value `None` is returned.
    /// 
    /// # Example
    /// 
    /// Demonstrates basic usage.
    /// 
    /// ```no_run
    /// use my_mini_redis::clients::BlockingClient;
    /// 
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    /// 
    ///     let val - client.get("foo").unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
    }

    /// Set `key` to hold the given `value`.
    /// 
    /// The `value` is associated with `key` until it is overwritten by the next
    /// call to `set` or it is removed.
    /// 
    /// If key already holds a value, it is overwritten. Any previous time to
    /// live associated with the key is discarded on successful SET operation.
    /// 
    /// # Examples
    /// 
    /// Demonstrates basic usage.
    /// 
    /// ```no_run
    /// use my_mini_redis::clients::BlockingClient;
    /// 
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    /// 
    ///     client.set("foo", "bar".into()).unwrap();
    /// 
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    pub fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()>{
        self.rt.block_on(self.inner.set(key, value))
    }

    /// Set `key` to hold the given `value`. The value expires after `expiration`
    /// 
    /// The `value` is associated with `key` until one of the following:
    /// - it expires
    /// - it is overwritten by the next call to `set`
    /// - it is removed
    /// 
    /// If key already holds a value, it is overwritten. Any previous time to
    /// live associated with the key is discarded on a successful SET operation.
    /// 
    /// # Example
    /// 
    /// Demonstrates basic usage. This example is not **guaranteed** to always
    /// work as it relies on time based logic and assumes the client and server
    /// stay relatively sychronized in time. The real world tends to not be so 
    /// favorable
    /// 演示基本用法。这个示例并不能保证总是有效，因为它依赖于基于时间的逻辑，
    /// 并假设客户端和服务器在时间上保持相对同步。实际情况往往并非如此
    /// 
    /// ```no_run
    /// use my_mini_redis::clients::BlockingClient;
    /// use std::thread;
    /// use std::time::Duration;
    /// 
    /// fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    /// 
    ///     client.set_expires("foo", "bar".int(), ttl).unwrap();
    /// 
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_wq(val, "bar");
    /// 
    ///     thread::sleep(ttl);
    /// 
    ///     let val = client.get("foo").unwrap();
    ///     assert!(val.is_some());
    /// }
    pub fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expirationis: Duration,
    ) -> crate::Result<()> {
        self.rt
            .block_on(self.inner.set_expires(key, value, expirationis))
    }
    /// Posts `message` to the given `channel`.
    /// 
    /// Returns the number of subscribers currently listening on the channel.
    /// There is on guarantee that these subscribers receive the message as they
    /// may disconnect at any time.
    /// 
    /// # Example
    /// 
    /// Demonstrates basic usage.
    /// 
    /// ```no_run
    /// use my_mini_redis::clients::BlockingClient;
    /// 
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    /// 
    ///     let val = client.publish("foo", "bar".into()).unwrap();
    ///     println!("Got = {:?}", val);   
    /// }
    /// ```
    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }

    /// Subscribes the client to the specified channels.
    /// 
    /// Once a client issues a subscribe command, it may no longer issue any
    /// no-pub/sub commands. The function consumes `self` and returns a 
    /// `BlockingSubscriber`.
    /// 
    /// The `BlockingSubscriber` value is used to receive messages as well as
    /// manage the list of channels the client is subscribed to.
    pub fn subscribe(self, channels: Vec<String>) -> crate::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;
        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

impl BlockingSubscriber {
    /// Returns the set of channels currently subscribed to.
    pub fn get_subscribed(&self) -> &[String] {
        self.inner.get_subscribed()
    }

    /// Receive the next message published on a subscribed channel, waiting if 
    /// necessary.
    /// 
    /// `None` indicates the subscription has been terminated.
    pub fn next_message(&mut self) -> crate::Result<Option<Message>> {
        self.rt.block_on(self.inner.next_message())
    }

    /// Convert the subscriber into an `Iterator` yielding new messages published
    /// on subscribed channels.
    pub fn into_iter(self) -> impl Iterator<Item = crate::Result<Message>> {
        SubscriberIterator {
            inner: self.inner,
            rt: self.rt,
        }
    }

    /// Subscribe to a list of new channels
    pub fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.subscibe(channels))
    }

    /// Unsubscribe to a list of new channels
    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.unsubscribe(channels))
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::Result<Message>;
    // transpose() 是Rust标准库中的一个方法，通常用于处理 Option<Result<T, E>> 
    //或 Result<Option<T>, E> 类型的值。在这个上下文中，它可能被用来转换 
    //Option<Future<...>> 成 Future<Option<...>>，或者执行类似的转换。
    fn next(&mut self) -> Option<crate::Result<Message>> {
        self.rt.block_on(self.inner.next_message()).transpose()
    }
}