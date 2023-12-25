use crate::clients::Client;
use crate::Result;

use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
// 枚举，用于将请求的命令从 "缓冲客户端 "句柄中传递出去
#[derive(Debug)]
enum Command {
    Get(String),
    Set(String, Bytes),
}

// 通过通道发送给链接任务的信息类型
// 
// `Command` is the command to forward to the connection.
//
// `oneshot::Sender` is a channel type that sends a **single** value. It is used
// here to send the response received from the connection back to the original
// requester.
type Message = (Command, oneshot::Sender<Result<Option<Bytes>>>);

/// Receive commands sent through the channel and forward them to client. The
/// response is returned back to the caller via a `oneshot`.
async fn run(mut client: Client, mut rx: Receiver<Message>) {
    // 不断从channel中弹出消息。 返回值`None`表示所有 `BufferedClient` 句柄都已经被
    // 释放，并且channel中绝不会发送其他消息。
    while let Some((cmd, tx)) = rx.recv().await {
        let response = match cmd {
            Command::Get(key) => client.get(&key).await,
            Command::Set(key, value) => client.set(&key, value).await 
        };

        // 将回复发送给调用者
        //
        // 发送信息失败表示 `rx`接收端在收到信息之前被关闭。这是很正常的运行时事件
        let _ = tx.send(response);
    }
}

#[derive(Clone)]
pub struct BufferedClient {
    tx: Sender<Message>,
}

impl BufferedClient {
    /// Create a new client request buffer
    /// 
    /// The `Client` performs Redis commands directly on the TCP connection.Only a
    /// single request may be in-flight at a given time and operations require
    /// mutable access to the `Client` handle. This prevents using a single Redis
    /// connection from multiple Tokio tasks.
    /// 客户端 "直接在 TCP 连接上执行 Redis 命令。
    /// 在给定时间内只能有一个请求在运行中，而且操作需要对 `Client` 句柄进行可变访问。
    /// 这样可以防止多个 Tokio 任务使用一个 Redis 连接。
    /// 
    /// The strategy for dealing with this class of problem is to spawn a dedicated
    /// Tokio task to manage the Redis connection and using "message passing" to 
    /// operate on the connection. Commands are pushed into a channel. The
    /// connection task pops commands off of the channel and applies them to the
    /// Redis connection. When the response is received, it is forwarded to the
    /// original requester. 
    /// 当buffer client收到Redis connection的回复后将其转发给原始请求者
    /// 
    /// The returned `BufferedClient` handle may be cloned before passing the 
    /// new handle to separate tasks.
    pub fn buffer(client: Client) -> BufferedClient {
        // 将信息数设定为固定值32. 在真实的应用中buffer的大小应该是可配置的，
        // 但是这里我们不需要这么做
        let (tx, rx) = channel(32);

        // 创建一个线程来处理对连接的请求
        tokio::spwan( async move { run(client, rx).await });

        // 返回句柄
        BufferedClient{ tx }
    }

    /// Get the value of a key.
    /// 
    /// Same as `Client::get` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        
        let get = Command::Get(key.into());

        let (tx, rx) = oneshot::channel();

        self.tx.send((get, tx)).await?;

        match rx.await {
            Ok(res) => res,
            Err(err) => Err(err.into()),
        }
    }

    /// Set `key` to hold the given `value`.
    /// 
    /// Same as `Client::set` but requests are **buffered** until the associated
    /// connection has the ability to send the request
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<()> {
        let set = Command::Set(key.into(), value);

        let (tx, rx) = oneshot::channel();

        self.tx.send((set, tx)).await?;

        match rx.await {
            Ok(res) => res.map(|_| ()),
            Err(err) => Err(err.into())
        }
    }
}