//! Minimal Redis server implementation
//! 
//! Provides an async `run` function that listens for inbound connections,
//! spwaning a task per connection.

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// Shared databases handle.
    /// 
    /// Contains the key / value stores as well as the broadcast channels 
    /// for pub/sub
    /// 
    /// This holds a wrapper around an `Arc`. The internal `Db` can be 
    /// retrieved(检索) and passed into the per connection state (`Handler`).
    db_holder: DbDropGuard,

    /// Tcp listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections.
    /// 
    /// A `Semaphore` is used to limit the max number of connections.
    /// Before attemptting to accept a new connection, a permit is 
    /// acquired from the semaphore. If none are available, the listener
    /// waits for one.
    /// 
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,

    /// Broadcasts a shutdown signal to all active connections.
    /// 
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shut down is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a 
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    /// 
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is 
    /// leveraged to detect all connection handlers completing(利用这一点可以监测
    /// 所有连接处理程序是否完成) When a connection handler is initialized, it is
    /// assigned a clone of `shutdown_complete_tx`.When the listener shuts down
    /// it drops the sender held by this `shutdown_complete_tx` field. Once all 
    /// handler tasks complete, all clones of the `Sender` are also dropped. 
    /// This results in `shutdown_complete_rx.recv()` completing with `None`. At
    /// this point, it is safe to exit the server process.
    shutdown_complete_tx: mpsc::Sender<()>
}

/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`
#[derive(Debug)]
struct Handler {
    /// Shared database handle.
    /// 
    /// When a command is received from `connection`, it is applied with `db`.
    /// The implementationi of command is in the `cmd` module. Each command
    /// will need to interact with `db` in order to complete the work.
    db: Db,

    /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`
    /// 
    /// When `Listener` receives an inbound connection, the `TcpStream` is 
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated(封装) in `Connection`.
    connection: Connection,

    /// Listen for shutdown notifications.
    /// 
    ///  A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    /// 
    /// 在后一种情况下，任何正在为对等端进行的工作都会继续，直到其达到了安全的状态，这时
    /// 连接才会关闭
    shutdown: Shutdown,

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,

}

/// Maximum number of concurrent connections the redis server will accept.
/// 
/// When this limit is reached, the server will stop accepting connections until
/// an active connection terminates.
/// 
/// A real application will want to make this value configurable, but for this 
/// example, it is hard coded.
/// 
/// This is also set tot a pretty low value to discourage using this in 
/// production (you'd think that all the disclaimers would make it obvious that
/// this is not a serious project.. but I thought that about mini-http as well).
const MAX_CONNECTIONS: usize = 250;

/// Run the mini-redis server.
/// 
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
/// 
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 当提供的`shutdown` future完成，我们必须给所有活跃连接发送一个关闭信号
    // 为了这个目的我们使用一个 broadcst channel。
    // 下面的调用无视了broadcast pair中的接收者，当接收者被需要时，
    // 使用subscribe()方法创建一个接收者
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    // 初始化Listener
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // 同时运行server并监听 `shutdown` 信号。server task 直到遇到错误发生
    // 才会停止， 所以正常情况下的循环，这个 `select!` 语句直到收到
    // `shutdown`信号才会停止
    //
    // `select!` 语句的格式如下
    //
    // ```
    // <result of async op> = <async op> => <step to perform with result>
    // ```
    // 
    // 所有 `<async op>` 语句都会被同时执行。只要第一个操作完成，其所关联的
    // `<step to perform with result>` 会被执行。
    //
    // `select!`宏是异步 Rust 的基础构件。更多详情，请参阅 API 文档：
    // https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // 这里如果收到了一个错误，Tcp listener多次建立连接失败，
            // 服务端就会放弃连接并关闭
            //
            // 处理单个连接时遇到的错误不会到此为止
            if let Err(err) = res {
                error!(cause = &err, "failed  to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    // 明确提取 `shutdown_complete` 接收器和发射器，删除 `shutdown_transmitter`。
    // 这是重要的，否则下面的await将永远不会完成
    let Listener{
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // 当`notify_shutdown`被drop，所有有订阅端的都会收到shutdown信号并且退出
    drop(notify_shutdown);
    // Drop 最后的`Sender`，以至于下面的`Receiver`可以完成
    drop(shutdown_complete_tx);

    // 等待所有活跃连接执行结束。当listenr中的`Sender`句柄在上面被drop，仅剩的
    // `Sender`由连接处理程序持有。当他们drop时，`mpsc` channel 将会关闭并且
    // `recv()`会返回`None`。
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// Run the server
    /// 
    /// Listen for inbound connection. For each inbound connection, spawn a
    /// task to process that connection.
    /// 
    /// # Errors
    /// 
    /// Return `Err` if accepting returns an error. This can happen for a 
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    /// 
    /// The process is not able to detect when a transient error resolves
    /// itself(程序无法检测瞬时错误何时自行解决。). One strategy for handling this
    /// is to implement a back off strategy, which is what we do here.
    /// 处理这种情况的一种策略是实施后退策略，我们在这里就是这样做的。
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // 等待permit变得空闲
            // 
            // `acquire_owned` 返回绑定到semaphore的permit
            // 当permit的值被dropped,它会自动返回semaphore
            //
            // 当semaphore被关闭时`acquire_owned()` 返回`Err`.
            // 我们永远不会关闭semaphore，所以`unwrap()`是安全的
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            // 接收一个新的socket。这将会尝试执行错误处理。
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.(没看懂)
            let socket = self.accept().await?;

            // 为每一个连接创建必要的处理程序状态
            let mut handler = Handler {
                db: self.db_holder.db(),

                connection: Connection::new(socket),

                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // 创建一个新任务来执行连接。Tokio 任务就像 异步绿色线程，并发执行。
            tokio::spawn(async move {
                // 执行连接，如果遇到错误，打log
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                // 将permit移动到任务中，当完成时将其drop。
                // 会将permit返回给semaphore
                drop(permit);
            });
        }
    }

    /// Accept an inbound connection.
    /// 
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after 
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        loop {
            // 执行建立连接操作。如果一个socket被成功接收了，返回这个socket
            // 否则保存错误
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;

            backoff *= 2;

        }
    }
}

impl  Handler {
    /// Process a single connection
    /// 
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket
    /// 
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// zzh_todo()
    /// http://redis.io/topics/pipelining
    /// 
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let cmd = Command::from_frame(frame)?;

            debug!(?cmd);

            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        }
        Ok(())
    }
}