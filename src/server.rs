//! Minimal Redis server 实现
//!
//! 提供一个异步的 run 函数，用于监听入站连接。为每一个连接生成任务。

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// Server listener的状态定义 在run调用中创建。
/// 它包含一个run方法，该方法执行TCP监听和每个连接状态的初始化。
#[derive(Debug)]
struct Listener {
    /// 共享数据库句柄。包含键/值存储以及用于发布/订阅的广播通道。
    ///
    /// 这是对Arc类型的一个包装,
    /// 内部的Db类型可以被检索并且传入每个连接状态（Handler）
    db_holder: DbDropGuard,

    ///由`run`调用者提供的Tcp监听器。
    listener: TcpListener,

    /// 限制最大连接数量
    ///
    /// Semaphore "用于限制最大连接数。
    /// 在尝试接受新连接之前，会从 Semaphore 获取许可。
    /// 如果没有，监听器就会等待一个。
    ///
    /// 当处理程序完成对连接的处理后，许可证就会返回到 semaphore。
    /// 我目测就是分发token来完成对连接限制
    limit_connections: Arc<Semaphore>,

    /// 广播一个关闭信号给所有连接
    ///
    /// 初始的关闭触发器由run函数的调用者提供。服务器负责优雅的关闭活动连接。
    /// 生成连接器任务时，会向其传递一个广播接收器句柄。启动优雅关闭时
    /// 会通过broadcast::Sender发送一个`()`值。每个活动连接都会收到该值，
    /// 进入安全终端状态，并完成任务
    notify_shutdown: broadcast::Sender<()>,

    /// 作为优雅关机流程的一部分，用于等待客户端 连接完成处理。
    ///
    /// 一旦所有 “发送方 ”句柄退出作用域，Tokio 通道就会关闭。
    /// 通道关闭时，接收方会收到None。利用这一点可以监测所有连接处理程序是否完成。
    /// connection handler被初始化时，会分配一个shutdown_complete_tx的克隆
    /// 当所有handler任务完成，所有clone的Sender都会被销毁dropped
    /// 这会导致 shutdown_complete_rx.recv() 以None完成。
    /// 这时候可以安全的退出server进程
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// 每个连接处理程序。读取来自 `connection` 的请求并将指令应用到Db中
#[derive(Debug)]
struct Handler {
    /// 共享数据库句柄 handle
    ///
    /// 当一个命令从连接（connection）中接收，他会被应用到db
    /// 命令的实现在 cmd 模块中，每一个命令都需要与`db`交互才能完成工作
    db: Db,

    /// 使用buffered `TcpStream` 实现的Redis协议编码器/解码器装饰的Tcp连接
    ///
    ///
    /// 当监听器接收到入站连接时， `TcpStream` 会被传递给"Connection::new"
    /// 后者会初始化相关的缓冲区。Connection允许handler处理程序在“帧”级别操作，并
    /// 保留Connection中封装的字节级协议解析细节
    connection: Connection,

    /// 监听关闭信号
    ///
    /// 内部结构为与在Listener中的Sender配对的broadcast::Receiver。
    /// connection handler处理来自connection的请求直到对端断开连接或者
    /// 收到了从Sender处发送的关闭信号
    /// 在后一种情况下，任何正在为对等端进行的工作都会继续，直到其达到了安全的状态，这时
    /// 连接才会关闭
    shutdown: Shutdown,

    /// 没有被直接使用，但是当Handler被销毁dropped时候，这个clone也会被销毁
    _shutdown_complete: mpsc::Sender<()>,
}

/// redis 服务器接受的最大并发连接数。达到此限制后，服务器将停止接受连接，
/// 直到活动连接终止。实际应用需要对该值进行配置，但在本示例中，该值是硬编码的。
const MAX_CONNECTIONS: usize = 250;

/// 运行mini-redis server
///
/// 接受来自提供的监听器的连接。对于每个入站连接，都会生成一个任务来处理该连接。
/// 服务器会一直运行到 “shutdown ”未来任务完成，此时服务器会以 优雅地关闭。
///
/// 可以使用 `tokio::signal::ctrl_c()` 作为 `shutdown` 参数。这将 监听 SIGINT 信号。
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
    let Listener {
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
    /// 运行服务
    ///
    /// 监听接入的连接，对于每一个接入的连接，分配一个任务来处理连接
    ///
    /// # 错误
    ///
    /// 返回`Err` 如果accepting返回一个错误。出现这种情况的原因有很多，
    /// 但随着时间的推移都会得到解决。例如，如果底层操作系统已达到
    /// 最大套接字数量的内部限制，接受就会失败。套接字的内部限制，accepting就会失败。
    ///
    /// 程序无法检测瞬时错误何时自行解决。
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

            // 创建一个新任务来执行连接。Tokio 任务就像异步绿色线程，并发执行。
            // 使用多线程 + io多路复用
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

    /// 接收一个连接请求
    ///
    /// 错误通过退避策略和重试来处理。采用指数退避策略。在第一次失败后，任务等待1秒。
    /// 在第二次失败后，任务等待2秒。每次后续失败都会使等待时间翻倍。
    /// 如果在第6次尝试后接受连接仍然失败这个函数返回失败
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

impl Handler {
    /// 处理一个单独的连接
    ///
    /// 请求帧从套接字读取并处理。响应被写回套接字
    ///
    /// 目前，流水线功能尚未实现。流水线处理是指在每个连接上同时处理多个请求，
    /// 而不需要交错帧的能力。更多详情请参见
    /// http://redis.io/topics/pipelining
    ///
    /// 收到关闭信号后，连接将被处理，直到达到安全状态，然后终止。
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

            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }
        Ok(())
    }
}
