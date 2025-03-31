use tokio::sync::broadcast;

/// 监听服务器关闭信号
///
///
/// 关机信号使用 `broadcast::Receiver`。只发送一个值。
/// 一旦通过广播通道发送了一个值，服务器应该关闭。
///
/// The `Shutdown` 结构监听信号，并跟踪信号是否已收到。
/// 调用者可以查询是否已收到关闭信号。
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` 如果关闭信号已经被收到
    is_shutdown: bool,

    /// 用于监听关机的接收半信道。
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// 使用所传入的`broadcast::Receiver`创建一个新的 `Shutdown` 对象
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// 返回 `true` 如果关闭信号被接收
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// 接收关闭信号，如果有必要等待的话
    pub(crate) async fn recv(&mut self) {
        // 如果关闭信号已经被接收，直接返回
        if self.is_shutdown {
            return;
        }

        // 无法接收lag error因为只发送一个值
        let _ = self.notify.recv().await;

        // 记录关闭信号被接收
        self.is_shutdown = true;
    }
}
