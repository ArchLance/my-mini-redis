use tokio::sync::broadcast;

/// Listen for the server shutdown signal.
///
/// Shutdown is signalled using a `broadcast::Receiver`. Only a single value is
/// ever sent. Once a value has been sent via the broadcast channel, the server
/// should shutdown.
///
/// The `Shutdown` struct listens for the signal and tracks that the signal has
/// been received. Callers may query for whether the shutdown signal has been
/// received or not.
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received
    is_shutdown: bool,

    /// The receive half of the channel used to listen for shutdown
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub(crate) async fn recv(&mut self) {
        // 如果关闭信号已经收到，则直接返回
        if self.is_shutdown {
            return;
        }
        // 无法接收 "滞后错误"，因为只发送一个值。
        // Cannot receive a "lag error" as only one value is ever sent.
        // 详情见https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
        let _ = self.notify.recv().await;

        self.is_shutdown = true;
    }
}
