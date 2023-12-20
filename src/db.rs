use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// A wrapper around a `Db` instance. This exists to allow orderly cleanup
/// of the `Db` by signalling the background purge task to shut down when
/// this struct is dropped.
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// The `Db` instance that will be shut down when this `DbHolder` struct
    /// is dropped.
    db: Db,
}

/// Server state shared across al connections.
///
/// `Db` conntains a `HashMap` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// The shared state is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small
    ///
    /// A Tokio mutex is mostly intended to be used when locks need to be held
    /// across `.await` yield points. All other cases are **usually** best
    /// served by a std mutex. If the critical section does not include any
    /// async operations but is long (CPU intensive or performing blocking
    /// operations), then the entire operation, including waiting for the mutex,
    /// is considered a "blocking" operation and `tokio::task::spawn_blocking`
    /// should be used.
    state: Mutex<State>,

    /// Notifies the background task handling entry expiration. The background
    /// task waits on this to be notified, then checks for expired values or the
    /// shutdown signal.
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// The key-value data. We are not trying to do anything fancy so a
    /// `std::collections::HashMap` works fine.
    entries: HashMap<String, Entry>,

    /// The pub/sub key-space. Redis uses a **separate** key space for key-value
    /// and pub/sub. `mini-redis` handles this by using a separate `HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Tracks key TTLs
    ///
    /// A `BTreeSet` is used to maintain expirations sorted by when they expire.
    /// This allows the background task to iterate this map to find the value
    /// expiring next.
    ///
    /// While highly unlikely, it is possibe for more than one expiration to be
    /// created for the same instant. Because of this, the `Instant` is
    /// insufficient for the key. A unique key (`String`) is used to
    /// break these ties.
    expirations: BTreeSet<(Instant, String)>,

    /// True when the Db instance is shutting down. This happens when all `Db`
    /// values drop. Setting this to `true` signals to the background task to
    /// exit.
    shutdown: bool,
}

/// Entry in the key-value store
#[derive(Debug)]
struct Entry {
    /// Stored data
    data: Bytes,

    /// Instant at which the entry expires and should be removed from the database
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// Create a new `DbHolder`, wrapping a `Db` instance. When this is dropped
    /// the `Db`'s purge task will be shut down.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// Get the shared database. Internally, this is an
    /// `Arc`, so a clone only increments the ref count.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 向`Db`实例发送信号，关闭清除过期密钥的任务
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Create a new, empty, `Db` instance. Allocates shared state and spawn a
    /// background task to manage key expiration.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if there is no value associated with the key. This may be
    /// due to never having assigned a value to the key or previously assigned
    /// value expired.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 需要先获得锁， 拿到entry并clone
        //
        // 由于数据用`Bytes`存储，clone is shallow clone
        // 数据并没有被copied
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Set the value associated with a key along with an optional expiration
    /// Duration.
    ///
    /// If a value is already associated with the key,it is removed.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // If this `set` becomes the key that expires **next**, the background
        // task needs to be notified so it can update its state.
        //
        // Whether or not the task needs to be notified is computed during the
        // `set` routine
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

            // state.next_expiration()获取当前等待过期的第一个entry的时间戳when。
            // map函数将新entry的过期时间when与最近一个要过期的entry的expiration进行比较。
            // 如果expiration更大,说明新entry是下一个过期的,返回true。
            // 否则expiration小于或等于when,返回false。
            // unwrap_or(true)是为了处理next_expiration()可能返回None的情况,
            // 如果是None，证明set中没有即将过期的entry，则直接返回true。
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });
        //state.entries是一个HashMap,键是String,值是Entry结构。
        //当调用insert方法向HashMap插入一对键值对时,如果该键之前存在,insert方法会返回之前的值。
        //如果键不存在,insert方法会返回None。
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // 如果之前有值，则需要讲之前的key从set也就是expirations中移除，避免缺少数据
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // key 后面要用所以不能将所有权给元组
                state.expirations.remove(&(when, key.clone()));
            }
        }
        // 如果在插入前删除在(when, key)相等时会造成bug
        //
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // 在唤醒任务之前释放锁，这样可以使得任务被唤醒就可以拿到锁，
        // 而不是被唤醒后等待当前作用域释放锁
        drop(state);

        if notify {
            // 如果当前任务需要被唤醒，则唤醒任务
            self.shared.background_task.notify_one();
        }
    }

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH`
    /// commands
    pub(crate) fn subscibe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        let mut state = self.shared.state.lock().unwrap();

        // 如果当前请求channel中没有entry，那么创建一个新的broadcast channel 并且将其和key联系起来
        // 如果已经存在了，那么返回一个已经和key联系起来的receiver
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // 一个成功在broadcast channel上发送的message，订阅者的数量被返回
            // 一个错误表示这里没有接受者，在这种情况下应该返回0
            .map(|tx| tx.send(value).unwrap_or(0))
            // 如果当前key没有相应的entry， 所以这里也是没有订阅者，所以也返回0
            .unwrap_or(0)
    }

    /// Signals the purge background task to shut down. This is called by the
    /// `DbShutdown`s `Drop` implementation
    fn shutdown_purge_task(&self) {
        // 后台任务必须被告知关闭，这个件事通过将`State::shutdown` to  `true` 并且告知task
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // 同样在notify task之前先drop锁，使得任务不用等待
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the **next**
    /// key will expire. The background task will sleep until this instant
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // db正在关闭，所有handles to the stared state已经释放。
            // 后台任务应该退出
            return None;
        }

        //关于 lock() 方法： 在 Rust 中，当你使用一个互斥锁（Mutex）来保护共享数据时，
        //你通常会调用 lock() 方法来访问这些数据。调用 lock() 会返回一个 MutexGuard，
        //这是一个智能指针，它提供对被互斥锁保护的数据的访问。
        //MutexGuard 和借用检查器： 当你持有一个 MutexGuard，你实际上持有对受保护数据的独占访问权。
        //但是，Rust 的借用检查器有时不能完全理解 MutexGuard 背后的复杂性。
        //特别是当你尝试在同一个作用域中访问同一个互斥锁保护的多个不同字段时，
        //借用检查器可能会错误地认为这造成了数据竞争。
        //解决方案 - 在循环外获取“真实”可变引用： 为了解决这个问题，注释中提到的方法是
        //在循环之外获取对 State 的一个“真实”可变引用。这意味着你先锁定互斥锁，
        //然后在进入循环之前获取一个对受保护数据的可变引用。
        //这样做可以确保借用检查器能够正确地理解你在循环中对这些数据的访问是安全的。
        let state = &mut *state;

        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }
        None
    }
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task
///
/// Wait to be notified. On notification, purge any expired keys from the shared
/// state handle. If `shutdown` is set, terminate the task
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 如果shutdown 标志被设置， 任务应该退出
    while !shared.is_shutdown() {
        // 清除所有过期的key,这个方法返回了下一个key过期的时间
        // 工作器需要等到下一个过期的时间到，之后再次清除
        if let Some(when) = shared.purge_expired_keys() {
            // 等待直到下一个key过期或者直到后台任务被唤醒。如果任务被唤醒，
            // 它必须重新加载状态就像新key被设置为提前到期，这个通过循环来做
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 将来没有key过期，等待任务被唤醒
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}
