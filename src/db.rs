use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// `Db` 实例的包装器。它的存在是为了在删除该结构体时，
/// 通过向后台清除任务发出关闭信号，有序地清理 `Db`。
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// 当删除此 `DbHolder` 结构时将关闭的 `Db` 实例。
    db: Db, // Db: Arc<Shared>  Shared: {Mutex<State>, Notify}
}

/// 对所有连接共享的服务端状态
///
/// `Db`包含一个存储键值对数据的`HashMap`和一个包含所有活跃的pub/sub channel的
/// `broadcast::Sender`的`HashMap`
///
/// `Db`实例是共享状态的句柄。克隆`Db`是浅拷贝，只会使得原子ref计数递增
///
/// 当创建一个`Db`值时，会生成一个后台任务。该任务用于在请求的持续时间结束后
/// 使值过期。该任务一直运行到`Db`的所有实例被销毁，然后任务终止
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// 共享状态的句柄。后台任务也会有一个`Arc<Shared>`
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// 共享状态由一个Mutex保护的。这是一个std::sync::Mutex
    state: Mutex<State>,

    /// 通知处理entry的后台任务到期。后台任务等待通知，然后检查过期值或关闭信号。
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// 保存键值对
    entries: HashMap<String, Entry>,

    /// 用来存储消息pub发布端
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// 追踪键的过期时间
    ///
    ///  一个 `BTreeSet` 用于维护按过期时间排序的过期值。
    /// 这样，后台任务就可以遍历该映射，找到下一个到期的值。
    ///
    /// 虽然可能性极小，但是还是有可能出现多个key同一个过期时间
    /// 所以用过期时间Instant和key的元组来解决
    expirations: BTreeSet<(Instant, String)>,

    /// 当Db实例被关闭时为True。当所有`Db`值都下降时就会发生这种情况。将此设置为`True`
    /// 则向后台任务发出退出信号
    shutdown: bool,
}

/// key-value中的value，用来保存底层字节表示
#[derive(Debug)]
struct Entry {
    /// 存储数据
    data: Bytes,

    /// 条目过期并应该从数据库中删除的时间
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// 创建一个新的 `DbHolder`，封装一个 `Db` 实例。
    /// 当该实例被删除时 将关闭 `Db` 的清除任务。
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// 自动派生的 Clone 实现会对结构体的每个字段调用 clone()。
    /// Db 结构体只有一个字段 shared，类型是 Arc<Shared>。
    /// 而 Arc 的 clone() 实现只会增加引用计数，而不会复制底层数据。
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
    /// 创建一个新的空的`Db`实例，为共享状态开辟空间，创建一个后台任务来处理key过期
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

        // 开始后台清理过期key任务
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// 得到key的相关value值
    ///
    /// 如果键没有相关联的值，则返回 `None`。
    /// 这可能是由于从未为键赋值或先前赋值的值已过期。
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 需要先获得锁， 拿到entry并clone
        //
        // 由于数据用`Bytes`存储，clone is shallow clone
        // 数据并没有被copied
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 设置一个key-value对，并可以自由设置过期时间，如果一个value已经和一个key关联
    /// 它将被移除
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // 如果该`set`称为下一个过期的key，则需要通知后台任务，以更新其状态。
        // 在set函数中计算是否需要通知后台清理任务
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // 计算过期时间
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

        // 如果之前有值，则需要将之前的key从set也就是expirations中移除，避免缺少数据
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // key 后面要用所以不能将所有权给元组
                state.expirations.remove(&(when, key.clone()));
            }
        }
        // 插入新的过期时间
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

    /// 给请求的channel返回一个broadcast::Receiver<Bytes>
    ///
    /// 返回的`Receiver`被用来接收`PUBLISH`命令广播的值
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

    /// 向频道发布一条信息。返回订阅者的数量
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

    /// 关闭后台清理任务，这个函数由
    /// `DbDropGuard`s `Drop` implementation
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
    /// 清除所有过期密钥，并返回下一个密钥过期的 “时刻”。
    /// 后台任务将休眠至此时刻
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
