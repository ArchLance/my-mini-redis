use crate::{Connection, Db, Frame, Parse, ParseError};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

/// 设置key-value对
///
/// 如果key已经存在一个对应的value，将会被覆盖，不管其类型
/// 成功执行 SET 操作后，以前与密钥相关的任何存活时间都将被丢弃。
/// SET 操作时，与密钥相关的任何先前的存活时间都将被丢弃。
///
/// # Options
///
/// 支持两个选项：
///
/// * EX `seconds` -- 用秒设置过期时间
/// * PX `milliseconds` --  用毫秒设置过期时间
#[derive(Debug)]
pub struct Set {
    key: String,

    value: Bytes,

    expire: Option<Duration>,
}

impl Set {
    /// 创建一个新的Set命令，将key的值设置为value
    ///
    /// 如果expire是Some类型，value应该在指定duration后过期
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }
    /// 获得key
    pub fn key(&self) -> &str {
        &self.key
    }
    /// 获得值
    pub fn value(&self) -> &Bytes {
        &self.value
    }
    /// 获得过期时间
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }
    /// 从一个收到的frame中解析出Set实例
    ///
    /// Parse参数提供了一个类似于游标的 API，用于从`Frame`中读取字段。
    /// 此时，整个帧已经从socket 收到。
    ///
    /// "set"字符串已经被消费
    ///
    /// # Returns
    ///
    /// 成功返回Set实例，如果不完整返回Err
    ///
    /// # Format
    ///
    /// 希望数组Frame包含至少三个entries
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        let key = parse.next_string()?;

        let value = parse.next_bytes()?;

        let mut expire = None;

        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(EndOfStream) => {}
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire })
    }

    /// 应用Set命令到指定的Db实例
    ///
    /// 回复被写回dst，这个函数被server调用，目的是为了执行收到的命令
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.set(self.key, self.value, self.expire);

        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }
    /// 将命令转换为对等的Frame
    ///
    /// 当需要把Set命令发送给server时，这个函数被client调用
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(ms) = self.expire {
            // 这里使用px因为这允许更高的精度并且src/bin/cli.rs
            // 会将到期参数解析为毫秒，在duration_from_ms_str()函数中
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(ms.as_millis() as u64);
        }
        frame
    }
}
