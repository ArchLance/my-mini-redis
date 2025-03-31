use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

/// 获取key对应value
///
/// 如果key不存在，特殊值nil将会被返回。一个错误被返回，如果key不是一个字符串
/// 因为GET值接收字符串
#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    /// 创建一个新的Get command来持有key
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }
    /// 获得key
    pub fn key(&self) -> &str {
        &self.key
    }
    /// 将接收到的frame解析成Get命令
    ///
    /// Parse类型供了一个类似于cursor的 API，用于从 Frame 中读取字段。
    /// 此时，已从套接字接收到整个帧。
    ///
    /// `get`字符串已经被消费了
    ///
    /// # Returns
    ///
    /// 返回得到的value，如果frame不完整则返回Err
    ///
    /// # Format
    ///
    /// 希望一个数组帧包含下面两个entry
    ///
    /// ```text
    /// get key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    /// 将Get命令用到Db的接口中
    ///
    /// 结果被写回dst，这个函数会被server调用，目的是为了执行接收到的command
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };

        debug!(?response);
        // 将回应写回客户端
        dst.write_frame(&response).await?;

        Ok(())
    }
    /// 将command转换为一个对等的Frame
    ///
    /// 这个被client调用，当需要将Get命令发送到server
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
