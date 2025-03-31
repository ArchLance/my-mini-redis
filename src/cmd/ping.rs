use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::{debug, instrument};

/// 如果没有提供参数，则返回 PONG，否则返回参数bulk类型的副本
///
/// 该命令通常用于测试连接是否仍然有效，或测量延迟
#[derive(Debug, Default)]
pub struct Ping {
    /// 一个可选的msg被返回
    msg: Option<Bytes>,
}

impl Ping {
    /// 创建一个带有optional msg的Ping command
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    /// 从接收到的frame解析出Ping实例
    ///
    /// Parse参数提供了一个类似于游标的 API，用于从`Frame`中读取字段。
    /// 此时，整个帧已经从socket中收到。
    ///
    /// "ping" 字符串已经被消费了
    ///
    /// # Returns
    ///
    /// 成功则返回`ping`的值，如果frame不完整返回Err
    ///
    /// # Format
    ///
    /// 希望一个数组Frame包含ping和optional msg
    ///
    /// ```text
    /// ping [message]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// 应用Ping命令并返回信息
    ///
    /// 回复被写回dst，这个函数被server调用，目的是为了执行收到的命令
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将命令转换为对等的Frame
    ///
    /// 当需要把Ping命令发送给server时，这个函数被client调用
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }
        frame
    }
}
