use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// 解析命令的工具
///
/// 命令以数组框架的形式表示。框架中的每个条目都是一个 "token"
/// 一个`Parse`会使用数组Frame进行初始化，并提供cursor like api
/// 每一个命令结构体包含一个`parse_frame`方法，该方法使用`Parse`来提取命令字段
#[derive(Debug)]
pub(crate) struct Parse {
    /// Array frame 迭代器
    parts: vec::IntoIter<Frame>,
}

/// 解析帧时遇到错误
///
/// 运行时只处理 `EndOfStream` 错误。所有其他错误都会导致连接终止。
#[derive(Debug)]
pub(crate) enum ParseError {
    /// 由于帧已完全耗尽，提取值的尝试失败
    EndOfStream,

    /// 其他错误
    Other(crate::Error),
}

impl Parse {
    /// 创建一个新的 `Parse` 来解析 `frame` 的内容、
    ///
    /// 返回Err如果不是Array Frame
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(arr) => arr,
            other => return Err(format!("protocol error; expected array, got {:?}", other).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }
    /// 返回下一个条目。数组帧是帧的数组，因此下一个条目是一个frame
    pub(crate) fn next(&mut self) -> Result<Frame, ParseError> {
        // ok_or()直接返回一个静态默认值。
        // ok_or_else()可以通过闭包产生默认值,支持更复杂的错误处理逻辑。
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// 以字符串形式返回entry
    ///
    /// 如果下一个entry不能表示字符串则返回错误
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error: invalid string".into()),
            other => Err(format!(
                "protocol error: expected simple frame or bulk frame, get {:?}",
                other
            )
            .into()),
        }
    }

    /// 以原始字节形式返回下一个条目。
    ///
    ///  如果下一个条目不能以原始字节表示，则返回错误信息
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            // todo 这里Integer是不是也能解析成bytes啊
            // 自己添加的不知道能不能用到
            Frame::Integer(num) => Ok(Bytes::from(num.to_be_bytes().to_vec())),
            other => Err(format!(
                "protocol error: expected simple frame or bulk frame, got {:?}",
                other
            )
            .into()),
        }
    }

    /// 以整数形式返回下一个条目。
    ///
    ///  这包括 `Simple`、`Bulk` 和 `Integer` 帧类型。
    ///
    /// 如果不能表示为一个整数则返回错误
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;
        match self.next()? {
            Frame::Simple(s) => {
                atoi::<u64>(s.as_bytes()).ok_or_else(|| "protocol error: invalid number".into())
            }
            Frame::Bulk(data) => {
                atoi::<u64>(&data).ok_or_else(|| "protocol error: invalid number".into())
            }
            Frame::Integer(num) => Ok(num),
            other => Err(format!("protocol error; expected int frame but got {:?}", other).into()),
        }
    }

    /// 确保数组中不再有条目
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            return Ok(());
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error: unexpected end of stream".fmt(f),
            ParseError::Other(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
