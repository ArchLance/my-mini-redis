use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// Utility for parsing a command
///
/// Commands are represented as array frames. Each entry in the frame is a
/// "token". A `Parse` is initialized with the array frame and provides a
/// cursor-like API. Each command struct includes a `parse_frame` method that
/// uses a `Parse` to extract its fields
#[derive(Debug)]
pub(crate) struct Parse {
    /// Array frame iterator
    parts: vec::IntoIter<Frame>,
}

/// Error encountered while parsing a frame
///
/// Only `EndOfStream` errors are handled at runtime. All other errors result in
/// the connection being terminated.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// Attempting to extract a value failed due to the frame being fully consumed
    EndOfStream,

    /// All other errors
    Other(crate::Error),
}

impl Parse {
    /// Create a new `Parse` to parse the contents of `frame`,
    ///
    /// Returns `Err` if `frame` is not an array Frame
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(arr) => arr,
            other => return Err(format!("protocol error; expected array, got {:?}", other).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }
    /// Return the next entry. Array frame are array of frames, so the next
    /// entry is a frame
    pub(crate) fn next(&mut self) -> Result<Frame, ParseError> {
        // ok_or()直接返回一个静态默认值。
        // ok_or_else()可以通过闭包产生默认值,支持更复杂的错误处理逻辑。
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// Return the entry as a string
    ///
    /// If the next entry cannot be represented as a String, then an error is returned.
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

    /// Return the next entry as raw bytes.
    ///
    /// If the next entry cannot be represented as raw bytes, an error is returned
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

    /// Return the next entry as an integer.
    ///
    /// This include `Simple`, `Bulk` and `Integer` frame types.
    /// `Simple` and `Bulk` frame types are parsed.
    ///
    /// if the next entry cannot be represented as an integer, then an error is returned
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

    /// Ensure there are no more entries in the array
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
