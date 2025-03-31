//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// 在Redis协议中的Frame
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// 没有足够的数据解析成一个frame
    Incomplete,

    /// 不规范的编码
    Other(crate::Error),
}

impl Frame {
    /// 返回一个空数组
    // pub(crate) 代表本crate内可见
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }
    /// 将bulk frame放入数组中，self必须是一个Array frame
    ///
    /// # Panics
    ///
    /// 如果不是Array frame则抛出异常
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// 判断src是否是一个完整的信息
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            // 简单字符串: +OK\r\n
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            // 简单错误: -Error message\r\n
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            // 整形: :[<+|->]<value>\r\n
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            // 长字符串: $<length>\r\n<data>\r\n
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // 跳过'-1\r\n'
                    skip(src, 4)
                } else {
                    // 这里需要实现 From<TryFromIntError> for Error
                    // 读取bulk string长度
                    let len: usize = get_decimal(src)?.try_into()?;

                    // 跳过字节数+2(\r\n)
                    skip(src, len + 2)
                }
            }
            // 数组: *<number-of-elements>\r\n<element-1>...<element-n>
            b'*' => {
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            // 其他任意字符
            actual => Err(format!("protocol error: invalid frame type byte `{}`", actual).into()),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?.to_vec();
                // 需要实现 impl From<FromUtf8Error> for Error
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?.to_vec();

                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len: usize = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    /// 将frame转换为一个"unexpected frame" error
    pub(crate) fn to_error(&self) -> crate::Error {
        // 需要实现fmt::Display for Frame
        format!("unexpected frame: {}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::Simple(response) => response.fmt(f),
            Frame::Error(msg) => write!(f, "error: {}", msg),
            Frame::Integer(num) => num.fmt(f),
            Frame::Bulk(msg) => match std::str::from_utf8(msg) {
                Ok(string) => string.fmt(f),
                Err(_) => write!(f, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(f),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }

                    part.fmt(f)?;
                }

                Ok(())
            }
        }
    }
}

/// 取Cursor当前指向的第一个字节,但Cursor不向前移动
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    // https://docs.rs/bytes/latest/bytes/buf/trait.Buf.html
    // 如果还有字节可以消费，返回true
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    // fn remaining(&self) -> &[u8]
    // 返回一个从当前位置开始，到buffer结束的字节数

    // chunk()返回一个从当前位置的切片，长度在0到 `Buf::remaining()` 之间
    // 注意这个函数可以返回一个长度比 `Buf::remaining()` 更短的切片
    // (这允许不连续的内部表示)
    Ok(src.chunk()[0])
}

/// 取Cursor当前指向的第一个字节，Cursor向后移动一个字节
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

/// 使Cursor向后移动n个字节
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}
/// 将一行转换为u64
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;
    atoi::<u64>(line).ok_or_else(|| "protocol error: invalid frame format".into())
}

/// 获取一行(\r\n)
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    // get_ref()获得当前Cursor的底层数据结构的引用
    let end = src.get_ref().len() - 1;
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            // []将get_ref()获得的引用deref了，所以变成了[u8]，需要加&
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

impl From<String> for Error {
    fn from(value: String) -> Error {
        Error::Other(value.into())
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Error {
        Error::Other(value.into())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_value: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_value: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}
