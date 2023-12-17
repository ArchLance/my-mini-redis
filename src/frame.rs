//! Provides a type representing a Redis protocol frame as well as utilities for 
//! parsing frames from a byte array


use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// A frame in the Redis protocol
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
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error)
}

impl Frame {
    /// Returns an empty array
    // pub(crate) 代表本crate内可见
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }
    /// Push a "bulk" frame into the array. `self` must be an Array frame.
    /// 
    /// # Panics
    /// 
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes){
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

    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        Ok(())
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
fn skip(src: &mut Cursor<&[u8]>,n: usize) -> Result<(), Error> {
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
    let end = src.get_ref().len()-1;
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i+1] == b'\n' {
            src.set_position((i+2) as u64);
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(f),
            Error::Other(err) => err.fmt(f)
        }
    }
}