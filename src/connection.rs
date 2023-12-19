use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// Send and receive `Frame` value from a remote peer.
/// 
/// When implementing networking protocol, message on that protocol is 
/// often comoposed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
/// 
/// To read frames, the `Connection` use an internal buffer, which is filled up
/// until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
/// 
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.

#[derive(Debug)]
pub struct Connection {
    //  `TcpStream` 被一个提供了写入级别缓冲的 `BufWriter` 所装饰。
    // 由Tokio提供的 `BufWriter` 实现可以满足我们的需要。
    stream: BufWriter<TcpStream>,

    // 用来读frame的buffer
    buffer: BytesMut
}

impl Connection {
    /// Create a new `Connectioin`, backed by `socket`, Read an write buffers
    /// are initialized
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // read buffer 默认大小为4KB 对于mini redis的使用情景这样是可以的
            // 但是真实的应用会因为他们特别的使用情景而调整这个值。
            // 很有可能 read buffer 越大，效果越好
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    /// 
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    /// 
    /// # Returns
    /// 
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop{
            // 尝试从buffer中解析出一个frame。如果buffer中有足够的数据，返回一个frame
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            } 

            // 如果没有读到足够的数据，尝试从socket中读取更多数据
            // 如果成功，会返回读取的字节数量，0代表TcpStream的结尾
            // await等待read_buf做完
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 远程关闭了连接。若要干净的关闭，buffer中不应该有数据
                // 如果有，这表示远程在发送frame时关闭了socket
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Tries to parse a frame from buffer. If the buffer contains enough
    /// data. the frame is returned and the data removed from the buffer.If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // Cursor用来跟踪在buffer中的当前位置。 Cursor也实现了`bytes`包中的`Buf`
        // 这提供了许多有用的工具来操作bytes
        let mut cursor = Cursor::new(&self.buffer[..]);

        // 首先快速判断buffer中数据是否合法，这比解析buffer中的数据要快很多
        // 在我们知道这是一个完整的frame之前，我们不需要为保存frame data的数据
        // 结构分配空间
        match Frame::check(&mut cursor) {
            Ok(_) => {
                // check过后，len会是一个完整frame的长度包括 ”\r\n“
                let len = cursor.position() as usize;
                // 将cursor位置设置为0，以供parse()解析
                cursor.set_position(0);
                // 此处分配空间来保存frame数据是必要的
                // 如果编码frame表示是非法的，错误被返回。
                // 这种情况应该终止当前连接，而不是影响到其他连接
                let frame = Frame::parse(&mut cursor)?;
                
                // 摒弃已经解析过的frame data
                // 这个操作经常通过移动内部cursor实现，但有些时候
                // 可能会通过重新分配内存和copy数据来实现
                self.buffer.advance(len);

                // 返回解析的frame
                Ok(Some(frame))
            },
            // 如果没有足够的数据来解析成一个frame。我们必须等待更多的数据
            // 从socket中被接收。在这个match结束后，从socket中读数据将会被执行
            // 所以在这里，我们不想返回一个Err，因为这个"error"是一个运行时
            // 所期望的条件
            Err(Incomplete) => Ok(None),
            // 这个error表示解析frame时出现了错误，这个表示当前连接处在非法状态
            // 这里要返回`Err`，使得连接停止
            Err(e) => Err(e.into())
        }
    }

    /// Write a single `Frame` value to the underlying stream
    ///
    /// The `Frame` value is written to the socket using various `write_*`
    /// function provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these function on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket. 
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // Array通过编码其他entry来编码。 其他frame type被认为是字面量。
        // 现在，mini redis还不能编码recursive frame structures。
        match frame{
            Frame::Array(vec) => {
                self.stream.write_u8(b'*').await?;

                self.write_decimal(vec.len() as u64).await?;

                for entry in &*vec{
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?
        }

        // 确保encode frame 被写入socket。上面的调用是将数据写入buffered stream。
        // 调用`flush`将在buffer中剩余的内容写入到socket中
        self.stream.flush().await
    }

    /// Write a frame literal to the stream
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            },
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            },
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            // 不能使用递归策略从一个值内部对Array进行编码。一般来说异步函数
            // 不支持递归。Mini-redis还不需要对nested(嵌套)arrays进行编码
            // 所以暂时跳过
            Frame::Array(_val) => unreachable!(),
        }
        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}