mod get;
pub use get::Get;

mod ping;
pub use ping::Ping;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, Shutdown};

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subcribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    /// 从接收到的帧中解析命令。
    ///
    /// `Frame` 必须代表一个mini redis支持的redis命令，且必须是数组变量
    ///
    /// # 返回值
    ///
    /// 成功command值被返回，失败则返回错误
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subcribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;

        Ok(command)
    }

    /// 将相应命令应用到指定Db实例
    ///
    ///
    /// 响应被写入 `dst`。服务器会调用该函数来执行收到的命令
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subcribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` 无法被执行，它只能在`Subscribe`指令执行时，被收到
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context.".into()),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Publish(_) => "publish",
            Command::Set(_) => "set",
            Command::Subcribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Ping(_) => "ping",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
