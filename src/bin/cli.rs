use my_mini_redis::{clients::Client, DEFAULT_PORT};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::convert::Infallible;
use std::num::ParseIntError;
use std::str;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(
    name = "my-mini-redis-cli",
    version,
    author,
    about = "Issue Redis commands"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// ping的msg
        #[clap(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
    /// 获得key对应value
    Get {
        /// get所需key
        key: String,
    },
    /// 设置key-value对
    Set {
        /// key的名称
        key: String,

        /// value
        #[clap(value_parser = bytes_from_str)]
        value: Bytes,

        /// 过期时间
        #[clap(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    /// 向指定channel发送信息
    Publish {
        /// channel的名字
        channel: String,

        #[clap(value_parser = bytes_from_str)]
        /// 发布的信息
        message: Bytes,
    },
    /// 一个客户端订阅channels
    Subscribe {
        /// 指定channels
        channels: Vec<String>,
    },
}

/// 注解"[tokio::main]"表示在调用函数时应启动 Tokio 运行时。
/// 函数的主体将在新生成的运行时中执行。
///
/// 这里使用 `flavor = "current_thread"` 来避免产生后台线程。
/// CLI 工具的用例更受益于轻量级的多线程。
#[tokio::main(flavor = "current_thread")]
async fn main() -> my_mini_redis::Result<()> {
    // 记录日志
    tracing_subscriber::fmt::try_init()?;

    // 解析命令行参数
    let cli = Cli::parse();

    // 获得远程连接的地址
    let addr = format!("{}:{}", cli.host, cli.port);

    // 建立连接
    let mut client = Client::connect(&addr).await?;

    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expire),
        } => {
            client.set_expires(&key, value, expire).await?;
            println!("OK");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into());
            }
            let mut subscriber = client.subscribe(channels).await?;

            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "got message from the channel: {}; message = {:?}",
                    msg.channel, msg.content
                );
            }
        }
    }
    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

fn bytes_from_str(src: &str) -> Result<Bytes, Infallible> {
    Ok(Bytes::from(src.to_string()))
}
