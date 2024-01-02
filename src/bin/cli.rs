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

    #[clap(name= "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// Message to ping
        #[clap(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
    /// Get the value of key
    Get {
        /// Name of key to get
        key: String,
    },
    /// Set key to hold the string value
    Set {
        /// Name of key to set
        key: String,

        /// Value to set.
        #[clap(value_parser = bytes_from_str)]
        value: Bytes,

        /// Expire the value after specified amount of time
        #[clap(value_parser = duration_from_ms_str)]
        expires: Option<Duration>
    },
    /// Publisher to send a message to a specific channel,
    Publish {
        /// Name of channel
        channel: String,

        #[clap(value_parser = bytes_from_str)]
        /// Message to publish
        message: Bytes,
    },
    /// Subscribe a client to a specific channel or channels
    Subcribe {
        /// Specific channel or channels
        channels: Vec<String>,
    }
}

/// Entry point for CLI tool.
/// 
/// The `[tokio::main]` annotation signals that the Tokio runtime should be 
/// started when the function is called. The body of the function is executed
/// within the newly spawned runtime.
/// 注解"[tokio::main]"表示在调用函数时应启动 Tokio 运行时。函数的主体将在新生成的运行时中执行。
/// 
/// `flavor = "current_thread"` is used here o avoid spawning background
/// threads. The CLI tool use case benefits more by being lighter instead of 
/// multi-threaded.
/// 这里使用 `flavor = "current_thread"` 来避免产生后台线程。CLI 工具的用例更受益于轻量级的多线程。
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
        },
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
        },
        Command::Set { key, value, expires: None } => {
            client.set(&key, value).await?;
            println!("OK");
        },
        Command::Set { key, value, expires: Some(expire) } => {
            client.set_expires(&key, value, expire).await?;
            println!("OK");
        },
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        },
        Command::Subcribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into());
            }
            let mut subscriber = client.subscribe(channels).await?;

            while let Some(msg) = subscriber.next_message().await? {
                println!("got message from the channel: {}; message = {:?}",
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