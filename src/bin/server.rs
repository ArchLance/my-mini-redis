//! my-mini-redis server
//!
//! 这个文件是server组件的入口点，主要完成两个任务
//! 1. 执行命令行参数解析
//! 2. 将解析后的参数传递给my-mini-redis::server模块处理
//!  `clap` 包被用来解析参数

use my_mini_redis::{server, DEFAULT_PORT};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::sdk::trace as sdktrace;
#[cfg(feature = "otel")]
use opentelemetry_aws::trace::XrayPropagator;
#[cfg(feature = "otel")]
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, util::TryInitError, EnvFilter,
};

#[tokio::main]
pub async fn main() -> my_mini_redis::Result<()> {
    // 初始化日志
    set_up_logging()?;
    // 解析命令行参数
    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);
    //  这行代码在本地IP地址上启动一个TCP服务器，监听指定的端口，以异步方式执行，
    // 并进行适当的错误处理。成功后，listener变量将包含一个已准备好接受客户端连接的TCP监听器。
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    // 调用server模块中run函数，当signal::ctrl_c()这个future结束后，执行优雅关闭
    server::run(listener, signal::ctrl_c()).await;
    // 返回值
    Ok(())
}

#[derive(Parser, Debug)]
#[clap(
    name = "my-mini-redis-server",
    version,
    author,
    about = "A Redis server"
)]
struct Cli {
    #[clap(long)]
    port: Option<u16>,
}

#[cfg(not(feature = "otel"))]
fn set_up_logging() -> my_mini_redis::Result<()> {
    tracing_subscriber::fmt::try_init()
}

#[cfg(feature = "otel")]
fn set_up_logging() -> Result<(), TryInitError> {
    // 将全局传播器设置为 X 射线传播器
    // 注意：如果需要在同一跟踪中跨服务传递 x-amzn-trace-id，
    // 则需要此行。不过，这需要额外的代码，此处未画出。
    //有关使用 hyper 的完整示例，请参阅
    // https://github.com/open-telemetry/opentelemetry-rust/blob/main/examples/aws-xray/src/server.rs#L14-L26

    use tracing_subscriber::{fmt, EnvFilter};
    global::set_text_map_propagator(XrayPropagator::default());

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            sdktrace::config()
                .with_sampler(sdktrace::Sampler::AlwaysOn)
                // 需要将轨迹 ID 转换为 Xray 兼容格式
                .with_id_generator(sdktrace::XrayIdGenerator::default()),
        )
        .install_simple()
        .expect("Unable to initialize OtlpPipeline");

    // 使用配置的跟踪器创建跟踪层
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // 从 `RUST_LOG` 环境变量中解析一个 `EnvFilter` 配置
    let filter = EnvFilter::from_default_env();

    // 使用跟踪订阅器`Registry`, 或者其他实现了`LookupSpan`的订阅者
    tracing_subscriber::registry()
        .with(opentelemetry)
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}
