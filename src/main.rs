//! Deep Value CLI 入口。
//!
//! ```bash
//! deep-value ping              # 测试 Tushare 连通性
//! deep-value cache clear       # 清除缓存
//! ```

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

use deep_value::config::AppConfig;
use deep_value::db;
use deep_value::tushare::client::TushareClient;
use deep_value::tushare::pg_cache::PgCache;

/// Deep Value 量化回测框架
#[derive(Parser)]
#[command(name = "deep-value", version, about = "低估分散不深研 — Rust 实现")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 测试 Tushare API 连通性
    Ping,

    /// 缓存管理
    Cache {
        #[command(subcommand)]
        action: CacheAction,
    },

    /// 数据库管理
    Db {
        #[command(subcommand)]
        action: DbAction,
    },
}

#[derive(Subcommand)]
enum CacheAction {
    /// 清除所有 PostgreSQL raw 缓存记录
    Clear,
}

#[derive(Subcommand)]
enum DbAction {
    /// 测试 PostgreSQL 连通性
    Ping,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Ping => cmd_ping().await?,
        Commands::Cache { action } => match action {
            CacheAction::Clear => cmd_cache_clear().await?,
        },
        Commands::Db { action } => match action {
            DbAction::Ping => cmd_db_ping().await?,
        },
    }

    Ok(())
}

/// 连通性测试。
async fn cmd_ping() -> Result<()> {
    let config = AppConfig::load()?;
    let client = TushareClient::new_with_pg(&config.tushare_token, &config.database_url).await?;
    let result = client.ping().await?;
    println!("{result}");
    Ok(())
}

/// 清除 PostgreSQL raw 缓存。
async fn cmd_cache_clear() -> Result<()> {
    let config = AppConfig::load()?;
    let pool = db::connect(&config.database_url).await?;
    db::init_schema(&pool).await?;
    let cache = PgCache::new(pool);
    let count = cache.clear_all().await?;
    info!(count, "PostgreSQL raw 缓存已清除");
    println!("✅ 已清除 {count} 条 PostgreSQL raw 缓存记录");
    Ok(())
}

/// PostgreSQL 连通性测试。
async fn cmd_db_ping() -> Result<()> {
    let config = AppConfig::load()?;
    let pool = db::connect(&config.database_url).await?;
    db::health_check(&pool).await?;
    println!("✅ PostgreSQL 连接成功");
    Ok(())
}
