//! Deep Value CLI 入口。
//!
//! ```bash
//! deep-value ping              # 测试 Tushare 连通性
//! deep-value cache clear       # 清除缓存
//! ```

use std::collections::HashMap;

use anyhow::Result;
use clap::{Parser, Subcommand};
use polars::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

use deep_value::config::AppConfig;
use deep_value::data::{audit, cross_section, financials};
use deep_value::db;
use deep_value::report::formatter;
use deep_value::strategy::{
    anomaly,
    domain::{DeepValueConfig, EliminatedStock, Holding, SnapshotResult},
    scoring, screening,
};
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

    /// 执行单期真实选股快照
    Snapshot {
        /// 交易日期，格式 YYYYMMDD
        #[arg(long, default_value = "20250515")]
        date: String,

        /// 最终持仓数量
        #[arg(long, default_value_t = 10)]
        top: usize,
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
        Commands::Snapshot { date, top } => cmd_snapshot(&date, top).await?,
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

/// 单期真实选股快照。
async fn cmd_snapshot(date: &str, top: usize) -> Result<()> {
    let config = AppConfig::load()?;
    let client = TushareClient::new_with_pg(&config.tushare_token, &config.database_url).await?;
    let result = run_ashare_snapshot(&client, date, top).await?;
    println!("{}", formatter::format_snapshot(&result));
    Ok(())
}

async fn run_ashare_snapshot(
    client: &TushareClient,
    trade_date: &str,
    top: usize,
) -> Result<SnapshotResult> {
    let mut config = DeepValueConfig {
        top_n: top,
        ..DeepValueConfig::default()
    };
    config.market = "ashare".to_string();

    let mut step_records = Vec::new();
    let mut data_warnings = Vec::new();
    let mut market_pb_map = HashMap::new();
    let mut is_investable_map = HashMap::new();

    let market_pb = cross_section::get_market_pb_median(client, trade_date).await?;
    let investable = market_pb.is_finite() && market_pb < config.market_pb_threshold;
    market_pb_map.insert("ashare".to_string(), market_pb);
    is_investable_map.insert("ashare".to_string(), investable);

    if config.enforce_market_gate && !investable {
        data_warnings.push(format!(
            "A 股 PB 中位数 {:.2} 高于阈值 {:.2}，按配置不建仓",
            market_pb, config.market_pb_threshold
        ));
        return Ok(SnapshotResult {
            trade_date: trade_date.to_string(),
            market: config.market,
            market_pb_map,
            is_investable_map,
            step_records,
            holdings: Vec::new(),
            eliminated: Vec::new(),
            industry_dist: HashMap::new(),
            data_warnings,
        });
    }

    let mut df = cross_section::build_cross_section(client, trade_date).await?;

    let before = df.height();
    df = df
        .lazy()
        .filter(
            col("pb")
                .gt(lit(0.0))
                .and(col("pb").lt_eq(lit(config.pb_max))),
        )
        .collect()?;
    step_records.push(screening::make_step(
        1,
        &format!("PB <= {:.2}", config.pb_max),
        before,
        df.height(),
        false,
        "",
    ));

    let before = df.height();
    step_records.push(screening::make_step(
        2,
        "十年 PB 高点检查",
        before,
        df.height(),
        true,
        "当前 Rust 数据层尚未实现十年 PB 历史接口，跳过该步骤",
    ));
    data_warnings.push("Step 2 十年 PB 高点检查已跳过：缺少历史 PB 数据源".to_string());

    let net_equity = financials::get_net_equity(client, trade_date).await?;
    df = df
        .lazy()
        .join(
            net_equity.lazy(),
            [col("ts_code")],
            [col("ts_code")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    let audit_info = audit::get_audit_info(client, trade_date).await?;
    df = df
        .lazy()
        .join(
            audit_info.lazy(),
            [col("ts_code")],
            [col("ts_code")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    let before = df.height();
    if config.audit_big4_required {
        df = df
            .lazy()
            .filter(
                col("is_big4")
                    .eq(lit(true))
                    .or(col("net_equity_bn").gt(lit(config.audit_exemption_equity_bn))),
            )
            .collect()?;
    }
    step_records.push(screening::make_step(
        3,
        "四大审计或大净资产豁免",
        before,
        df.height(),
        false,
        &format!("净资产 > {:.0} 亿豁免", config.audit_exemption_equity_bn),
    ));

    let before = df.height();
    df = df
        .lazy()
        .filter(col("dv_ratio").gt_eq(lit(config.dv_ratio_min)))
        .collect()?;
    step_records.push(screening::make_step(
        4,
        &format!("股息率 >= {:.2}", config.dv_ratio_min),
        before,
        df.height(),
        false,
        "",
    ));

    let before = df.height();
    df = df
        .lazy()
        .filter(col("net_equity_bn").gt_eq(lit(config.net_equity_min_bn)))
        .collect()?;
    step_records.push(screening::make_step(
        5,
        &format!("净资产 >= {:.0} 亿", config.net_equity_min_bn),
        before,
        df.height(),
        false,
        "",
    ));

    let current_income = financials::get_current_year_income(client, trade_date).await?;
    let current_dividend = financials::get_current_year_dividend(client, trade_date).await?;
    let income_10y = financials::get_10y_income(client, trade_date, config.lookback_years).await?;
    let dividend_10y =
        financials::get_10y_dividend(client, trade_date, config.lookback_years).await?;

    let before = df.height();
    let anomaly_result = anomaly::remove_anomalies(
        &df,
        &current_income,
        &current_dividend,
        &income_10y,
        &dividend_10y,
    )?;
    let eliminated = anomaly_result
        .removed
        .iter()
        .map(|stock| EliminatedStock {
            ts_code: stock.ts_code.clone(),
            name: lookup_name(&df, &stock.ts_code).unwrap_or_else(|| stock.ts_code.clone()),
            reason: stock.reason.clone(),
        })
        .collect();
    df = anomaly_result.kept;
    step_records.push(screening::make_step(
        6,
        "异常利润/分红排雷",
        before,
        df.height(),
        false,
        "",
    ));

    let before = df.height();
    df = scoring::score_candidates(&df)?;
    step_records.push(screening::make_step(
        7,
        "PB/PE/股息率打分",
        before,
        df.height(),
        false,
        "",
    ));

    let before = df.height();
    df = screening::enforce_industry_cap(&df, config.industry_cap, config.top_n)?;
    step_records.push(screening::make_step(
        8,
        "行业分散约束",
        before,
        df.height(),
        false,
        &format!("单行业上限 {:.0}%", config.industry_cap * 100.0),
    ));

    let before = df.height();
    df = screening::build_portfolio(&df, config.target_equity_weight)?;
    step_records.push(screening::make_step(
        9,
        "等权组合构建",
        before,
        df.height(),
        false,
        &format!("目标股票仓位 {:.0}%", config.target_equity_weight * 100.0),
    ));

    let holdings = dataframe_to_holdings(&df)?;
    let industry_dist = industry_distribution(&holdings);

    Ok(SnapshotResult {
        trade_date: trade_date.to_string(),
        market: config.market,
        market_pb_map,
        is_investable_map,
        step_records,
        holdings,
        eliminated,
        industry_dist,
        data_warnings,
    })
}

fn lookup_name(df: &DataFrame, ts_code: &str) -> Option<String> {
    let ts_codes = df.column("ts_code").ok()?.str().ok()?;
    let names = df.column("name").ok()?.str().ok()?;
    for i in 0..df.height() {
        if ts_codes.get(i) == Some(ts_code) {
            return names.get(i).map(ToOwned::to_owned);
        }
    }
    None
}

fn dataframe_to_holdings(df: &DataFrame) -> Result<Vec<Holding>> {
    let mut holdings = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        holdings.push(Holding {
            ts_code: string_at(df, "ts_code", i),
            name: string_at(df, "name", i),
            market: "ashare".to_string(),
            industry: string_at(df, "industry", i),
            pb: f64_at(df, "pb", i),
            pe: f64_at(df, "pe_ttm", i),
            dv_ratio: f64_at(df, "dv_ratio", i),
            net_equity_bn: f64_at(df, "net_equity_bn", i),
            pb_score: f64_at(df, "pb_score", i),
            pe_score: f64_at(df, "pe_score", i),
            div_score: f64_at(df, "div_score", i),
            total_score: f64_at(df, "total_score", i),
            weight: f64_at(df, "weight", i),
        });
    }
    Ok(holdings)
}

fn industry_distribution(holdings: &[Holding]) -> HashMap<String, usize> {
    let mut dist = HashMap::new();
    for holding in holdings {
        *dist.entry(holding.industry.clone()).or_insert(0) += 1;
    }
    dist
}

fn string_at(df: &DataFrame, column: &str, row: usize) -> String {
    df.column(column)
        .ok()
        .and_then(|col| col.str().ok())
        .and_then(|col| col.get(row))
        .unwrap_or("")
        .to_string()
}

fn f64_at(df: &DataFrame, column: &str, row: usize) -> f64 {
    df.column(column)
        .ok()
        .and_then(|col| col.f64().ok())
        .and_then(|col| col.get(row))
        .unwrap_or(f64::NAN)
}
