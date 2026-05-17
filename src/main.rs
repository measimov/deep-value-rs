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
use deep_value::data::{cross_section, financials};
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

        /// 跳过市场 PB 中位数建仓门槛检查
        #[arg(long)]
        skip_pb_check: bool,
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
        Commands::Snapshot {
            date,
            top,
            skip_pb_check,
        } => cmd_snapshot(&date, top, skip_pb_check).await?,
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
async fn cmd_snapshot(date: &str, top: usize, skip_pb_check: bool) -> Result<()> {
    let config = AppConfig::load()?;
    let client = TushareClient::new_with_pg(&config.tushare_token, &config.database_url).await?;
    let result = run_ashare_snapshot(&client, date, top, skip_pb_check).await?;
    println!("{}", formatter::format_snapshot(&result));
    Ok(())
}

async fn run_ashare_snapshot(
    client: &TushareClient,
    trade_date: &str,
    top: usize,
    skip_pb_check: bool,
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

    if skip_pb_check {
        data_warnings.push(format!(
            "已按命令行参数跳过市场 PB 门槛检查：A 股 PB 中位数 {:.2}，默认阈值 {:.2}",
            market_pb, config.market_pb_threshold
        ));
    }

    if config.enforce_market_gate && !skip_pb_check && !investable {
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
    let pb_10y = get_10y_pb_max(client, trade_date, config.lookback_years).await?;
    df = df
        .lazy()
        .join(
            pb_10y.lazy(),
            [col("ts_code")],
            [col("ts_code")],
            JoinArgs::new(JoinType::Left),
        )
        .filter(col("pb_10y_max").gt(lit(config.pb_10y_must_exceed)))
        .collect()?;
    step_records.push(screening::make_step(
        2,
        "十年 PB 高点检查",
        before,
        df.height(),
        false,
        &format!("十年 PB max > {:.2}", config.pb_10y_must_exceed),
    ));

    let financial_pool_size = (config.top_n * 10).max(config.top_n).min(df.height());
    if df.height() > financial_pool_size {
        let scored_pool = scoring::score_candidates(&df)?;
        df = screening::enforce_industry_cap(
            &scored_pool,
            config.industry_cap,
            financial_pool_size,
        )?;
        data_warnings.push(format!(
            "逐只拉取财务/审计数据前，按初筛打分和行业分散保留前 {} 只候选以控制 Tushare 请求量",
            financial_pool_size
        ));
    }

    let pool_codes = ts_codes_from_df(&df)?;
    let net_equity = get_net_equity_for_codes(client, trade_date, &pool_codes).await?;
    df = df
        .lazy()
        .join(
            net_equity.lazy(),
            [col("ts_code")],
            [col("ts_code")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    let audit_info = get_audit_for_codes(client, trade_date, &pool_codes).await?;
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
    let audit_note = format!("净资产 > {:.0} 亿豁免", config.audit_exemption_equity_bn);
    step_records.push(screening::make_step(
        3,
        "四大审计或大净资产豁免",
        before,
        df.height(),
        false,
        &audit_note,
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

    let before = df.height();
    let anomaly_codes = ts_codes_from_df(&df)?;
    let (current_income, current_dividend, income_10y, dividend_10y) =
        load_anomaly_inputs(client, trade_date, config.lookback_years, &anomaly_codes).await?;
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

async fn load_anomaly_inputs(
    client: &TushareClient,
    trade_date: &str,
    lookback_years: usize,
    codes: &[String],
) -> Result<(DataFrame, DataFrame, DataFrame, DataFrame)> {
    let current_income = get_current_income_for_codes(client, trade_date, codes).await?;
    let current_dividend = get_current_dividend_for_codes(client, trade_date, codes).await?;
    let income_10y = get_10y_income_for_codes(client, trade_date, lookback_years, codes).await?;
    let dividend_10y =
        get_10y_dividend_for_codes(client, trade_date, lookback_years, codes).await?;
    Ok((current_income, current_dividend, income_10y, dividend_10y))
}

async fn get_10y_pb_max(
    client: &TushareClient,
    trade_date: &str,
    lookback_years: usize,
) -> Result<DataFrame> {
    let year: i32 = trade_date[..4].parse().unwrap_or(2025);
    let month_day = &trade_date[4..];
    let start_year = year - lookback_years as i32 + 1;
    let mut max_pb: HashMap<String, f64> = HashMap::new();

    for y in start_year..=year {
        let date = format!("{y}{month_day}");
        let df = client
            .query(
                "daily_basic",
                &[("trade_date", date.as_str())],
                Some("ts_code,pb"),
            )
            .await?;
        if df.height() == 0 {
            continue;
        }
        let pb = df
            .lazy()
            .with_column(col("pb").cast(DataType::Float64))
            .collect()?;
        let codes = pb.column("ts_code")?.str()?;
        let pb_values = pb.column("pb")?.f64()?;
        for i in 0..pb.height() {
            if let (Some(code), Some(value)) = (codes.get(i), pb_values.get(i)) {
                max_pb
                    .entry(code.to_string())
                    .and_modify(|old| *old = old.max(value))
                    .or_insert(value);
            }
        }
    }

    let mut codes: Vec<String> = max_pb.keys().cloned().collect();
    codes.sort();
    let values: Vec<f64> = codes.iter().map(|code| max_pb[code]).collect();
    Ok(df!("ts_code" => codes, "pb_10y_max" => values)?)
}

async fn get_net_equity_for_codes(
    client: &TushareClient,
    trade_date: &str,
    codes: &[String],
) -> Result<DataFrame> {
    let period = format!("{}1231", financials::safe_financial_year(trade_date));
    let mut out_codes = Vec::new();
    let mut values = Vec::new();
    for code in codes {
        let df = client
            .query(
                "balancesheet",
                &[
                    ("ts_code", code.as_str()),
                    ("period", period.as_str()),
                    ("report_type", "1"),
                ],
                Some("ts_code,end_date,total_hldr_eqy_exc_min_int"),
            )
            .await?;
        if df.height() == 0 {
            continue;
        }
        out_codes.push(code.clone());
        values.push(parse_f64_cell(&df, "total_hldr_eqy_exc_min_int", 0).unwrap_or(0.0) / 1e8);
    }
    Ok(df!("ts_code" => out_codes, "net_equity_bn" => values)?)
}

async fn get_audit_for_codes(
    client: &TushareClient,
    trade_date: &str,
    codes: &[String],
) -> Result<DataFrame> {
    let period = format!("{}1231", financials::safe_financial_year(trade_date));
    let mut out_codes = Vec::new();
    let mut flags = Vec::new();
    for code in codes {
        let df = client
            .query(
                "fina_audit",
                &[("ts_code", code.as_str()), ("period", period.as_str())],
                Some("ts_code,audit_agency"),
            )
            .await?;
        if df.height() == 0 {
            continue;
        }
        let agency = string_at(&df, "audit_agency", 0);
        out_codes.push(code.clone());
        flags.push(deep_value::strategy::domain::is_big4(&agency));
    }
    Ok(df!("ts_code" => out_codes, "is_big4" => flags)?)
}

async fn get_current_income_for_codes(
    client: &TushareClient,
    trade_date: &str,
    codes: &[String],
) -> Result<DataFrame> {
    let period = format!("{}1231", financials::safe_financial_year(trade_date));
    let mut out_codes = Vec::new();
    let mut values = Vec::new();
    for code in codes {
        let df = client
            .query(
                "income",
                &[
                    ("ts_code", code.as_str()),
                    ("period", period.as_str()),
                    ("report_type", "1"),
                ],
                Some("ts_code,end_date,n_income"),
            )
            .await?;
        if df.height() == 0 {
            continue;
        }
        out_codes.push(code.clone());
        values.push(parse_f64_cell(&df, "n_income", 0).unwrap_or(0.0));
    }
    Ok(df!("ts_code" => out_codes, "current_net_income" => values)?)
}

async fn get_current_dividend_for_codes(
    client: &TushareClient,
    trade_date: &str,
    codes: &[String],
) -> Result<DataFrame> {
    let period = format!("{}1231", financials::safe_financial_year(trade_date));
    let mut out_codes = Vec::new();
    let mut values = Vec::new();
    for code in codes {
        let df = client
            .query(
                "dividend",
                &[("ts_code", code.as_str()), ("end_date", period.as_str())],
                Some("ts_code,end_date,cash_div_tax"),
            )
            .await?;
        out_codes.push(code.clone());
        values.push(sum_f64_column(&df, "cash_div_tax"));
    }
    Ok(df!("ts_code" => out_codes, "current_dividend_total" => values)?)
}

async fn get_10y_income_for_codes(
    client: &TushareClient,
    trade_date: &str,
    lookback_years: usize,
    codes: &[String],
) -> Result<DataFrame> {
    let safe_year = financials::safe_financial_year(trade_date);
    let start_year = safe_year - lookback_years as i32 + 1;
    let mut out_codes = Vec::new();
    let mut values = Vec::new();
    for code in codes {
        let mut total = 0.0;
        for year in start_year..=safe_year {
            let period = format!("{year}1231");
            let df = client
                .query(
                    "income",
                    &[
                        ("ts_code", code.as_str()),
                        ("period", period.as_str()),
                        ("report_type", "1"),
                    ],
                    Some("ts_code,end_date,n_income"),
                )
                .await?;
            total += parse_f64_cell(&df, "n_income", 0).unwrap_or(0.0);
        }
        out_codes.push(code.clone());
        values.push(total);
    }
    Ok(df!("ts_code" => out_codes, "sum_net_income_10y" => values)?)
}

async fn get_10y_dividend_for_codes(
    client: &TushareClient,
    trade_date: &str,
    lookback_years: usize,
    codes: &[String],
) -> Result<DataFrame> {
    let safe_year = financials::safe_financial_year(trade_date);
    let start_year = safe_year - lookback_years as i32 + 1;
    let mut out_codes = Vec::new();
    let mut values = Vec::new();
    for code in codes {
        let mut total = 0.0;
        for year in start_year..=safe_year {
            let period = format!("{year}1231");
            let df = client
                .query(
                    "dividend",
                    &[("ts_code", code.as_str()), ("end_date", period.as_str())],
                    Some("ts_code,end_date,cash_div_tax"),
                )
                .await?;
            total += sum_f64_column(&df, "cash_div_tax");
        }
        out_codes.push(code.clone());
        values.push(total);
    }
    Ok(df!("ts_code" => out_codes, "sum_dividend_10y" => values)?)
}

fn ts_codes_from_df(df: &DataFrame) -> Result<Vec<String>> {
    Ok(df
        .column("ts_code")?
        .str()?
        .into_iter()
        .filter_map(|value| value.map(ToOwned::to_owned))
        .collect())
}

fn parse_f64_cell(df: &DataFrame, column: &str, row: usize) -> Option<f64> {
    if row >= df.height() {
        return None;
    }
    let col = df.column(column).ok()?;
    if let Ok(values) = col.f64() {
        return values.get(row);
    }
    col.str().ok()?.get(row)?.parse().ok()
}

fn sum_f64_column(df: &DataFrame, column: &str) -> f64 {
    if df.height() == 0 {
        return 0.0;
    }
    let Ok(col) = df.column(column) else {
        return 0.0;
    };
    if let Ok(values) = col.f64() {
        return values.into_iter().flatten().sum();
    }
    col.str()
        .map(|values| {
            values
                .into_iter()
                .filter_map(|value| value.and_then(|text| text.parse::<f64>().ok()))
                .sum()
        })
        .unwrap_or(0.0)
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
