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
use sqlx::Row;
use tracing::info;
use tracing_subscriber::EnvFilter;

use deep_value::backtest::{engine, metrics};
use deep_value::config::AppConfig;
use deep_value::data::{cross_section, financials, local, sync};
use deep_value::db;
use deep_value::report::formatter;
use deep_value::strategy::{
    anomaly,
    domain::{CostConfig, DeepValueConfig, EliminatedStock, Holding, SnapshotResult},
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

        /// 从本地 PostgreSQL typed 表读取，不调用 Tushare API
        #[arg(long)]
        local: bool,
    },

    /// 季度再平衡回测（离线）
    Backtest {
        /// 起始日期，格式 YYYYMMDD
        #[arg(long, default_value = "20150101")]
        start: String,

        /// 结束日期，格式 YYYYMMDD
        #[arg(long, default_value = "20250520")]
        end: String,

        /// 每期持仓数量
        #[arg(long, default_value_t = 30)]
        top: usize,

        /// 从本地 PostgreSQL typed 表读取
        #[arg(long)]
        local: bool,

        /// 跳过市场 PB 中位数建仓门槛检查
        #[arg(long)]
        skip_pb_check: bool,
    },

    /// 检查 PostgreSQL 中 Tushare 数据完整性
    DataQuality {
        /// 市场: a | hk | us
        #[arg(long, default_value = "a")]
        market: String,

        /// 起始日期，格式 YYYYMMDD
        #[arg(long, default_value = "20150101")]
        start: String,

        /// 结束日期，格式 YYYYMMDD
        #[arg(long, default_value = "20250520")]
        end: String,
    },

    /// 预取 Tushare 数据到 PostgreSQL typed 表
    Sync {
        /// 市场: a | hk | us
        #[arg(long, default_value = "a")]
        market: String,

        /// 起始日期，格式 YYYYMMDD（全量模式）
        #[arg(long, default_value = "20150101")]
        start: String,

        /// 结束日期，格式 YYYYMMDD（全量模式）
        #[arg(long, default_value = "20250515")]
        end: String,

        /// 周年快照月日，格式 MMDD（默认使用 end 的 MMDD）
        #[arg(long)]
        anniversary: Option<String>,

        /// API 最小调用间隔（毫秒）
        #[arg(long, default_value_t = 600)]
        delay_ms: u64,

        /// full sync 为本地十年 PB 检查预取的历史年数
        #[arg(long, default_value_t = 10)]
        lookback_years: usize,

        /// HK/US 同步范围: all | meta | market | financial
        #[arg(long, default_value = "all")]
        scope: String,

        /// HK/US 调试/分批同步时最多处理多少只股票；默认不限制
        #[arg(long)]
        max_codes: Option<usize>,

        /// 增量模式：只补缺失数据
        #[arg(long)]
        incremental: bool,

        /// 增量模式类型: daily | financial | meta（仅 incremental 时有效）
        #[arg(long, default_value = "daily")]
        sync_mode: String,
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
        Commands::Backtest {
            start,
            end,
            top,
            local,
            skip_pb_check,
        } => cmd_backtest(&start, &end, top, local, skip_pb_check).await?,
        Commands::Cache { action } => match action {
            CacheAction::Clear => cmd_cache_clear().await?,
        },
        Commands::Db { action } => match action {
            DbAction::Ping => cmd_db_ping().await?,
        },
        Commands::DataQuality { market, start, end } => {
            cmd_data_quality(&market, &start, &end).await?
        }
        Commands::Sync {
            market,
            start,
            end,
            anniversary,
            delay_ms,
            lookback_years,
            scope,
            max_codes,
            incremental,
            sync_mode,
        } => {
            cmd_sync(
                &market,
                &start,
                &end,
                anniversary.as_deref(),
                delay_ms,
                lookback_years,
                &scope,
                max_codes,
                incremental,
                &sync_mode,
            )
            .await?
        }
        Commands::Snapshot {
            date,
            top,
            skip_pb_check,
            local,
        } => cmd_snapshot(&date, top, skip_pb_check, local).await?,
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

/// 检查本地 Tushare 数据完整性。
async fn cmd_data_quality(market: &str, start: &str, end: &str) -> Result<()> {
    let config = AppConfig::load()?;
    let pool = db::connect(&config.database_url).await?;
    db::init_schema(&pool).await?;

    if !matches!(
        market.to_ascii_lowercase().as_str(),
        "a" | "cn" | "ashare" | "a-share"
    ) {
        return cmd_global_data_quality(&pool, market, start, end).await;
    }

    println!("数据质量检查: {start}..{end}");

    let job_rows = sqlx::query(
        r#"
        select status, count(*) as jobs
        from deep_value.tushare_sync_jobs
        group by status
        order by status
        "#,
    )
    .fetch_all(&pool)
    .await?;
    println!("\n同步任务状态:");
    for row in job_rows {
        let status: String = row.try_get("status")?;
        let jobs: i64 = row.try_get("jobs")?;
        println!("  {status}: {jobs}");
    }

    println!("\n日线类缺失开市日:");
    for (name, table) in [
        ("daily", "tushare_daily"),
        ("adj_factor", "tushare_adj_factor"),
        ("daily_basic", "tushare_daily_basic"),
        ("stk_limit", "tushare_stk_limit"),
        ("suspend_d", "tushare_suspend_d"),
    ] {
        let sql = format!(
            r#"
            with open_dates as (
                select cal_date
                from deep_value.tushare_trade_cal
                where exchange = 'SSE'
                  and is_open = '1'
                  and cal_date between $1 and $2
            )
            select count(*) as missing_open_dates,
                   min(cal_date) as first_missing,
                   max(cal_date) as last_missing
            from open_dates o
            left join (select distinct trade_date from deep_value.{table}) d
              on d.trade_date = o.cal_date
            where d.trade_date is null
            "#,
        );
        let row = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .fetch_one(&pool)
            .await?;
        let missing: i64 = row.try_get("missing_open_dates")?;
        let first: Option<String> = row.try_get("first_missing")?;
        let last: Option<String> = row.try_get("last_missing")?;
        println!("  {name}: missing={missing}, first={first:?}, last={last:?}");
    }

    let fin_rows = sqlx::query(
        r#"
        select 'income' as table_name, count(*) as rows, max(end_date) as max_end_date from deep_value.tushare_income
        union all select 'balancesheet', count(*), max(end_date) from deep_value.tushare_balancesheet
        union all select 'cashflow', count(*), max(end_date) from deep_value.tushare_cashflow
        union all select 'fina_indicator', count(*), max(end_date) from deep_value.tushare_fina_indicator
        union all select 'financial_rows:income_vip', count(*), max(end_date) from deep_value.tushare_financial_rows where api_name = 'income_vip'
        union all select 'financial_rows:balancesheet_vip', count(*), max(end_date) from deep_value.tushare_financial_rows where api_name = 'balancesheet_vip'
        union all select 'financial_rows:cashflow_vip', count(*), max(end_date) from deep_value.tushare_financial_rows where api_name = 'cashflow_vip'
        union all select 'financial_rows:fina_indicator_vip', count(*), max(end_date) from deep_value.tushare_financial_rows where api_name = 'fina_indicator_vip'
        order by table_name
        "#,
    )
    .fetch_all(&pool)
    .await?;
    println!("\n财报覆盖:");
    for row in fin_rows {
        let table_name: String = row.try_get("table_name")?;
        let rows: i64 = row.try_get("rows")?;
        let max_end_date: Option<String> = row.try_get("max_end_date")?;
        println!("  {table_name}: rows={rows}, max_end_date={max_end_date:?}");
    }

    let future_stock_row = sqlx::query(
        r#"
        select count(*) as rows, min(list_date) as min_list_date, max(list_date) as max_list_date
        from deep_value.tushare_stock_basic
        where list_date > $1
        "#,
    )
    .bind(end)
    .fetch_one(&pool)
    .await?;
    let future_rows: i64 = future_stock_row.try_get("rows")?;
    let min_list_date: Option<String> = future_stock_row.try_get("min_list_date")?;
    let max_list_date: Option<String> = future_stock_row.try_get("max_list_date")?;
    println!("\nstock_basic 截面提示:");
    println!(
        "  list_date > {end}: rows={future_rows}, min={min_list_date:?}, max={max_list_date:?}"
    );

    let non_stock_limit_row = sqlx::query(
        r#"
        select count(distinct s.ts_code) as codes
        from deep_value.tushare_stk_limit s
        left join deep_value.tushare_stock_basic b on b.ts_code = s.ts_code
        where b.ts_code is null
        "#,
    )
    .fetch_one(&pool)
    .await?;
    let non_stock_limit_codes: i64 = non_stock_limit_row.try_get("codes")?;
    println!("\nstk_limit 代码域提示:");
    println!("  not_in_stock_basic_distinct_codes={non_stock_limit_codes}");

    Ok(())
}

async fn cmd_global_data_quality(
    pool: &sqlx::PgPool,
    market: &str,
    start: &str,
    end: &str,
) -> Result<()> {
    let (prefix, cal_table, basic_table, daily_table, daily_adj_table, adjfactor_table, fin_table) =
        match market.to_ascii_lowercase().as_str() {
            "hk" | "h" | "hongkong" | "hong-kong" => (
                "hk",
                "tushare_hk_trade_cal",
                "tushare_hk_basic",
                "tushare_hk_daily",
                "tushare_hk_daily_adj",
                "tushare_hk_adjfactor",
                "tushare_hk_financial_rows",
            ),
            "us" | "usa" | "u.s." => (
                "us",
                "tushare_us_trade_cal",
                "tushare_us_basic",
                "tushare_us_daily",
                "tushare_us_daily_adj",
                "tushare_us_adjfactor",
                "tushare_us_financial_rows",
            ),
            _ => anyhow::bail!("未知数据质量检查市场: {market}，可选: a | hk | us"),
        };

    println!("{prefix} 数据质量检查: {start}..{end}");

    let like_pattern = format!("{prefix}_%");
    let job_rows = sqlx::query(
        r#"
        select status, count(*) as jobs
        from deep_value.tushare_sync_jobs
        where api_name like $1
        group by status
        order by status
        "#,
    )
    .bind(like_pattern)
    .fetch_all(pool)
    .await?;
    println!("\n同步任务状态:");
    for row in job_rows {
        let status: String = row.try_get("status")?;
        let jobs: i64 = row.try_get("jobs")?;
        println!("  {status}: {jobs}");
    }

    let basic_sql = format!(
        r#"
        select count(*) as rows,
               count(ts_code) as non_null_codes,
               min(nullif(list_date, 'NaT')) as min_list_date,
               max(coalesce(nullif(nullif(delist_date,''), 'NaT'), nullif(list_date, 'NaT'))) as max_known_date
        from deep_value.{basic_table}
        "#
    );
    let basic = sqlx::query(&basic_sql).fetch_one(pool).await?;
    println!("\n基础信息:");
    println!(
        "  rows={}, non_null_codes={}, min_list_date={:?}, max_known_date={:?}",
        basic.try_get::<i64, _>("rows")?,
        basic.try_get::<i64, _>("non_null_codes")?,
        basic.try_get::<Option<String>, _>("min_list_date")?,
        basic.try_get::<Option<String>, _>("max_known_date")?
    );

    println!("\n日线类缺失开市日:");
    for (name, table) in [
        ("daily", daily_table),
        ("daily_adj", daily_adj_table),
        ("adjfactor", adjfactor_table),
    ] {
        let sql = format!(
            r#"
            with open_dates as (
                select cal_date
                from deep_value.{cal_table}
                where is_open = '1'
                  and cal_date between $1 and $2
            )
            select count(*) as missing_open_dates,
                   min(cal_date) as first_missing,
                   max(cal_date) as last_missing
            from open_dates o
            left join (select distinct trade_date from deep_value.{table}) d
              on d.trade_date = o.cal_date
            where d.trade_date is null
            "#,
        );
        let row = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .fetch_one(pool)
            .await?;
        println!(
            "  {name}: missing={}, first={:?}, last={:?}",
            row.try_get::<i64, _>("missing_open_dates")?,
            row.try_get::<Option<String>, _>("first_missing")?,
            row.try_get::<Option<String>, _>("last_missing")?
        );
    }

    let coverage_sql = format!(
        r#"
        select '{daily_table}' as table_name, count(*) as rows, min(trade_date) as min_date, max(trade_date) as max_date from deep_value.{daily_table}
        union all select '{daily_adj_table}', count(*), min(trade_date), max(trade_date) from deep_value.{daily_adj_table}
        union all select '{adjfactor_table}', count(*), min(trade_date), max(trade_date) from deep_value.{adjfactor_table}
        order by table_name
        "#
    );
    let coverage_rows = sqlx::query(&coverage_sql).fetch_all(pool).await?;
    println!("\n行情覆盖:");
    for row in coverage_rows {
        println!(
            "  {}: rows={}, min={:?}, max={:?}",
            row.try_get::<String, _>("table_name")?,
            row.try_get::<i64, _>("rows")?,
            row.try_get::<Option<String>, _>("min_date")?,
            row.try_get::<Option<String>, _>("max_date")?
        );
    }

    let fin_sql = format!(
        r#"
        select api_name, count(*) as rows, min(end_date) as min_end_date, max(end_date) as max_end_date
        from deep_value.{fin_table}
        group by api_name
        order by api_name
        "#
    );
    let fin_rows = sqlx::query(&fin_sql).fetch_all(pool).await?;
    println!("\n财务覆盖:");
    for row in fin_rows {
        println!(
            "  {}: rows={}, min={:?}, max={:?}",
            row.try_get::<String, _>("api_name")?,
            row.try_get::<i64, _>("rows")?,
            row.try_get::<Option<String>, _>("min_end_date")?,
            row.try_get::<Option<String>, _>("max_end_date")?
        );
    }

    Ok(())
}

/// 预取 Tushare 数据到 PostgreSQL typed 表。
async fn cmd_sync(
    market: &str,
    start: &str,
    end: &str,
    anniversary: Option<&str>,
    delay_ms: u64,
    lookback_years: usize,
    scope: &str,
    max_codes: Option<usize>,
    incremental: bool,
    sync_mode: &str,
) -> Result<()> {
    let config = AppConfig::load()?;
    let pool = db::connect(&config.database_url).await?;
    db::init_schema(&pool).await?;
    let cache = PgCache::new(pool);
    let client = TushareClient::with_pg_cache(&config.tushare_token, cache);

    let normalized_market = market.to_ascii_lowercase();
    let is_a_share = matches!(
        normalized_market.as_str(),
        "a" | "cn" | "ashare" | "a-share"
    );

    let stats = if is_a_share {
        if incremental {
            sync::run_sync_incremental(
                &client,
                client.pg_cache().unwrap(),
                sync_mode,
                end,
                delay_ms,
            )
            .await?
        } else {
            let ann = anniversary.unwrap_or(&end[4..]);
            sync::run_sync(
                &client,
                client.pg_cache().unwrap(),
                start,
                end,
                ann,
                lookback_years,
                delay_ms,
            )
            .await?
        }
    } else {
        if incremental {
            anyhow::bail!(
                "HK/US 暂不支持 --incremental，请使用 --scope meta|market|financial|all 断点续跑"
            );
        }
        sync::run_global_sync(
            &client,
            client.pg_cache().unwrap(),
            market,
            scope,
            start,
            end,
            delay_ms,
            max_codes,
        )
        .await?
    };

    println!(
        "Sync complete: {} calls, {} rows, {} skipped, {:.1}s, {} errors",
        stats.total_calls, stats.total_rows, stats.skipped, stats.elapsed_secs, stats.errors
    );
    if stats.errors > 0 {
        anyhow::bail!(
            "同步完成但有 {} 个 API 调用失败，数据可能不完整。检查日志中的 warn 条目。",
            stats.errors
        );
    }
    Ok(())
}

/// 季度再平衡回测。
async fn cmd_backtest(
    start: &str,
    end: &str,
    top: usize,
    local: bool,
    skip_pb_check: bool,
) -> Result<()> {
    let config = AppConfig::load()?;
    let pool = db::connect(&config.database_url).await?;
    db::init_schema(&pool).await?;
    let cache = PgCache::new(pool);

    if !local {
        anyhow::bail!("回测目前仅支持 --local 模式。请先运行 sync 初始化数据。");
    }

    let result = run_backtest_local(&cache, start, end, top, skip_pb_check).await?;
    println!("{}", formatter::format_backtest(&result));
    Ok(())
}

async fn run_backtest_local(
    cache: &PgCache,
    start_date: &str,
    end_date: &str,
    top: usize,
    skip_pb_check: bool,
) -> Result<engine::BacktestResult> {
    let cost = CostConfig::default();

    // 使用已有的 daily_basic 周年日作为再平衡点
    let rebalance_dates = pick_available_rebalance_dates(cache, start_date, end_date).await?;
    info!(
        count = rebalance_dates.len(),
        first = rebalance_dates.first().map(|s| s.as_str()).unwrap_or(""),
        last = rebalance_dates.last().map(|s| s.as_str()).unwrap_or(""),
        "再平衡日期（daily_basic 可用日）"
    );

    // 每期选股
    let mut period_holdings: Vec<(String, Vec<String>)> = Vec::new();
    let mut all_codes: Vec<String> = Vec::new();
    for date in &rebalance_dates {
        let snap = run_ashare_snapshot_local(cache, date, top, skip_pb_check).await?;
        let codes: Vec<String> = snap.holdings.iter().map(|h| h.ts_code.clone()).collect();
        for c in &codes {
            if !all_codes.contains(c) {
                all_codes.push(c.clone());
            }
        }
        period_holdings.push((date.clone(), codes));
    }

    // 加载全区间价格
    let prices = local::get_daily_prices(cache, &all_codes, start_date, end_date).await?;
    let benchmark = local::get_index_daily(cache, "000300.SH", start_date, end_date).await?;

    // 逐期计算收益
    let mut nav = 1.0;
    let mut nav_series: Vec<f64> = vec![nav];
    let mut nav_dates: Vec<String> = vec![rebalance_dates[0].clone()];
    let mut period_returns: Vec<engine::PeriodReturn> = Vec::new();
    let mut all_holding_returns: Vec<deep_value::strategy::domain::HoldingReturn> = Vec::new();

    for i in 0..period_holdings.len() {
        let (ref p_start, ref codes) = period_holdings[i];
        let p_end: String = if i + 1 < period_holdings.len() {
            period_holdings[i + 1].0.clone()
        } else {
            end_date.to_string()
        };

        let (gross_ret, hrs) = engine::compute_period_return(codes, &prices, p_start, &p_end)?;
        all_holding_returns.extend(hrs);

        let (added, removed) = if i == 0 {
            (codes.len(), 0)
        } else {
            let prev = &period_holdings[i - 1].1;
            let added = codes.iter().filter(|c| !prev.contains(c)).count();
            let removed = prev.iter().filter(|c| !codes.contains(c)).count();
            (added, removed)
        };
        let turnover = metrics::turnover_ratio(added, removed, codes.len());
        let cost_pct = metrics::transaction_cost(
            added,
            removed,
            codes.len(),
            cost.commission_rate,
            cost.stamp_tax,
            cost.slippage,
        );
        let net_ret = (1.0 + gross_ret) * (1.0 - cost_pct) - 1.0;

        nav *= 1.0 + net_ret;
        nav_series.push(nav);
        nav_dates.push(p_end.clone());

        period_returns.push(engine::PeriodReturn {
            date: p_start.clone(),
            end_date: p_end.clone(),
            gross_return: gross_ret,
            cost: cost_pct,
            net_return: net_ret,
            turnover,
            holdings_count: codes.len(),
            added,
            removed,
        });
    }

    // 基准净值
    let bm_prices: Vec<f64> = benchmark
        .column("benchmark_close")?
        .f64()?
        .into_iter()
        .filter_map(|v| v)
        .collect();
    let bm_first = bm_prices.first().copied().unwrap_or(1.0);
    let bm_nav: Vec<f64> = bm_prices.iter().map(|p| p / bm_first).collect();

    // 用实际日期间隔天数计算年化指标
    let total_days = days_between(start_date, end_date).max(1);
    let ann_ret = metrics::annualized_return(*nav_series.last().unwrap_or(&1.0) - 1.0, total_days);
    let bm_total = *bm_nav.last().unwrap_or(&1.0) - 1.0;
    let bm_ann = metrics::annualized_return(bm_total, total_days);

    let daily_returns: Vec<f64> = nav_series.windows(2).map(|w| w[1] / w[0] - 1.0).collect();
    let vol = metrics::annualized_volatility(&daily_returns);
    let sharpe = metrics::sharpe_ratio(&daily_returns, 0.03);
    let (max_dd, dd_start_idx, dd_end_idx) = metrics::max_drawdown(&nav_series);
    let calmar = metrics::calmar_ratio(ann_ret, max_dd);
    let wins = all_holding_returns
        .iter()
        .filter(|h| h.holding_return > 0.0)
        .count();
    let win_rate = if all_holding_returns.is_empty() {
        0.0
    } else {
        wins as f64 / all_holding_returns.len() as f64
    };
    let total_turnover: f64 = period_returns.iter().map(|p| p.turnover).sum::<f64>() * 100.0;
    let total_cost: f64 = period_returns.iter().map(|p| p.cost).sum::<f64>() * 100.0;

    let metrics = deep_value::strategy::domain::BacktestMetrics {
        total_return: (*nav_series.last().unwrap_or(&1.0) - 1.0),
        annualized_return: ann_ret,
        benchmark_total_return: bm_total,
        benchmark_annualized: bm_ann,
        excess_return: ann_ret - bm_ann,
        max_drawdown: max_dd,
        max_drawdown_start: nav_dates.get(dd_start_idx).cloned().unwrap_or_default(),
        max_drawdown_end: nav_dates.get(dd_end_idx).cloned().unwrap_or_default(),
        sharpe_ratio: sharpe,
        calmar_ratio: calmar,
        volatility: vol,
        win_rate,
        total_turnover,
        total_cost,
        num_rebalances: period_holdings.len(),
        avg_holding_days: 0.0,
    };

    Ok(engine::BacktestResult {
        metrics,
        nav_series: df!(
            "date" => nav_dates,
            "nav" => nav_series,
        )?,
        period_returns,
        holding_returns: all_holding_returns,
    })
}

fn days_between(start: &str, end: &str) -> usize {
    use chrono::NaiveDate;
    let s = NaiveDate::parse_from_str(start, "%Y%m%d").unwrap();
    let e = NaiveDate::parse_from_str(end, "%Y%m%d").unwrap();
    (e - s).num_days().max(1) as usize
}

async fn pick_available_rebalance_dates(
    cache: &PgCache,
    start: &str,
    end: &str,
) -> Result<Vec<String>> {
    let trade_dates = local::get_trade_cal(cache, start, end).await?;
    let mut dates: Vec<String> = Vec::new();
    for d in &trade_dates {
        if d.as_str() < start || d.as_str() >= end {
            continue;
        }
        let params = std::collections::HashMap::from([("trade_date".to_string(), d.clone())]);
        if let Ok(Some(df)) = cache
            .load_typed("daily_basic", &params, Some("ts_code"))
            .await
        {
            if df.height() > 0 {
                dates.push(d.clone());
            }
        }
    }
    Ok(dates)
}

/// 单期真实选股快照。
async fn cmd_snapshot(date: &str, top: usize, skip_pb_check: bool, local: bool) -> Result<()> {
    let config = AppConfig::load()?;

    let result = if local {
        let pool = db::connect(&config.database_url).await?;
        db::init_schema(&pool).await?;
        let cache = PgCache::new(pool);
        run_ashare_snapshot_local(&cache, date, top, skip_pb_check).await?
    } else {
        let client =
            TushareClient::new_with_pg(&config.tushare_token, &config.database_url).await?;
        run_ashare_snapshot(&client, date, top, skip_pb_check).await?
    };

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

/// 从本地 PostgreSQL typed 表执行选股快照（无需 Tushare API）。
async fn run_ashare_snapshot_local(
    cache: &PgCache,
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

    let market_pb = local::get_market_pb_median(cache, trade_date).await?;
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

    let mut df = local::build_cross_section(cache, trade_date).await?;

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
    let pb_10y = local::get_10y_pb_max(cache, trade_date, config.lookback_years).await?;
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
            "逐只拉取财务/审计数据前，按初筛打分和行业分散保留前 {} 只候选",
            financial_pool_size
        ));
    }

    // Bulk load from typed tables (no per-stock API calls)
    let net_equity = local::get_net_equity(cache, trade_date).await?;
    df = df
        .lazy()
        .join(
            net_equity.lazy(),
            [col("ts_code")],
            [col("ts_code")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    let audit_info = local::get_audit_info(cache, trade_date).await?;
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
    let _anomaly_codes = ts_codes_from_df(&df)?;
    let current_income = local::get_current_year_income(cache, trade_date).await?;
    let current_dividend = local::get_current_year_dividend(cache, trade_date).await?;
    let income_10y = local::get_10y_income(cache, trade_date, config.lookback_years).await?;
    let dividend_10y = local::get_10y_dividend(cache, trade_date, config.lookback_years).await?;
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
                &[("ts_code", code.as_str())],
                Some("ts_code,end_date,cash_div_tax"),
            )
            .await?;
        let total = sum_div_for_period(&df, &period);
        out_codes.push(code.clone());
        values.push(total);
    }
    Ok(df!("ts_code" => out_codes, "current_dividend_total" => values)?)
}

fn sum_div_for_period(df: &DataFrame, period: &str) -> f64 {
    if df.height() == 0 {
        return 0.0;
    }
    let dates = df.column("end_date").ok().and_then(|c| c.str().ok());
    let divs = df.column("cash_div_tax").ok().and_then(|c| c.str().ok());
    let (Some(dates), Some(divs)) = (dates, divs) else {
        return 0.0;
    };
    let mut total = 0.0;
    for i in 0..df.height() {
        if dates.get(i) == Some(period) {
            total += divs
                .get(i)
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
        }
    }
    total
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
        let df = client
            .query(
                "dividend",
                &[("ts_code", code.as_str())],
                Some("ts_code,end_date,cash_div_tax"),
            )
            .await?;
        let mut total = 0.0;
        for year in start_year..=safe_year {
            let period = format!("{year}1231");
            total += sum_div_for_period(&df, &period);
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
