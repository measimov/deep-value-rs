//! Tushare 数据同步管线。
//!
//! 全量同步: `run_sync` — 拉取日期范围内所有数据。
//! 增量同步: `run_sync_incremental` — 按模式只补缺失部分。
//!
//! 速率限制匹配 5000-point 等级：~500 req/min（默认 ~120ms 间隔）。

use std::collections::HashSet;
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::info;

use crate::data::financials::safe_financial_year;
use crate::tushare::client::{RateLimiter, TushareClient};
use crate::tushare::pg_cache::PgCache;

/// 同步统计。
#[derive(Debug, Default)]
pub struct SyncStats {
    pub total_calls: usize,
    pub total_rows: usize,
    pub elapsed_secs: f64,
    pub errors: usize,
    pub skipped: usize,
}

// =========================================================================
// 全量同步
// =========================================================================

/// 全量同步：拉取 start..end 范围内所有数据到 typed 表。
pub async fn run_sync(
    client: &TushareClient,
    start_date: &str,
    end_date: &str,
    _anniversary_mmdd: &str,
    delay_ms: u64,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    // 1. stock_basic
    force_step(
        client,
        &limiter,
        &mut stats,
        "stock_basic",
        &[("list_status", "L")],
        Some("ts_code,name,industry,list_status,list_date"),
    )
    .await?;

    let codes = fetch_listed_codes(client, &limiter, &mut stats).await?;
    if codes.is_empty() {
        stats.elapsed_secs = started.elapsed().as_secs_f64();
        return Ok(stats);
    }

    // 2. daily_basic (monthly snapshots for backtest rebalancing + 10y PB max)
    let start_year: i32 = start_date[..4].parse().unwrap_or(2015);
    let end_year: i32 = end_date[..4].parse().unwrap_or(2025);
    let pb_start_year = (start_year - 9).max(1990);
    let mut daily_basic_dates: Vec<String> = Vec::new();

    for year in pb_start_year..=end_year {
        for month in 1..=12 {
            daily_basic_dates.push(format!("{year}{month:02}15"));
        }
    }
    // dedup + sort
    daily_basic_dates.sort();
    daily_basic_dates.dedup();

    for date in &daily_basic_dates {
        force_step(
            client,
            &limiter,
            &mut stats,
            "daily_basic",
            &[("trade_date", date.as_str())],
            Some("ts_code,pb,pe_ttm,dv_ratio,total_mv"),
        )
        .await?;
    }

    // 3-5. income_vip, balancesheet_vip, fina_indicator_vip
    let fin_start = safe_financial_year(start_date) - 9;
    let fin_end = safe_financial_year(end_date);
    for year in fin_start..=fin_end {
        let period = format!("{year}1231");
        force_step(client, &limiter, &mut stats, "income_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some("ts_code,end_date,n_income")).await?;
        force_step(client, &limiter, &mut stats, "balancesheet_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some("ts_code,end_date,total_hldr_eqy_exc_min_int")).await?;
        force_step(client, &limiter, &mut stats, "fina_indicator_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some("ts_code,end_date,roe,roa,grossprofit_margin,netprofit_margin,debt_to_assets,current_ratio,bps,eps,cfps,or_yoy,profit_dedt")).await?;
        force_step(client, &limiter, &mut stats, "cashflow_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some("ts_code,end_date,n_cashflow_act")).await?;
    }

    // disclosure_date (latest period only)
    let disc_period = format!("{}1231", fin_end);
    force_step(client, &limiter, &mut stats, "disclosure_date",
        &[("end_date", disc_period.as_str())],
        Some("ts_code,end_date,ann_date,actual_date")).await?;

    // 6. fina_audit (per stock, latest period)
    let audit_period = format!("{}1231", fin_end);
    for code in &codes {
        force_step(client, &limiter, &mut stats, "fina_audit",
            &[("ts_code", code.as_str()), ("period", audit_period.as_str())],
            Some("ts_code,audit_agency")).await?;
    }

    // 7. dividend (per stock, full history)
    for code in &codes {
        force_step(client, &limiter, &mut stats, "dividend",
            &[("ts_code", code.as_str())],
            Some("ts_code,end_date,cash_div_tax,stk_div")).await?;
    }

    // 8. trade_cal
    force_step(client, &limiter, &mut stats, "trade_cal",
        &[("exchange", "SSE"), ("start_date", start_date), ("end_date", end_date), ("is_open", "1")],
        Some("exchange,cal_date,is_open")).await?;

    // 9. daily + adj_factor (monthly over full range for backtest pricing)
    for date in &daily_basic_dates {
        force_step(client, &limiter, &mut stats, "daily",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,close")).await?;
        force_step(client, &limiter, &mut stats, "adj_factor",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,adj_factor")).await?;
    }

    // 10. index_daily (benchmark: 000300.SH)
    force_step(client, &limiter, &mut stats, "index_daily",
        &[("ts_code", "000300.SH"), ("start_date", start_date), ("end_date", end_date)],
        Some("ts_code,trade_date,close")).await?;

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(calls = stats.total_calls, rows = stats.total_rows, errors = stats.errors, elapsed = stats.elapsed_secs, "全量同步完成");
    Ok(stats)
}

// =========================================================================
// 增量同步
// =========================================================================

/// 增量同步：按模式仅补缺失数据。
///
/// `mode`: `"daily"` | `"financial"` | `"meta"`
/// `end_date`: 最新参考日期 (YYYYMMDD)
pub async fn run_sync_incremental(
    client: &TushareClient,
    cache: &PgCache,
    mode: &str,
    end_date: &str,
    delay_ms: u64,
) -> Result<SyncStats> {
    match mode {
        "daily" => sync_daily_incremental(client, cache, end_date, delay_ms).await,
        "financial" => sync_financial_incremental(client, cache, end_date, delay_ms).await,
        "meta" => sync_meta_incremental(client, delay_ms).await,
        _ => anyhow::bail!("未知增量模式: {mode}，可选: daily | financial | meta"),
    }
}

/// 增量 — 每日行情：补拉最近 N 个交易日缺失的 daily_basic / daily / adj_factor / index_daily。
async fn sync_daily_incremental(
    client: &TushareClient,
    cache: &PgCache,
    end_date: &str,
    delay_ms: u64,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    // 获取最近 5 个交易日
    let all_trade_dates = get_trade_dates(client, &limiter, &mut stats, end_date, 10).await?;
    let recent: Vec<&String> = all_trade_dates.iter().rev().take(5).collect();
    if recent.is_empty() {
        info!("无交易日数据，跳过 daily 增量");
        stats.elapsed_secs = started.elapsed().as_secs_f64();
        return Ok(stats);
    }

    // 检查 daily_basic 缺失
    let existing_dates = cache.existing_daily_basic_dates().await?;
    for date in &recent {
        if existing_dates.contains(*date) {
            stats.skipped += 1;
            continue;
        }
        force_step(client, &limiter, &mut stats, "daily_basic",
            &[("trade_date", date.as_str())],
            Some("ts_code,pb,pe_ttm,dv_ratio,total_mv")).await?;
    }

    // 拉取最近 2 个交易日的 daily / adj_factor（全量）
    for date in recent.iter().take(2) {
        force_step(client, &limiter, &mut stats, "daily",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,close")).await?;
        force_step(client, &limiter, &mut stats, "adj_factor",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,adj_factor")).await?;
    }

    // index_daily — 最近 30 天
    let first_date = recent.last().map(|s| s.to_string()).unwrap_or_default();
    force_step(client, &limiter, &mut stats, "index_daily",
        &[("ts_code", "000300.SH"), ("start_date", first_date.as_str()), ("end_date", end_date)],
        Some("ts_code,trade_date,close")).await?;

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(calls = stats.total_calls, skipped = stats.skipped, elapsed = stats.elapsed_secs, "daily 增量完成");
    Ok(stats)
}

/// 增量 — 财务数据：补拉缺失 period 的 VIP 接口 + 新上市股票的 audit/dividend。
async fn sync_financial_incremental(
    client: &TushareClient,
    cache: &PgCache,
    end_date: &str,
    delay_ms: u64,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    // 计算理论需要的 period 范围
    let fin_end = safe_financial_year(end_date);
    let fin_start = fin_end - 9;

    // 检查各表已有 period（三表独立判断，避免部分缺失时跳过）
    let income_periods = cache.existing_income_periods().await?;
    let balance_periods = cache.existing_balancesheet_periods().await?;
    let indicator_periods = cache.existing_fina_indicator_periods().await?;
    let cashflow_periods = cache.existing_cashflow_periods().await?;

    for year in fin_start..=fin_end {
        let period = format!("{year}1231");
        if !income_periods.contains(&period) {
            force_step(client, &limiter, &mut stats, "income_vip",
                &[("period", period.as_str()), ("report_type", "1")],
                Some("ts_code,end_date,n_income")).await?;
        } else {
            stats.skipped += 1;
        }
        if !balance_periods.contains(&period) {
            force_step(client, &limiter, &mut stats, "balancesheet_vip",
                &[("period", period.as_str()), ("report_type", "1")],
                Some("ts_code,end_date,total_hldr_eqy_exc_min_int")).await?;
        } else {
            stats.skipped += 1;
        }
        if !indicator_periods.contains(&period) {
            force_step(client, &limiter, &mut stats, "fina_indicator_vip",
                &[("period", period.as_str()), ("report_type", "1")],
                Some("ts_code,end_date,roe,roa,grossprofit_margin,netprofit_margin,debt_to_assets")).await?;
        } else {
            stats.skipped += 1;
        }
        if !cashflow_periods.contains(&period) {
            force_step(client, &limiter, &mut stats, "cashflow_vip",
                &[("period", period.as_str()), ("report_type", "1")],
                Some("ts_code,end_date,n_cashflow_act")).await?;
        } else {
            stats.skipped += 1;
        }
    }

    // 新上市股票：fina_audit + dividend
    let all_codes = fetch_listed_codes(client, &limiter, &mut stats).await?;
    let audit_period = format!("{}1231", fin_end);
    let existing_audit = cache.existing_audit_codes(&audit_period).await?;
    let existing_div = cache.existing_dividend_codes().await?;

    for code in &all_codes {
        if !existing_audit.contains(code) {
            force_step(client, &limiter, &mut stats, "fina_audit",
                &[("ts_code", code.as_str()), ("period", audit_period.as_str())],
                Some("ts_code,audit_agency")).await?;
        } else {
            stats.skipped += 1;
        }
        if !existing_div.contains(code) {
            force_step(client, &limiter, &mut stats, "dividend",
                &[("ts_code", code.as_str())],
                Some("ts_code,end_date,cash_div_tax,stk_div")).await?;
        } else {
            stats.skipped += 1;
        }
    }

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(calls = stats.total_calls, skipped = stats.skipped, elapsed = stats.elapsed_secs, "financial 增量完成");
    Ok(stats)
}

/// 增量 — 元数据：stock_basic + 今年 trade_cal。
async fn sync_meta_incremental(
    client: &TushareClient,
    delay_ms: u64,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    let end_year: i32 = chrono::Utc::now().format("%Y").to_string().parse().unwrap_or(2026);

    force_step(client, &limiter, &mut stats, "stock_basic",
        &[("list_status", "L")],
        Some("ts_code,name,industry,list_status,list_date")).await?;

    force_step(client, &limiter, &mut stats, "trade_cal",
        &[("exchange", "SSE"),
          ("start_date", format!("{end_year}0101").as_str()),
          ("end_date", format!("{end_year}1231").as_str()),
          ("is_open", "1")],
        Some("exchange,cal_date,is_open")).await?;

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(calls = stats.total_calls, skipped = stats.skipped, elapsed = stats.elapsed_secs, "meta 增量完成");
    Ok(stats)
}

// =========================================================================
// Helpers
// =========================================================================

async fn force_step(
    client: &TushareClient,
    limiter: &RateLimiter,
    stats: &mut SyncStats,
    api_name: &str,
    params: &[(&str, &str)],
    fields: Option<&str>,
) -> Result<()> {
    limiter.wait_if_needed().await;
    match client.query_force(api_name, params, fields).await {
        Ok(df) => {
            stats.total_calls += 1;
            stats.total_rows += df.height();
            info!(api = api_name, rows = df.height(), "sync");
        }
        Err(e) => {
            stats.total_calls += 1;
            stats.errors += 1;
            tracing::warn!(api = api_name, error = %e, "sync 调用失败，继续");
        }
    }
    Ok(())
}

async fn fetch_listed_codes(
    client: &TushareClient,
    limiter: &RateLimiter,
    stats: &mut SyncStats,
) -> Result<Vec<String>> {
    limiter.wait_if_needed().await;
    let df = client
        .query_force("stock_basic", &[("list_status", "L")], Some("ts_code"))
        .await
        .context("获取上市股票列表失败")?;
    stats.total_calls += 1;
    stats.total_rows += df.height();
    let codes: Vec<String> = df
        .column("ts_code")
        .ok()
        .and_then(|col| col.str().ok())
        .map(|col| col.into_iter().filter_map(|v| v.map(String::from)).collect())
        .unwrap_or_default();
    let mut unique: Vec<String> = codes.into_iter().collect::<HashSet<_>>().into_iter().collect();
    unique.sort();
    info!(count = unique.len(), "上市股票列表");
    Ok(unique)
}

async fn get_trade_dates(
    client: &TushareClient,
    limiter: &RateLimiter,
    stats: &mut SyncStats,
    end_date: &str,
    limit: usize,
) -> Result<Vec<String>> {
    let start_date = shift_date(end_date, -(limit as i32 * 2));
    limiter.wait_if_needed().await;
    let df = client
        .query_force("trade_cal",
            &[("exchange", "SSE"), ("start_date", start_date.as_str()), ("end_date", end_date), ("is_open", "1")],
            Some("cal_date"))
        .await?;
    stats.total_calls += 1;
    stats.total_rows += df.height();
    let mut dates: Vec<String> = df
        .column("cal_date").ok()
        .and_then(|c| c.str().ok())
        .map(|c| c.into_iter().filter_map(|v| v.map(String::from)).collect())
        .unwrap_or_default();
    dates.sort();
    let drop = dates.len().saturating_sub(limit);
    if drop > 0 {
        dates.drain(..drop);
    }
    Ok(dates)
}

fn shift_date(date: &str, days: i32) -> String {
    use chrono::NaiveDate;
    NaiveDate::parse_from_str(date, "%Y%m%d")
        .map(|d| d + chrono::Duration::days(days as i64))
        .map(|d| d.format("%Y%m%d").to_string())
        .unwrap_or_else(|_| date.to_string())
}
