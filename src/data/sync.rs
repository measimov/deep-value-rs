//! Tushare 数据同步管线。
//!
//! 全量同步: `run_sync` — 拉取日期范围内所有数据。
//! 增量同步: `run_sync_incremental` — 按模式只补缺失部分。
//!
//! 默认速率限制约 100 req/min（600ms 间隔），低于 2000-point 档频次上限。

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use polars::prelude::DataFrame;
use tracing::info;

use crate::data::financials::safe_financial_year;
use crate::tushare::client::{RateLimiter, TushareClient};
use crate::tushare::pg_cache::PgCache;

const STOCK_BASIC_FIELDS: &str = "ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type";
const DAILY_BASIC_FIELDS: &str = "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv";
const DISCLOSURE_DATE_FIELDS: &str = "ts_code,end_date,ann_date,actual_date,pre_date,modify_date";
const TRADE_CAL_FIELDS: &str = "exchange,cal_date,is_open,pretrade_date";
const STK_LIMIT_FIELDS: &str = "ts_code,trade_date,pre_close,up_limit,down_limit";
const TUSHARE_ALL_FIELDS: &str = "";
const INCOME_FIELDS: &str = TUSHARE_ALL_FIELDS;
const BALANCESHEET_FIELDS: &str = TUSHARE_ALL_FIELDS;
const CASHFLOW_FIELDS: &str = TUSHARE_ALL_FIELDS;
const FINA_INDICATOR_FIELDS: &str = TUSHARE_ALL_FIELDS;
const FORECAST_FIELDS: &str = "ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason";
const EXPRESS_FIELDS: &str = "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps,perf_summary,is_audit,remark";
const FINA_MAINBZ_FIELDS: &str =
    "ts_code,end_date,bz_item,bz_code,bz_sales,bz_profit,bz_cost,curr_type,update_flag";
const INDEX_WEIGHT_FIELDS: &str = "index_code,con_code,trade_date,weight";
const TOP10_HOLDER_FIELDS: &str = "ts_code,ann_date,end_date,holder_name,hold_amount,hold_ratio,hold_float_ratio,hold_change,holder_type";
const PLEDGE_STAT_FIELDS: &str =
    "ts_code,end_date,pledge_count,unrest_pledge,rest_pledge,total_share,pledge_ratio";
const REPURCHASE_FIELDS: &str =
    "ts_code,ann_date,end_date,proc,exp_date,vol,amount,high_limit,low_limit";
const INDEX_WEIGHT_CODES: &[&str] = &["000300.SH", "399300.SZ"];

const HK_BASIC_FIELDS: &str =
    "ts_code,name,fullname,enname,cn_spell,market,list_status,list_date,delist_date,trade_unit,isin,curr_type";
const US_BASIC_FIELDS: &str = "ts_code,name,enname,classify,list_date,delist_date";
const GLOBAL_TRADE_CAL_FIELDS: &str = "cal_date,is_open,pretrade_date";
const HK_DAILY_FIELDS: &str =
    "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount";
const HK_DAILY_ADJ_FIELDS: &str = "ts_code,trade_date,close,open,high,low,pre_close,change,pct_change,vol,amount,vwap,adj_factor,turnover_ratio,free_share,total_share,free_mv,total_mv";
const HK_ADJFACTOR_FIELDS: &str = "ts_code,trade_date,cum_adjfactor,close_price";
const US_DAILY_FIELDS: &str = "ts_code,trade_date,close,open,high,low,pre_close,change,pct_change,vol,amount,vwap,turnover_ratio,total_mv,pe,pb";
const US_DAILY_ADJ_FIELDS: &str = "ts_code,trade_date,close,open,high,low,pre_close,change,pct_change,vol,amount,vwap,adj_factor,turnover_ratio,free_share,total_share,free_mv,total_mv,exchange";
const US_ADJFACTOR_FIELDS: &str = "ts_code,trade_date,exchange,cum_adjfactor,close_price";
const GLOBAL_FINANCIAL_FIELDS: &str = TUSHARE_ALL_FIELDS;

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
///
/// 重跑和断点续传只依赖 `tushare_sync_jobs`，不依赖 typed 表里的局部数据。
pub async fn run_sync(
    client: &TushareClient,
    cache: &PgCache,
    start_date: &str,
    end_date: &str,
    _anniversary_mmdd: &str,
    lookback_years: usize,
    delay_ms: u64,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    // 1. stock_basic (L — listed stocks for universe)
    force_step(
        client,
        cache,
        &limiter,
        &mut stats,
        "stock_basic",
        &[("list_status", "L")],
        Some(STOCK_BASIC_FIELDS),
    )
    .await?;
    // 1b. stock_basic (D+P+G — non-listed statuses for survivorship-bias correction)
    for &st in &["D", "P", "G"] {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "stock_basic",
            &[("list_status", st)],
            Some(STOCK_BASIC_FIELDS),
        )
        .await?;
    }

    let codes = load_stock_codes(cache).await?;
    if codes.is_empty() {
        stats.elapsed_secs = started.elapsed().as_secs_f64();
        return Ok(stats);
    }

    let start_year: i32 = start_date[..4].parse().unwrap_or(2015);
    let end_year: i32 = end_date[..4].parse().unwrap_or(2025);

    let daily_basic_start_date = daily_basic_lookback_start_date(start_date, lookback_years);

    // 2. trade_cal first — daily_basic needs the lookback window for local PB checks.
    force_step(
        client,
        cache,
        &limiter,
        &mut stats,
        "trade_cal",
        &[
            ("exchange", "SSE"),
            ("start_date", daily_basic_start_date.as_str()),
            ("end_date", end_date),
            ("is_open", "1"),
        ],
        Some(TRADE_CAL_FIELDS),
    )
    .await?;
    let daily_basic_trade_dates =
        load_trade_dates(cache, daily_basic_start_date.as_str(), end_date).await?;
    if daily_basic_trade_dates.is_empty() {
        anyhow::bail!(
            "未能加载 {}..{} 的交易日历，无法生成 daily_basic lookback 任务",
            daily_basic_start_date,
            end_date
        );
    }
    let trade_dates = load_trade_dates(cache, start_date, end_date).await?;
    if trade_dates.is_empty() {
        anyhow::bail!("未能加载 {start_date}..{end_date} 的交易日历，无法生成全历史日频任务");
    }

    // 3. daily_basic — include lookback history needed by local 10-year PB checks.
    for date in &daily_basic_trade_dates {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "daily_basic",
            &[("trade_date", date.as_str())],
            Some(DAILY_BASIC_FIELDS),
        )
        .await?;
    }

    // 4-7. VIP financials — every report period in the requested history window.
    for period in full_sync_financial_periods(start_date, end_date, lookback_years) {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "income_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(INCOME_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "balancesheet_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(BALANCESHEET_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "fina_indicator_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(FINA_INDICATOR_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "cashflow_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(CASHFLOW_FIELDS),
        )
        .await?;

        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "forecast_vip",
            &[("period", period.as_str())],
            Some(FORECAST_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "express_vip",
            &[("period", period.as_str())],
            Some(EXPRESS_FIELDS),
        )
        .await?;
        for &bz_type in &["P", "D", "I"] {
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "fina_mainbz_vip",
                &[("period", period.as_str()), ("type", bz_type)],
                Some(FINA_MAINBZ_FIELDS),
            )
            .await?;
        }
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "disclosure_date",
            &[("end_date", period.as_str())],
            Some(DISCLOSURE_DATE_FIELDS),
        )
        .await?;
    }

    // 8. fina_audit — all history per stock; local period is derived from end_date.
    for code in &codes {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "fina_audit",
            &[("ts_code", code.as_str())],
            Some("ts_code,audit_agency,ann_date,end_date,audit_result,audit_fees,audit_sign"),
        )
        .await?;
    }

    // 9. dividend — all history per stock.
    for code in &codes {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "dividend",
            &[("ts_code", code.as_str())],
            Some("ts_code,end_date,cash_div_tax,stk_div,record_date,ex_date,ann_date,div_proc"),
        )
        .await?;
    }

    // 10. shareholder, pledge, and ownership risk data.
    for code in &codes {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "top10_holders",
            &[
                ("ts_code", code.as_str()),
                ("start_date", start_date),
                ("end_date", end_date),
            ],
            Some(TOP10_HOLDER_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "top10_floatholders",
            &[
                ("ts_code", code.as_str()),
                ("start_date", start_date),
                ("end_date", end_date),
            ],
            Some(TOP10_HOLDER_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "pledge_stat",
            &[("ts_code", code.as_str())],
            Some(PLEDGE_STAT_FIELDS),
        )
        .await?;
    }

    // 11. daily + adj_factor + suspend/limit — full open-day coverage.
    for date in &trade_dates {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "daily",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,open,high,low,close,pre_close,pct_chg,change,vol,amount"),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "adj_factor",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,adj_factor"),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "suspend_d",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,suspend_type,suspend_timing"),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "stk_limit",
            &[("trade_date", date.as_str())],
            Some(STK_LIMIT_FIELDS),
        )
        .await?;
    }

    // 11. index_daily
    force_step(
        client,
        cache,
        &limiter,
        &mut stats,
        "index_daily",
        &[
            ("ts_code", "000300.SH"),
            ("start_date", start_date),
            ("end_date", end_date),
        ],
        Some("ts_code,trade_date,open,high,low,close,pre_close,pct_chg,change,vol,amount"),
    )
    .await?;

    // 12. index_weight — monthly windows keep each query below Tushare page caps.
    for year in start_year..=end_year {
        for month in 1..=12 {
            let (month_start, month_end) = month_window(year, month);
            if month_end.as_str() < start_date || month_start.as_str() > end_date {
                continue;
            }
            let window_start = if month_start.as_str() < start_date {
                start_date.to_string()
            } else {
                month_start
            };
            let window_end = if month_end.as_str() > end_date {
                end_date.to_string()
            } else {
                month_end
            };
            for &index_code in INDEX_WEIGHT_CODES {
                force_step(
                    client,
                    cache,
                    &limiter,
                    &mut stats,
                    "index_weight",
                    &[
                        ("index_code", index_code),
                        ("start_date", window_start.as_str()),
                        ("end_date", window_end.as_str()),
                    ],
                    Some(INDEX_WEIGHT_FIELDS),
                )
                .await?;
            }
        }
    }

    // 13. repurchase — annual windows avoid writing partial range results.
    for year in start_year..=end_year {
        let window_start = format!("{year}0101");
        let window_end = format!("{year}1231");
        let query_start = if window_start.as_str() < start_date {
            start_date.to_string()
        } else {
            window_start
        };
        let query_end = if window_end.as_str() > end_date {
            end_date.to_string()
        } else {
            window_end
        };
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "repurchase",
            &[
                ("start_date", query_start.as_str()),
                ("end_date", query_end.as_str()),
            ],
            Some(REPURCHASE_FIELDS),
        )
        .await?;
    }

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(
        calls = stats.total_calls,
        rows = stats.total_rows,
        skipped = stats.skipped,
        errors = stats.errors,
        elapsed = stats.elapsed_secs,
        "全量同步完成"
    );
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
        "meta" => sync_meta_incremental(client, cache, delay_ms).await,
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
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "daily_basic",
            &[("trade_date", date.as_str())],
            Some(DAILY_BASIC_FIELDS),
        )
        .await?;
    }

    // 拉取最近 2 个交易日的 daily / adj_factor（全量）
    for date in recent.iter().take(2) {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "daily",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,open,high,low,close,pre_close,pct_chg,change,vol,amount"),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "adj_factor",
            &[("trade_date", date.as_str())],
            Some("ts_code,trade_date,adj_factor"),
        )
        .await?;
    }

    // index_daily — 最近 30 天
    let first_date = recent.last().map(|s| s.to_string()).unwrap_or_default();
    force_step(
        client,
        cache,
        &limiter,
        &mut stats,
        "index_daily",
        &[
            ("ts_code", "000300.SH"),
            ("start_date", first_date.as_str()),
            ("end_date", end_date),
        ],
        Some("ts_code,trade_date,open,high,low,close,pre_close,pct_chg,change,vol,amount"),
    )
    .await?;

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(
        calls = stats.total_calls,
        skipped = stats.skipped,
        elapsed = stats.elapsed_secs,
        "daily 增量完成"
    );
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

    // 只依赖 sync job checkpoint 跳过，避免 typed 表局部写入导致断点续传误判。

    for period in incremental_financial_periods(end_date, 10) {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "income_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(INCOME_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "balancesheet_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(BALANCESHEET_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "fina_indicator_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(FINA_INDICATOR_FIELDS),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "cashflow_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some(CASHFLOW_FIELDS),
        )
        .await?;
    }

    // 新上市股票：fina_audit + dividend；是否已完成由 sync_jobs 判断。
    let all_codes = fetch_listed_codes(client, &limiter, &mut stats).await?;

    for code in &all_codes {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "fina_audit",
            &[("ts_code", code.as_str())],
            Some("ts_code,audit_agency,ann_date,end_date,audit_result,audit_fees,audit_sign"),
        )
        .await?;
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "dividend",
            &[("ts_code", code.as_str())],
            Some("ts_code,end_date,cash_div_tax,stk_div,record_date,ex_date,ann_date,div_proc"),
        )
        .await?;
    }

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(
        calls = stats.total_calls,
        skipped = stats.skipped,
        elapsed = stats.elapsed_secs,
        "financial 增量完成"
    );
    Ok(stats)
}

/// 增量 — 元数据：stock_basic + 今年 trade_cal。
async fn sync_meta_incremental(
    client: &TushareClient,
    cache: &PgCache,
    delay_ms: u64,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    let end_year: i32 = chrono::Utc::now()
        .format("%Y")
        .to_string()
        .parse()
        .unwrap_or(2026);

    for &st in &["L", "D", "P", "G"] {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "stock_basic",
            &[("list_status", st)],
            Some(STOCK_BASIC_FIELDS),
        )
        .await?;
    }

    force_step(
        client,
        cache,
        &limiter,
        &mut stats,
        "trade_cal",
        &[
            ("exchange", "SSE"),
            ("start_date", format!("{end_year}0101").as_str()),
            ("end_date", format!("{end_year}1231").as_str()),
            ("is_open", "1"),
        ],
        Some(TRADE_CAL_FIELDS),
    )
    .await?;

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(
        calls = stats.total_calls,
        skipped = stats.skipped,
        elapsed = stats.elapsed_secs,
        "meta 增量完成"
    );
    Ok(stats)
}

// =========================================================================
// 港股 / 美股同步
// =========================================================================

/// 同步港股或美股数据。
///
/// `market`: `hk` | `us`。
/// `scope`: `all` | `meta` | `market` | `financial`。
/// `market` scope 会包含 basic + trade calendar，因为交易日和代码域是生成任务的基础。
pub async fn run_global_sync(
    client: &TushareClient,
    cache: &PgCache,
    market: &str,
    scope: &str,
    start_date: &str,
    end_date: &str,
    delay_ms: u64,
    max_codes: Option<usize>,
) -> Result<SyncStats> {
    validate_global_scope(scope)?;
    match normalize_market(market) {
        Some("hk") => {
            run_hk_sync(
                client, cache, scope, start_date, end_date, delay_ms, max_codes,
            )
            .await
        }
        Some("us") => {
            run_us_sync(
                client, cache, scope, start_date, end_date, delay_ms, max_codes,
            )
            .await
        }
        _ => anyhow::bail!("未知市场: {market}，可选: hk | us"),
    }
}

async fn run_hk_sync(
    client: &TushareClient,
    cache: &PgCache,
    scope: &str,
    start_date: &str,
    end_date: &str,
    delay_ms: u64,
    max_codes: Option<usize>,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    if sync_scope_includes_meta(scope) {
        for &status in &["L", "D", "P"] {
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "hk_basic",
                &[("list_status", status)],
                Some(HK_BASIC_FIELDS),
            )
            .await?;
        }

        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "hk_tradecal",
            &[
                ("start_date", start_date),
                ("end_date", end_date),
                ("is_open", "1"),
            ],
            Some(GLOBAL_TRADE_CAL_FIELDS),
        )
        .await?;
    }

    if sync_scope_includes_market(scope) {
        let trade_dates =
            load_global_trade_dates(cache, "hk_tradecal", start_date, end_date).await?;
        if trade_dates.is_empty() {
            anyhow::bail!("未能加载港股 {start_date}..{end_date} 交易日历，无法生成日频任务");
        }
        for date in &trade_dates {
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "hk_daily",
                &[("trade_date", date.as_str())],
                Some(HK_DAILY_FIELDS),
            )
            .await?;
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "hk_daily_adj",
                &[("trade_date", date.as_str())],
                Some(HK_DAILY_ADJ_FIELDS),
            )
            .await?;
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "hk_adjfactor",
                &[("trade_date", date.as_str())],
                Some(HK_ADJFACTOR_FIELDS),
            )
            .await?;
        }
    }

    if sync_scope_includes_financial(scope) {
        let mut codes = load_global_stock_codes(cache, "hk_basic").await?;
        if let Some(max_codes) = max_codes {
            codes.truncate(max_codes);
        }
        if codes.is_empty() {
            anyhow::bail!("未能加载港股代码列表，无法生成财务任务");
        }
        for code in &codes {
            for &api_name in &[
                "hk_income",
                "hk_balancesheet",
                "hk_cashflow",
                "hk_fina_indicator",
            ] {
                force_step(
                    client,
                    cache,
                    &limiter,
                    &mut stats,
                    api_name,
                    &[
                        ("ts_code", code.as_str()),
                        ("start_date", start_date),
                        ("end_date", end_date),
                    ],
                    Some(GLOBAL_FINANCIAL_FIELDS),
                )
                .await?;
            }
        }
    }

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(
        calls = stats.total_calls,
        rows = stats.total_rows,
        skipped = stats.skipped,
        errors = stats.errors,
        elapsed = stats.elapsed_secs,
        "港股同步完成"
    );
    Ok(stats)
}

async fn run_us_sync(
    client: &TushareClient,
    cache: &PgCache,
    scope: &str,
    start_date: &str,
    end_date: &str,
    delay_ms: u64,
    max_codes: Option<usize>,
) -> Result<SyncStats> {
    let limiter = RateLimiter::new(delay_ms);
    let started = Instant::now();
    let mut stats = SyncStats::default();

    if sync_scope_includes_meta(scope) {
        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "us_basic",
            &[],
            Some(US_BASIC_FIELDS),
        )
        .await?;

        force_step(
            client,
            cache,
            &limiter,
            &mut stats,
            "us_tradecal",
            &[
                ("start_date", start_date),
                ("end_date", end_date),
                ("is_open", "1"),
            ],
            Some(GLOBAL_TRADE_CAL_FIELDS),
        )
        .await?;
    }

    if sync_scope_includes_market(scope) {
        let trade_dates =
            load_global_trade_dates(cache, "us_tradecal", start_date, end_date).await?;
        if trade_dates.is_empty() {
            anyhow::bail!("未能加载美股 {start_date}..{end_date} 交易日历，无法生成日频任务");
        }
        for date in &trade_dates {
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "us_daily",
                &[("trade_date", date.as_str())],
                Some(US_DAILY_FIELDS),
            )
            .await?;
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "us_daily_adj",
                &[("trade_date", date.as_str())],
                Some(US_DAILY_ADJ_FIELDS),
            )
            .await?;
            force_step(
                client,
                cache,
                &limiter,
                &mut stats,
                "us_adjfactor",
                &[("trade_date", date.as_str())],
                Some(US_ADJFACTOR_FIELDS),
            )
            .await?;
        }
    }

    if sync_scope_includes_financial(scope) {
        let mut codes = load_global_stock_codes(cache, "us_basic").await?;
        if let Some(max_codes) = max_codes {
            codes.truncate(max_codes);
        }
        if codes.is_empty() {
            anyhow::bail!("未能加载美股代码列表，无法生成财务任务");
        }
        for code in &codes {
            for &api_name in &[
                "us_income",
                "us_balancesheet",
                "us_cashflow",
                "us_fina_indicator",
            ] {
                force_step(
                    client,
                    cache,
                    &limiter,
                    &mut stats,
                    api_name,
                    &[
                        ("ts_code", code.as_str()),
                        ("start_date", start_date),
                        ("end_date", end_date),
                    ],
                    Some(GLOBAL_FINANCIAL_FIELDS),
                )
                .await?;
            }
        }
    }

    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(
        calls = stats.total_calls,
        rows = stats.total_rows,
        skipped = stats.skipped,
        errors = stats.errors,
        elapsed = stats.elapsed_secs,
        "美股同步完成"
    );
    Ok(stats)
}

fn normalize_market(market: &str) -> Option<&'static str> {
    match market.to_ascii_lowercase().as_str() {
        "a" | "cn" | "ashare" | "a-share" => Some("a"),
        "hk" | "h" | "hongkong" | "hong-kong" => Some("hk"),
        "us" | "usa" | "u.s." => Some("us"),
        _ => None,
    }
}

fn validate_global_scope(scope: &str) -> Result<()> {
    if matches!(
        scope.to_ascii_lowercase().as_str(),
        "all" | "meta" | "market" | "financial"
    ) {
        Ok(())
    } else {
        anyhow::bail!("未知 HK/US 同步范围: {scope}，可选: all | meta | market | financial")
    }
}

fn sync_scope_includes_meta(scope: &str) -> bool {
    matches!(
        scope.to_ascii_lowercase().as_str(),
        "all" | "meta" | "market" | "financial"
    )
}

fn sync_scope_includes_market(scope: &str) -> bool {
    matches!(scope.to_ascii_lowercase().as_str(), "all" | "market")
}

fn sync_scope_includes_financial(scope: &str) -> bool {
    matches!(scope.to_ascii_lowercase().as_str(), "all" | "financial")
}

// =========================================================================
// Helpers
// =========================================================================

async fn force_step(
    client: &TushareClient,
    cache: &PgCache,
    limiter: &RateLimiter,
    stats: &mut SyncStats,
    api_name: &str,
    params: &[(&str, &str)],
    fields: Option<&str>,
) -> Result<()> {
    const MAX_ATTEMPTS_PER_RUN: usize = 3;

    let job_key = TushareClient::cache_key_for(api_name, params, fields);
    let param_map = params_to_map(params);
    if !cache
        .ensure_sync_job(&job_key, api_name, &param_map, fields)
        .await?
    {
        stats.skipped += 1;
        return Ok(());
    }

    for attempt in 0..MAX_ATTEMPTS_PER_RUN {
        limiter.wait_if_needed().await;
        cache.mark_sync_job_running(&job_key).await?;
        match client.query_force(api_name, params, fields).await {
            Ok(df) => {
                stats.total_calls += 1;
                stats.total_rows += df.height();
                cache.mark_sync_job_done(&job_key, df.height()).await?;
                info!(api = api_name, rows = df.height(), "sync");
                return Ok(());
            }
            Err(e) => {
                stats.total_calls += 1;
                let error = format!("{e:#}");
                if let Some(delay) = retry_delay(&error, attempt, MAX_ATTEMPTS_PER_RUN) {
                    tracing::warn!(
                        api = api_name,
                        attempt = attempt + 1,
                        delay_secs = delay.as_secs(),
                        error = %error,
                        "sync 调用失败，将退避重试"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }

                stats.errors += 1;
                cache.mark_sync_job_failed(&job_key, &error).await?;
                tracing::warn!(api = api_name, error = %error, "sync 调用失败，已记录 checkpoint");
                return Ok(());
            }
        }
    }

    Ok(())
}

fn params_to_map(params: &[(&str, &str)]) -> HashMap<String, String> {
    params
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn retry_delay(error: &str, attempt: usize, max_attempts: usize) -> Option<Duration> {
    if attempt + 1 >= max_attempts {
        return None;
    }

    let lower = error.to_ascii_lowercase();
    if error.contains('频')
        || error.contains("每分钟")
        || lower.contains("-2001")
        || lower.contains("rate")
        || lower.contains("limit")
        || lower.contains("too many")
    {
        return Some(Duration::from_secs(60));
    }

    if lower.contains("http")
        || lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("connection")
        || error.contains("请求失败")
        || error.contains("JSON 解析失败")
    {
        return Some(Duration::from_secs(10 * (attempt as u64 + 1)));
    }

    None
}

const TUSHARE_A_SHARE_FLOOR_DATE: &str = "19900101";

fn daily_basic_lookback_start_date(start_date: &str, lookback_years: usize) -> String {
    if start_date.len() < 8 || lookback_years <= 1 {
        return start_date.to_string();
    }

    let Ok(year) = start_date[..4].parse::<i32>() else {
        return start_date.to_string();
    };
    let start_year = year - lookback_years as i32 + 1;
    let candidate = format!("{}{}", start_year, &start_date[4..]);
    if candidate.as_str() < TUSHARE_A_SHARE_FLOOR_DATE {
        TUSHARE_A_SHARE_FLOOR_DATE.to_string()
    } else {
        candidate
    }
}

const FINANCIAL_QUARTER_ENDS: [&str; 4] = ["0331", "0630", "0930", "1231"];

fn full_sync_financial_periods(
    start_date: &str,
    end_date: &str,
    lookback_years: usize,
) -> Vec<String> {
    let lookback_years = lookback_years.max(1) as i32;
    let start_year = safe_financial_year(start_date) - lookback_years + 1;
    financial_periods_from_year(start_year, end_date)
}

fn incremental_financial_periods(end_date: &str, lookback_years: usize) -> Vec<String> {
    let lookback_years = lookback_years.max(1) as i32;
    let end_year = parse_year(end_date).unwrap_or_else(|| safe_financial_year(end_date));
    let start_year = end_year - lookback_years + 1;
    financial_periods_from_year(start_year, end_date)
}

fn financial_periods_from_year(start_year: i32, end_date: &str) -> Vec<String> {
    let end_year = parse_year(end_date).unwrap_or_else(|| safe_financial_year(end_date));
    let mut periods = Vec::new();
    for year in start_year..=end_year {
        for qtr_end in FINANCIAL_QUARTER_ENDS {
            let period = format!("{year}{qtr_end}");
            if period.as_str() <= end_date {
                periods.push(period);
            }
        }
    }
    periods
}

fn parse_year(date: &str) -> Option<i32> {
    date.get(..4)?.parse().ok()
}

fn month_window(year: i32, month: u32) -> (String, String) {
    let last_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 30,
    };
    (
        format!("{year}{month:02}01"),
        format!("{year}{month:02}{last_day:02}"),
    )
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

async fn load_stock_codes(cache: &PgCache) -> Result<Vec<String>> {
    let params = HashMap::new();
    let Some(df) = cache
        .load_typed("stock_basic", &params, Some("ts_code"))
        .await?
    else {
        return Ok(Vec::new());
    };
    Ok(unique_sorted_column(&df, "ts_code"))
}

async fn load_trade_dates(
    cache: &PgCache,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<String>> {
    let params = HashMap::from([
        ("exchange".to_string(), "SSE".to_string()),
        ("start_date".to_string(), start_date.to_string()),
        ("end_date".to_string(), end_date.to_string()),
        ("is_open".to_string(), "1".to_string()),
    ]);
    let Some(df) = cache
        .load_typed("trade_cal", &params, Some("cal_date"))
        .await?
    else {
        return Ok(Vec::new());
    };
    Ok(unique_sorted_column(&df, "cal_date"))
}

async fn load_global_stock_codes(cache: &PgCache, api_name: &str) -> Result<Vec<String>> {
    let params = HashMap::new();
    let Some(df) = cache.load_typed(api_name, &params, Some("ts_code")).await? else {
        return Ok(Vec::new());
    };
    Ok(unique_sorted_column(&df, "ts_code"))
}

async fn load_global_trade_dates(
    cache: &PgCache,
    api_name: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<String>> {
    let params = HashMap::from([
        ("start_date".to_string(), start_date.to_string()),
        ("end_date".to_string(), end_date.to_string()),
        ("is_open".to_string(), "1".to_string()),
    ]);
    let Some(df) = cache
        .load_typed(api_name, &params, Some("cal_date"))
        .await?
    else {
        return Ok(Vec::new());
    };
    Ok(unique_sorted_column(&df, "cal_date"))
}

fn unique_sorted_column(df: &DataFrame, name: &str) -> Vec<String> {
    let mut values: Vec<String> = df
        .column(name)
        .ok()
        .and_then(|column| column.str().ok())
        .map(|column| {
            column
                .into_iter()
                .filter_map(|value| value.map(String::from))
                .collect()
        })
        .unwrap_or_default();
    values.sort();
    values.dedup();
    values
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
        .map(|col| {
            col.into_iter()
                .filter_map(|v| v.map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let mut unique: Vec<String> = codes
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
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
        .query_force(
            "trade_cal",
            &[
                ("exchange", "SSE"),
                ("start_date", start_date.as_str()),
                ("end_date", end_date),
                ("is_open", "1"),
            ],
            Some("cal_date"),
        )
        .await?;
    stats.total_calls += 1;
    stats.total_rows += df.height();
    let mut dates: Vec<String> = df
        .column("cal_date")
        .ok()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn field_set(fields: &str) -> HashSet<&str> {
        fields.split(",").collect()
    }

    #[test]
    fn stock_basic_fields_cover_official_outputs() {
        let fields = field_set(STOCK_BASIC_FIELDS);
        for field in [
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "fullname",
            "enname",
            "cnspell",
            "market",
            "exchange",
            "curr_type",
            "list_status",
            "list_date",
            "delist_date",
            "is_hs",
            "act_name",
            "act_ent_type",
        ] {
            assert!(fields.contains(field), "missing stock_basic field {field}");
        }
    }

    #[test]
    fn daily_basic_fields_cover_typed_outputs() {
        let fields = field_set(DAILY_BASIC_FIELDS);
        for field in [
            "ts_code",
            "trade_date",
            "close",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dv_ratio",
            "dv_ttm",
            "total_share",
            "float_share",
            "free_share",
            "total_mv",
            "circ_mv",
        ] {
            assert!(fields.contains(field), "missing daily_basic field {field}");
        }
    }

    #[test]
    fn metadata_and_limit_fields_cover_typed_outputs() {
        let disclosure = field_set(DISCLOSURE_DATE_FIELDS);
        assert!(disclosure.contains("pre_date"));
        assert!(disclosure.contains("modify_date"));

        let trade_cal = field_set(TRADE_CAL_FIELDS);
        assert!(trade_cal.contains("pretrade_date"));

        let stk_limit = field_set(STK_LIMIT_FIELDS);
        assert!(stk_limit.contains("pre_close"));
    }

    #[test]
    fn full_sync_financial_periods_include_current_available_quarter() {
        let periods = full_sync_financial_periods("20150101", "20250520", 10);
        assert!(periods.contains(&"20040331".to_string()));
        assert!(periods.contains(&"20250331".to_string()));
        assert!(!periods.contains(&"20250630".to_string()));
    }

    #[test]
    fn full_sync_financial_periods_exclude_future_quarter() {
        let periods = full_sync_financial_periods("20150101", "20250330", 10);
        assert!(periods.contains(&"20241231".to_string()));
        assert!(!periods.contains(&"20250331".to_string()));
    }

    #[test]
    fn incremental_financial_periods_include_latest_quarter() {
        let periods = incremental_financial_periods("20250520", 10);
        assert_eq!(periods.first().map(String::as_str), Some("20160331"));
        assert!(periods.contains(&"20250331".to_string()));
        assert!(!periods.contains(&"20250630".to_string()));
    }

    #[test]
    fn daily_basic_window_includes_pb_lookback_history() {
        assert_eq!(daily_basic_lookback_start_date("20150101", 10), "20060101");
        assert_eq!(daily_basic_lookback_start_date("20250520", 10), "20160520");
    }

    #[test]
    fn daily_basic_window_clamps_to_tushare_floor() {
        assert_eq!(
            daily_basic_lookback_start_date("19950101", 10),
            TUSHARE_A_SHARE_FLOOR_DATE
        );
        assert_eq!(daily_basic_lookback_start_date("20150101", 1), "20150101");
    }
}
