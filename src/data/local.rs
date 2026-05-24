//! Typed-table-backed data readers。
//!
//! 每个函数对应其 API 版本，但从 PostgreSQL typed 表读取，
//! 无需 Tushare API 调用。需要先通过 `sync` 命令填充 typed 表。

use std::collections::HashMap;

use anyhow::{Context, Result};
use polars::prelude::*;
use tracing::info;

use crate::data::financials::safe_financial_year;
use crate::strategy::domain::is_big4;
use crate::tushare::pg_cache::PgCache;

// ---------------------------------------------------------------------------
// Market PB median
// ---------------------------------------------------------------------------

pub async fn get_market_pb_median(cache: &PgCache, trade_date: &str) -> Result<f64> {
    let params = hmap(&[("trade_date", trade_date)]);
    let Some(df) = cache
        .load_typed("daily_basic", &params, Some("ts_code,pb"))
        .await?
    else {
        return Ok(f64::NAN);
    };
    if df.height() == 0 {
        return Ok(f64::NAN);
    }
    let pb_col = df
        .lazy()
        .with_column(col("pb").cast(DataType::Float64))
        .filter(col("pb").gt(lit(0.0)))
        .select([col("pb")])
        .collect()?;
    if pb_col.height() == 0 {
        return Ok(f64::NAN);
    }
    Ok(pb_col.column("pb")?.f64()?.median().unwrap_or(f64::NAN))
}

// ---------------------------------------------------------------------------
// Cross section
// ---------------------------------------------------------------------------

pub async fn build_cross_section(cache: &PgCache, trade_date: &str) -> Result<DataFrame> {
    let daily_params = hmap(&[("trade_date", trade_date)]);
    let daily_schema = Schema::from_iter([
        Field::new("ts_code".into(), DataType::String),
        Field::new("pb".into(), DataType::Float64),
        Field::new("pe_ttm".into(), DataType::Float64),
        Field::new("dv_ratio".into(), DataType::Float64),
        Field::new("total_mv".into(), DataType::Float64),
    ]);
    let daily = cache
        .load_typed(
            "daily_basic",
            &daily_params,
            Some("ts_code,pb,pe_ttm,dv_ratio,total_mv"),
        )
        .await?
        .map(|df| {
            df.lazy()
                .with_columns([
                    col("pb").cast(DataType::Float64),
                    col("pe_ttm").cast(DataType::Float64),
                    col("dv_ratio").cast(DataType::Float64),
                    col("total_mv").cast(DataType::Float64),
                ])
                .collect()
        })
        .transpose()?
        .unwrap_or_else(|| DataFrame::empty_with_schema(&daily_schema));
    info!(rows = daily.height(), "daily_basic (local)");

    let stock_params = hmap(&[("list_status", "L")]);
    let basic = cache
        .load_typed(
            "stock_basic",
            &stock_params,
            Some("ts_code,name,industry,list_date"),
        )
        .await?
        .unwrap_or_else(|| df_empty(&["ts_code", "name", "industry"]));
    info!(rows = basic.height(), "stock_basic (local)");

    if daily.height() == 0 {
        return Ok(daily);
    }

    let daily_lazy = daily.lazy().with_columns([
        col("pb").cast(DataType::Float64),
        col("pe_ttm").cast(DataType::Float64),
        col("dv_ratio").cast(DataType::Float64),
        col("total_mv").cast(DataType::Float64),
    ]);

    daily_lazy
        .join(
            basic.lazy(),
            [col("ts_code")],
            [col("ts_code")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()
        .context("local cross section join failed")
}

// ---------------------------------------------------------------------------
// 10-year PB max
// ---------------------------------------------------------------------------

pub async fn get_10y_pb_max(
    cache: &PgCache,
    trade_date: &str,
    lookback_years: usize,
) -> Result<DataFrame> {
    let year: i32 = trade_date[..4].parse().unwrap_or(2025);
    let month_day = &trade_date[4..];
    let start_year = year - lookback_years as i32 + 1;
    let mut max_pb: HashMap<String, f64> = HashMap::new();

    for y in start_year..=year {
        let date = format!("{y}{month_day}");
        let Some(df) = cache
            .load_daily_basic_on_or_before(date.as_str(), Some("ts_code,pb"))
            .await?
        else {
            continue;
        };
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

// ---------------------------------------------------------------------------
// Net equity (balancesheet)
// ---------------------------------------------------------------------------

pub async fn get_net_equity(cache: &PgCache, trade_date: &str) -> Result<DataFrame> {
    let period = format!("{}1231", safe_financial_year(trade_date));
    let params = hmap(&[("period", period.as_str()), ("report_type", "1")]);
    let Some(df) = cache
        .load_typed(
            "balancesheet",
            &params,
            Some("ts_code,end_date,total_hldr_eqy_exc_min_int"),
        )
        .await?
    else {
        return Ok(df_empty(&["ts_code", "net_equity_bn"]));
    };
    if df.height() == 0 {
        return Ok(df
            .select(["ts_code"])?
            .clone()
            .lazy()
            .with_column(lit(NULL).cast(DataType::Float64).alias("net_equity_bn"))
            .collect()?);
    }
    let raw = col("total_hldr_eqy_exc_min_int")
        .cast(DataType::Float64)
        .fill_null(lit(0.0));
    let out = df
        .lazy()
        .with_column(raw.clone())
        .with_column((raw / lit(1e8_f64)).alias("net_equity_bn"))
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .select([col("ts_code"), col("net_equity_bn")])
        .collect()?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Audit info
// ---------------------------------------------------------------------------

pub async fn get_audit_info(cache: &PgCache, trade_date: &str) -> Result<DataFrame> {
    let period = format!("{}1231", safe_financial_year(trade_date));
    let params = hmap(&[("period", period.as_str())]);
    let Some(df) = cache
        .load_typed("fina_audit", &params, Some("ts_code,audit_agency"))
        .await?
    else {
        return Ok(df_empty(&["ts_code", "is_big4"]));
    };
    if df.height() == 0 {
        return Ok(df_empty(&["ts_code", "is_big4"]));
    }
    let audit_col = df.column("audit_agency")?.str()?;
    let big4_flags: BooleanChunked = audit_col
        .into_iter()
        .map(|opt_val| opt_val.map(is_big4))
        .collect();
    let mut result = df.select(["ts_code"])?.clone();
    let _ = result.with_column(big4_flags.into_column().with_name("is_big4".into()));
    Ok(result
        .lazy()
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .collect()?)
}

// ---------------------------------------------------------------------------
// Current year income
// ---------------------------------------------------------------------------

pub async fn get_current_year_income(cache: &PgCache, trade_date: &str) -> Result<DataFrame> {
    let safe_year = safe_financial_year(trade_date);
    let period = format!("{safe_year}1231");
    let params = hmap(&[("period", period.as_str()), ("report_type", "1")]);
    let Some(df) = cache
        .load_typed("income", &params, Some("ts_code,end_date,n_income"))
        .await?
    else {
        return Ok(df_empty(&["ts_code", "current_net_income"]));
    };
    if df.height() == 0 {
        return Ok(df_empty(&["ts_code", "current_net_income"]));
    }
    Ok(df
        .lazy()
        .with_column(
            col("n_income")
                .cast(DataType::Float64)
                .alias("current_net_income"),
        )
        .sort(["ts_code"], Default::default())
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .select([col("ts_code"), col("current_net_income")])
        .collect()?)
}

// ---------------------------------------------------------------------------
// Current year dividend
// ---------------------------------------------------------------------------

pub async fn get_current_year_dividend(cache: &PgCache, trade_date: &str) -> Result<DataFrame> {
    let period = format!("{}1231", safe_financial_year(trade_date));
    let params = hmap(&[("end_date", period.as_str())]);
    let Some(df) = cache
        .load_typed("dividend", &params, Some("ts_code,end_date,cash_div_tax"))
        .await?
    else {
        return Ok(df_empty(&["ts_code", "current_dividend_total"]));
    };
    if df.height() == 0 {
        return Ok(df_empty(&["ts_code", "current_dividend_total"]));
    }
    Ok(df
        .lazy()
        .with_column(
            col("cash_div_tax")
                .cast(DataType::Float64)
                .fill_null(lit(0.0)),
        )
        .group_by([col("ts_code")])
        .agg([col("cash_div_tax").sum().alias("current_dividend_total")])
        .collect()?)
}

// ---------------------------------------------------------------------------
// 10-year income
// ---------------------------------------------------------------------------

pub async fn get_10y_income(
    cache: &PgCache,
    trade_date: &str,
    lookback_years: usize,
) -> Result<DataFrame> {
    let safe_year = safe_financial_year(trade_date);
    let start_year = safe_year - lookback_years as i32 + 1;
    let mut all_frames: Vec<DataFrame> = Vec::new();

    for year in start_year..=safe_year {
        let period = format!("{year}1231");
        let params = hmap(&[("period", period.as_str()), ("report_type", "1")]);
        let Some(df) = cache
            .load_typed("income", &params, Some("ts_code,n_income"))
            .await?
        else {
            continue;
        };
        if df.height() > 0 {
            let df = df
                .lazy()
                .with_column(col("n_income").cast(DataType::Float64).fill_null(lit(0.0)))
                .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
                .collect()?;
            all_frames.push(df);
        }
    }

    aggregate_10y_sum(all_frames, "n_income", "sum_net_income_10y")
}

// ---------------------------------------------------------------------------
// 10-year dividend
// ---------------------------------------------------------------------------

pub async fn get_10y_dividend(
    cache: &PgCache,
    trade_date: &str,
    lookback_years: usize,
) -> Result<DataFrame> {
    let safe_year = safe_financial_year(trade_date);
    let start_year = safe_year - lookback_years as i32 + 1;
    let mut all_frames: Vec<DataFrame> = Vec::new();

    for year in start_year..=safe_year {
        let period = format!("{year}1231");
        let params = hmap(&[("end_date", period.as_str())]);
        let Some(df) = cache
            .load_typed("dividend", &params, Some("ts_code,cash_div_tax"))
            .await?
        else {
            continue;
        };
        if df.height() > 0 {
            let df = df
                .lazy()
                .with_column(
                    col("cash_div_tax")
                        .cast(DataType::Float64)
                        .fill_null(lit(0.0)),
                )
                .group_by([col("ts_code")])
                .agg([col("cash_div_tax").sum().alias("year_div")])
                .collect()?;
            all_frames.push(df);
        }
    }

    aggregate_10y_sum(all_frames, "year_div", "sum_dividend_10y")
}

// ---------------------------------------------------------------------------
// Fina indicator (ROE, margins, leverage)
// ---------------------------------------------------------------------------

pub async fn get_fina_indicator(cache: &PgCache, trade_date: &str) -> Result<DataFrame> {
    let period = format!("{}1231", safe_financial_year(trade_date));
    let params = hmap(&[("period", period.as_str())]);
    let Some(df) = cache
        .load_typed(
            "fina_indicator",
            &params,
            Some("ts_code,end_date,roe,roa,grossprofit_margin,netprofit_margin,debt_to_assets"),
        )
        .await?
    else {
        return Ok(df_empty(&[
            "ts_code",
            "roe",
            "grossprofit_margin",
            "netprofit_margin",
            "debt_to_assets",
        ]));
    };
    if df.height() == 0 {
        return Ok(df_empty(&[
            "ts_code",
            "roe",
            "grossprofit_margin",
            "netprofit_margin",
            "debt_to_assets",
        ]));
    }
    Ok(df
        .lazy()
        .with_columns([
            col("roe").cast(DataType::Float64),
            col("roa").cast(DataType::Float64),
            col("grossprofit_margin").cast(DataType::Float64),
            col("netprofit_margin").cast(DataType::Float64),
            col("debt_to_assets").cast(DataType::Float64),
        ])
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .collect()?)
}

// ---------------------------------------------------------------------------
// Cashflow (operating cash flow for dividend sustainability)
// ---------------------------------------------------------------------------

pub async fn get_cashflow(cache: &PgCache, trade_date: &str) -> Result<DataFrame> {
    let period = format!("{}1231", safe_financial_year(trade_date));
    let params = hmap(&[("period", period.as_str()), ("report_type", "1")]);
    let Some(df) = cache
        .load_typed("cashflow", &params, Some("ts_code,end_date,n_cashflow_act"))
        .await?
    else {
        return Ok(df_empty(&["ts_code", "n_cashflow_act"]));
    };
    if df.height() == 0 {
        return Ok(df_empty(&["ts_code", "n_cashflow_act"]));
    }
    Ok(df
        .lazy()
        .with_column(col("n_cashflow_act").cast(DataType::Float64))
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .select([col("ts_code"), col("n_cashflow_act")])
        .collect()?)
}

// ---------------------------------------------------------------------------
// Backtest data: trade calendar, daily prices, index
// ---------------------------------------------------------------------------

pub async fn get_trade_cal(
    cache: &PgCache,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<String>> {
    let params = hmap(&[
        ("exchange", "SSE"),
        ("start_date", start_date),
        ("end_date", end_date),
        ("is_open", "1"),
    ]);
    let Some(df) = cache
        .load_typed("trade_cal", &params, Some("cal_date"))
        .await?
    else {
        return Ok(Vec::new());
    };
    let mut dates: Vec<String> = df
        .column("cal_date")?
        .str()?
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect();
    dates.sort();
    Ok(dates)
}

pub async fn get_daily_prices(
    cache: &PgCache,
    codes: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    let mut frames: Vec<DataFrame> = Vec::new();
    for code in codes {
        let params = hmap(&[
            ("ts_code", code.as_str()),
            ("start_date", start_date),
            ("end_date", end_date),
        ]);
        let Some(daily) = cache
            .load_typed("daily", &params, Some("ts_code,trade_date,close"))
            .await?
        else {
            continue;
        };
        if daily.height() == 0 {
            continue;
        }
        let Some(adj) = cache
            .load_typed("adj_factor", &params, Some("ts_code,trade_date,adj_factor"))
            .await?
        else {
            continue;
        };
        let merged = daily
            .lazy()
            .with_column(col("close").cast(DataType::Float64))
            .join(
                adj.lazy()
                    .with_column(col("adj_factor").cast(DataType::Float64)),
                [col("ts_code"), col("trade_date")],
                [col("ts_code"), col("trade_date")],
                JoinArgs::new(JoinType::Left),
            )
            .with_column((col("close") * col("adj_factor").fill_null(lit(1.0))).alias("close_adj"))
            .select([col("ts_code"), col("trade_date"), col("close_adj")])
            .collect()?;
        frames.push(merged);
    }
    if frames.is_empty() {
        let schema = Schema::from_iter([
            Field::new("ts_code".into(), DataType::String),
            Field::new("trade_date".into(), DataType::String),
            Field::new("close_adj".into(), DataType::Float64),
        ]);
        return Ok(DataFrame::empty_with_schema(&schema));
    }
    concat(
        frames
            .iter()
            .map(|df| df.clone().lazy())
            .collect::<Vec<_>>(),
        Default::default(),
    )?
    .sort(["ts_code", "trade_date"], Default::default())
    .collect()
    .context("merge daily prices failed")
}

pub async fn get_index_daily(
    cache: &PgCache,
    code: &str,
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    let params = hmap(&[
        ("ts_code", code),
        ("start_date", start_date),
        ("end_date", end_date),
    ]);
    let Some(df) = cache
        .load_typed("index_daily", &params, Some("trade_date,close"))
        .await?
    else {
        let schema = Schema::from_iter([
            Field::new("trade_date".into(), DataType::String),
            Field::new("benchmark_close".into(), DataType::Float64),
        ]);
        return Ok(DataFrame::empty_with_schema(&schema));
    };
    Ok(df
        .lazy()
        .rename(["close"], ["benchmark_close"], true)
        .with_column(col("benchmark_close").cast(DataType::Float64))
        .sort(["trade_date"], Default::default())
        .collect()?)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hmap(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn df_empty(columns: &[&str]) -> DataFrame {
    let schema = Schema::from_iter(columns.iter().map(|name| {
        let dt: DataType = if *name == "is_big4" {
            DataType::Boolean
        } else {
            DataType::String
        };
        Field::new((*name).into(), dt)
    }));
    DataFrame::empty_with_schema(&schema)
}

fn aggregate_10y_sum(
    frames: Vec<DataFrame>,
    value_col: &str,
    output_col: &str,
) -> Result<DataFrame> {
    if frames.is_empty() {
        let schema = Schema::from_iter([
            Field::new("ts_code".into(), DataType::String),
            Field::new(output_col.into(), DataType::Float64),
        ]);
        return Ok(DataFrame::empty_with_schema(&schema));
    }
    let combined = concat(
        frames
            .iter()
            .map(|df| df.clone().lazy())
            .collect::<Vec<_>>(),
        Default::default(),
    )?;
    Ok(combined
        .group_by([col("ts_code")])
        .agg([col(value_col).sum().alias(output_col)])
        .collect()?)
}
