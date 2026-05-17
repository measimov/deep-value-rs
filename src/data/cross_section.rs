//! A 股横截面数据构建 — 对应 Python 版 `build_cross_section_ashare`。
//!
//! 合并 `daily_basic` (日行情指标) 和 `stock_basic` (股票基本信息)，
//! 生成包含 PB/PE/股息率/行业/名称 的横截面 DataFrame。

use anyhow::{Context, Result};
use polars::prelude::*;
use tracing::info;

use crate::tushare::client::TushareClient;

/// 获取全市场 PB 中位数（用于市场前提判断）。
///
/// 调用 `daily_basic` 接口，计算 PB 中位数。
pub async fn get_market_pb_median(client: &TushareClient, trade_date: &str) -> Result<f64> {
    let df = client
        .query(
            "daily_basic",
            &[("trade_date", trade_date)],
            Some("ts_code,pb,total_mv"),
        )
        .await
        .context("获取 daily_basic 失败")?;

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

    let median = pb_col.column("pb")?.f64()?.median().unwrap_or(f64::NAN);

    info!(trade_date, median, "A 股 PB 中位数");
    Ok(median)
}

/// 构建 A 股横截面数据。
///
/// 合并 `daily_basic` + `stock_basic`，返回包含以下列的 DataFrame：
/// - `ts_code`, `name`, `industry`
/// - `pb`, `pe_ttm`, `dv_ratio`
/// - `total_mv` (总市值，万元)
pub async fn build_cross_section(client: &TushareClient, trade_date: &str) -> Result<DataFrame> {
    // 1. 日行情指标
    let daily = client
        .query(
            "daily_basic",
            &[("trade_date", trade_date)],
            Some("ts_code,pb,pe_ttm,dv_ratio,total_mv"),
        )
        .await
        .context("获取 daily_basic 失败")?;

    info!(rows = daily.height(), "daily_basic 获取完成");

    // 2. 股票基本信息（名称、行业）
    let basic = client
        .query(
            "stock_basic",
            &[("list_status", "L")],
            Some("ts_code,name,industry"),
        )
        .await
        .context("获取 stock_basic 失败")?;

    info!(rows = basic.height(), "stock_basic 获取完成");

    if daily.height() == 0 {
        return Ok(daily);
    }

    // 3. 合并
    let daily_lazy = daily.lazy().with_columns([
        col("pb").cast(DataType::Float64),
        col("pe_ttm").cast(DataType::Float64),
        col("dv_ratio").cast(DataType::Float64),
        col("total_mv").cast(DataType::Float64),
    ]);

    let result = daily_lazy
        .join(
            basic.lazy(),
            [col("ts_code")],
            [col("ts_code")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;

    info!(rows = result.height(), trade_date, "A 股横截面构建完成");

    Ok(result)
}

/// 获取交易日历。
pub async fn get_trade_cal(
    client: &TushareClient,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<String>> {
    let df = client
        .query(
            "trade_cal",
            &[
                ("exchange", "SSE"),
                ("start_date", start_date),
                ("end_date", end_date),
                ("is_open", "1"),
            ],
            Some("cal_date"),
        )
        .await
        .context("获取交易日历失败")?;

    let dates: Vec<String> = df
        .column("cal_date")?
        .str()?
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect();

    Ok(dates)
}
