//! 财务数据获取 — 十年净利润/分红/PB、当年指标。
//!
//! 对应 Python 版 `data_builders.py` 的财务数据部分。

use anyhow::{Context, Result};
use polars::prelude::*;
use tracing::info;

use crate::tushare::client::TushareClient;

/// 推算 trade_date 可安全使用的最新年报年度，避免前视偏差。
///
/// 中国上市公司年报最迟在次年 4 月 30 日前披露。
/// 因此在 1~4 月，最新的"已公开"年报来自 year-2；
/// 5 月及之后，year-1 年报已全部公开。
///
/// # Examples
/// ```
/// use deep_value::data::financials::safe_financial_year;
/// assert_eq!(safe_financial_year("20230105"), 2021);  // 1月 → 前年
/// assert_eq!(safe_financial_year("20230515"), 2022);  // 5月 → 去年
/// ```
pub fn safe_financial_year(trade_date: &str) -> i32 {
    let year: i32 = trade_date[..4].parse().unwrap_or(2024);
    let month: i32 = trade_date[4..6].parse().unwrap_or(1);
    if month < 5 {
        year - 2
    } else {
        year - 1
    }
}

/// 获取当年净利润（用于排雷）。
///
/// 根据 `trade_date` 自动选择可安全使用的最新年报年度，
/// 拉取 `income` 接口中的 `n_income` (归母净利润)。
pub async fn get_current_year_income(
    client: &TushareClient,
    trade_date: &str,
) -> Result<DataFrame> {
    let safe_year = safe_financial_year(trade_date);
    let period = format!("{}1231", safe_year);

    info!(period = %period, "获取当年净利润");

    let df = client
        .query(
            "income_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some("ts_code,end_date,n_income"),
        )
        .await
        .context("获取净利润失败")?;

    // 转换类型：n_income 为数值
    if df.height() == 0 {
        return Ok(df);
    }

    let df = df
        .lazy()
        .with_column(col("n_income").cast(DataType::Float64))
        .rename(["n_income"], ["current_net_income"], true)
        .collect()?;

    // 去重（保留每只股票的第一条记录）
    let df = df
        .lazy()
        .sort(["ts_code"], Default::default())
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .collect()?;

    info!(rows = df.height(), period = %period, "当年净利润完成");
    Ok(df)
}

/// 获取十年净利润汇总。
///
/// 返回 DataFrame 包含 `ts_code`, `sum_net_income_10y`。
pub async fn get_10y_income(
    client: &TushareClient,
    trade_date: &str,
    lookback_years: usize,
) -> Result<DataFrame> {
    let safe_year = safe_financial_year(trade_date);
    let start_year = safe_year - lookback_years as i32 + 1;

    info!(start_year, end_year = safe_year, "获取十年净利润");

    let mut all_frames: Vec<DataFrame> = Vec::new();

    for year in start_year..=safe_year {
        let period = format!("{}1231", year);
        let df = client
            .query(
                "income_vip",
                &[("period", period.as_str()), ("report_type", "1")],
                Some("ts_code,end_date,n_income"),
            )
            .await
            .with_context(|| format!("获取{}年净利润失败", year))?;

        if df.height() > 0 {
            let df = df
                .lazy()
                .with_column(col("n_income").cast(DataType::Float64).fill_null(lit(0.0)))
                .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
                .collect()?;
            all_frames.push(df);
        }

        // 避免 API 限速
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    if all_frames.is_empty() {
        let schema = Schema::from_iter([
            Field::new("ts_code".into(), DataType::String),
            Field::new("sum_net_income_10y".into(), DataType::Float64),
        ]);
        return Ok(DataFrame::empty_with_schema(&schema));
    }

    // 合并所有年份，按 ts_code 汇总
    let combined = concat(
        all_frames
            .iter()
            .map(|df| df.clone().lazy())
            .collect::<Vec<_>>(),
        Default::default(),
    )?;

    let result = combined
        .group_by([col("ts_code")])
        .agg([col("n_income").sum().alias("sum_net_income_10y")])
        .collect()?;

    info!(rows = result.height(), "十年净利润汇总完成");
    Ok(result)
}

/// 获取净资产数据（资产负债表）。
pub async fn get_net_equity(client: &TushareClient, trade_date: &str) -> Result<DataFrame> {
    let safe_year = safe_financial_year(trade_date);
    let period = format!("{}1231", safe_year);

    info!(period = %period, "获取净资产");

    let df = client
        .query(
            "balancesheet_vip",
            &[("period", period.as_str()), ("report_type", "1")],
            Some("ts_code,end_date,total_hldr_eqy_exc_min_int"),
        )
        .await
        .context("获取净资产失败")?;

    if df.height() == 0 {
        return Ok(df);
    }

    let df = df
        .lazy()
        .with_column(
            col("total_hldr_eqy_exc_min_int")
                .cast(DataType::Float64)
                .fill_null(lit(0.0))
                // 万元 → 亿元
                / lit(1e8_f64)
                .alias("net_equity_bn"),
        )
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .select([col("ts_code"), col("net_equity_bn")])
        .collect()?;

    info!(rows = df.height(), "净资产完成");
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_financial_year_jan() {
        // 1月 → 前年年报 (year-2)
        assert_eq!(safe_financial_year("20230105"), 2021);
    }

    #[test]
    fn test_safe_financial_year_apr() {
        // 4月 → 前年年报 (year-2)
        assert_eq!(safe_financial_year("20230415"), 2021);
    }

    #[test]
    fn test_safe_financial_year_may() {
        // 5月 → 去年年报 (year-1)
        assert_eq!(safe_financial_year("20230515"), 2022);
    }

    #[test]
    fn test_safe_financial_year_dec() {
        // 12月 → 去年年报 (year-1)
        assert_eq!(safe_financial_year("20231220"), 2022);
    }
}
