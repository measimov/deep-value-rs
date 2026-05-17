//! 通用回测引擎 — 从价格 DataFrame + 再平衡记录 → 净值曲线。
//!
//! 对应 Python 版 `backtest_engine.py` 的核心逻辑。

use anyhow::{Context, Result};
use polars::prelude::*;
use tracing::info;

use crate::backtest::metrics;
use crate::strategy::domain::{BacktestMetrics, HoldingReturn};
use crate::tushare::client::TushareClient;

/// 回测完整结果。
#[derive(Debug)]
pub struct BacktestResult {
    pub metrics: BacktestMetrics,
    /// 日频净值序列: date, nav, benchmark_nav, drawdown
    pub nav_series: DataFrame,
    /// 每期收益汇总
    pub period_returns: Vec<PeriodReturn>,
    /// 所有持仓的持有期收益
    pub holding_returns: Vec<HoldingReturn>,
}

/// 单期收益。
#[derive(Debug, Clone)]
pub struct PeriodReturn {
    pub date: String,
    pub end_date: String,
    pub gross_return: f64,
    pub cost: f64,
    pub net_return: f64,
    pub turnover: f64,
    pub holdings_count: usize,
    pub added: usize,
    pub removed: usize,
}

/// 获取 A 股日线价格（前复权）。
pub async fn fetch_ashare_prices(
    client: &TushareClient,
    codes: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    info!(
        codes = codes.len(),
        start = start_date,
        end = end_date,
        "获取 A 股日线"
    );

    let mut all_frames: Vec<DataFrame> = Vec::new();

    for (i, code) in codes.iter().enumerate() {
        let df = client
            .query(
                "daily",
                &[
                    ("ts_code", code.as_str()),
                    ("start_date", start_date),
                    ("end_date", end_date),
                ],
                Some("ts_code,trade_date,close"),
            )
            .await
            .with_context(|| format!("获取 {} 日线失败", code))?;

        if df.height() > 0 {
            // 获取复权因子
            let adj_df = client
                .query(
                    "adj_factor",
                    &[
                        ("ts_code", code.as_str()),
                        ("start_date", start_date),
                        ("end_date", end_date),
                    ],
                    Some("ts_code,trade_date,adj_factor"),
                )
                .await
                .with_context(|| format!("获取 {} 复权因子失败", code))?;

            // 合并并计算前复权价格
            if adj_df.height() > 0 {
                let merged = df
                    .lazy()
                    .with_column(col("close").cast(DataType::Float64))
                    .join(
                        adj_df
                            .lazy()
                            .with_column(col("adj_factor").cast(DataType::Float64)),
                        [col("ts_code"), col("trade_date")],
                        [col("ts_code"), col("trade_date")],
                        JoinArgs::new(JoinType::Left),
                    )
                    .with_column(
                        (col("close") * col("adj_factor").fill_null(lit(1.0))).alias("close_adj"),
                    )
                    .select([col("ts_code"), col("trade_date"), col("close_adj")])
                    .collect()?;
                all_frames.push(merged);
            } else {
                // 无复权因子，直接使用收盘价
                let simple = df
                    .lazy()
                    .with_column(col("close").cast(DataType::Float64).alias("close_adj"))
                    .select([col("ts_code"), col("trade_date"), col("close_adj")])
                    .collect()?;
                all_frames.push(simple);
            }
        }

        // 限速
        if (i + 1) % 20 == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    }

    if all_frames.is_empty() {
        let schema = Schema::from_iter([
            Field::new("ts_code".into(), DataType::String),
            Field::new("trade_date".into(), DataType::String),
            Field::new("close_adj".into(), DataType::Float64),
        ]);
        return Ok(DataFrame::empty_with_schema(&schema));
    }

    let combined = concat(
        all_frames
            .iter()
            .map(|df| df.clone().lazy())
            .collect::<Vec<_>>(),
        Default::default(),
    )?
    .sort(["ts_code", "trade_date"], Default::default())
    .collect()?;

    info!(rows = combined.height(), "A 股日线完成");
    Ok(combined)
}

/// 获取基准指数日线。
pub async fn fetch_benchmark(
    client: &TushareClient,
    start_date: &str,
    end_date: &str,
    benchmark_code: Option<&str>,
) -> Result<DataFrame> {
    let bm_code = benchmark_code.unwrap_or("000300.SH");
    info!(code = bm_code, "获取基准指数");

    let df = client
        .query(
            "index_daily",
            &[
                ("ts_code", bm_code),
                ("start_date", start_date),
                ("end_date", end_date),
            ],
            Some("trade_date,close"),
        )
        .await
        .context("获取基准失败")?;

    if df.height() == 0 {
        let schema = Schema::from_iter([
            Field::new("trade_date".into(), DataType::String),
            Field::new("benchmark_close".into(), DataType::Float64),
        ]);
        return Ok(DataFrame::empty_with_schema(&schema));
    }

    let result = df
        .lazy()
        .rename(["close"], ["benchmark_close"], true)
        .with_column(col("benchmark_close").cast(DataType::Float64))
        .sort(["trade_date"], Default::default())
        .collect()?;

    info!(rows = result.height(), "基准完成");
    Ok(result)
}

/// 计算单期等权收益率。
pub fn compute_period_return(
    holdings: &[String],
    prices: &DataFrame,
    start_date: &str,
    end_date: &str,
) -> Result<(f64, Vec<HoldingReturn>)> {
    let mut holding_returns = Vec::new();
    let mut stock_returns = Vec::new();

    let ts_col = prices.column("ts_code")?.str()?;
    let date_col = prices.column("trade_date")?.str()?;
    let price_col = prices.column("close_adj")?.f64()?;

    for code in holdings {
        // 筛选该股票在日期范围内的数据
        let mut entry_price = None;
        let mut exit_price = None;
        let mut entry_date = String::new();
        let mut exit_date = String::new();

        for i in 0..prices.height() {
            let ts = ts_col.get(i).unwrap_or("");
            let date = date_col.get(i).unwrap_or("");
            if ts != code || date < start_date || date > end_date {
                continue;
            }
            if let Some(p) = price_col.get(i) {
                if entry_price.is_none() {
                    entry_price = Some(p);
                    entry_date = date.to_string();
                }
                exit_price = Some(p);
                exit_date = date.to_string();
            }
        }

        let (ep, xp) = match (entry_price, exit_price) {
            (Some(e), Some(x)) if e > 0.0 => (e, x),
            _ => {
                holding_returns.push(HoldingReturn {
                    ts_code: code.clone(),
                    name: code.clone(),
                    entry_date: start_date.to_string(),
                    exit_date: end_date.to_string(),
                    entry_price: 0.0,
                    exit_price: 0.0,
                    holding_return: 0.0,
                    holding_days: 0,
                });
                continue;
            }
        };

        let ret = (xp - ep) / ep;
        stock_returns.push(ret);

        holding_returns.push(HoldingReturn {
            ts_code: code.clone(),
            name: code.clone(),
            entry_date,
            exit_date,
            entry_price: (ep * 10000.0).round() / 10000.0,
            exit_price: (xp * 10000.0).round() / 10000.0,
            holding_return: (ret * 1e6).round() / 1e6,
            holding_days: 0, // TODO: compute from dates
        });
    }

    let period_return = if stock_returns.is_empty() {
        0.0
    } else {
        stock_returns.iter().sum::<f64>() / stock_returns.len() as f64
    };

    Ok((period_return, holding_returns))
}

/// 从净值序列计算完整的回测指标。
pub fn compute_metrics_from_nav(
    nav_values: &[f64],
    dates: &[String],
    benchmark_values: &[f64],
    period_returns: &[PeriodReturn],
    holding_returns: &[HoldingReturn],
    num_rebalances: usize,
) -> BacktestMetrics {
    if nav_values.len() < 2 {
        return BacktestMetrics {
            total_return: 0.0,
            annualized_return: 0.0,
            benchmark_total_return: 0.0,
            benchmark_annualized: 0.0,
            excess_return: 0.0,
            max_drawdown: 0.0,
            max_drawdown_start: String::new(),
            max_drawdown_end: String::new(),
            sharpe_ratio: 0.0,
            calmar_ratio: 0.0,
            volatility: 0.0,
            win_rate: 0.0,
            total_turnover: 0.0,
            total_cost: 0.0,
            num_rebalances,
            avg_holding_days: 0.0,
        };
    }

    let total_return = nav_values.last().unwrap() / nav_values[0] - 1.0;
    let days = dates.len().saturating_sub(1);
    let ann_return = metrics::annualized_return(total_return, days);

    // 日收益率
    let daily_returns: Vec<f64> = nav_values.windows(2).map(|w| w[1] / w[0] - 1.0).collect();

    let volatility = metrics::annualized_volatility(&daily_returns);
    let sharpe = metrics::sharpe_ratio(&daily_returns, 0.03);
    let (max_dd, dd_start_idx, dd_end_idx) = metrics::max_drawdown(nav_values);
    let calmar = metrics::calmar_ratio(ann_return, max_dd);

    // 基准
    let bm_total = if benchmark_values.len() >= 2 {
        benchmark_values.last().unwrap() / benchmark_values[0] - 1.0
    } else {
        0.0
    };
    let bm_annual = metrics::annualized_return(bm_total, days);

    // 胜率
    let wins = holding_returns
        .iter()
        .filter(|h| h.holding_return > 0.0)
        .count();
    let win_rate = if holding_returns.is_empty() {
        0.0
    } else {
        wins as f64 / holding_returns.len() as f64
    };

    // 换手 & 成本
    let total_turnover: f64 = period_returns.iter().map(|p| p.turnover).sum::<f64>() * 100.0;
    let total_cost: f64 = period_returns.iter().map(|p| p.cost).sum::<f64>() * 100.0;

    BacktestMetrics {
        total_return: (total_return * 1e6).round() / 1e6,
        annualized_return: (ann_return * 1e6).round() / 1e6,
        benchmark_total_return: (bm_total * 1e6).round() / 1e6,
        benchmark_annualized: (bm_annual * 1e6).round() / 1e6,
        excess_return: ((ann_return - bm_annual) * 1e6).round() / 1e6,
        max_drawdown: (max_dd * 1e6).round() / 1e6,
        max_drawdown_start: dates.get(dd_start_idx).cloned().unwrap_or_default(),
        max_drawdown_end: dates.get(dd_end_idx).cloned().unwrap_or_default(),
        sharpe_ratio: (sharpe * 10000.0).round() / 10000.0,
        calmar_ratio: (calmar * 10000.0).round() / 10000.0,
        volatility: (volatility * 1e6).round() / 1e6,
        win_rate: (win_rate * 10000.0).round() / 10000.0,
        total_turnover: (total_turnover * 100.0).round() / 100.0,
        total_cost: (total_cost * 100.0).round() / 100.0,
        num_rebalances,
        avg_holding_days: 0.0, // TODO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_period_return() {
        let prices = df!(
            "ts_code" => &["A", "A", "A", "B", "B", "B"],
            "trade_date" => &["20250101", "20250102", "20250103", "20250101", "20250102", "20250103"],
            "close_adj" => &[10.0, 11.0, 12.0, 20.0, 19.0, 22.0],
        )
        .unwrap();

        let holdings = vec!["A".to_string(), "B".to_string()];
        let (ret, hrs) = compute_period_return(&holdings, &prices, "20250101", "20250103").unwrap();

        // A: (12-10)/10 = 0.2, B: (22-20)/20 = 0.1 → avg = 0.15
        assert!((ret - 0.15).abs() < 1e-6);
        assert_eq!(hrs.len(), 2);
    }

    #[test]
    fn test_metrics_from_nav() {
        let nav = vec![1.0, 1.05, 1.10, 1.08, 1.15, 1.20];
        let dates: Vec<String> = (0..6).map(|i| format!("2025010{}", i + 1)).collect();
        let bm = vec![1.0, 1.02, 1.03, 1.01, 1.05, 1.06];

        let m = compute_metrics_from_nav(&nav, &dates, &bm, &[], &[], 4);
        assert!(m.total_return > 0.0);
        assert!(m.annualized_return > 0.0);
        assert!(m.max_drawdown < 0.0);
        assert!(m.sharpe_ratio > 0.0);
    }
}
