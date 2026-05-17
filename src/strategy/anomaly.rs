//! 排雷逻辑 — 检测异常高利润或分红。
//!
//! 对应 Python 版 `domain.py:remove_anomalies()`。
//!
//! # 排雷规则
//! - **规则 1a**: 当年净利润 > 十年净利润总和（且十年总和 > 0）→ 剔除
//! - **规则 1b**: 十年净利润总和 ≤ 0 且当年净利润 > |十年总和| → 剔除
//!   （历史长期亏损但当年突然暴利，常见于资产处置等非经常损益）
//! - **规则 2**: 当年分红总额 > 十年分红总额 → 剔除

use polars::prelude::*;

/// 排雷结果。
pub struct AnomalyResult {
    /// 通过排雷的股票（保留）
    pub kept: DataFrame,
    /// 被剔除的股票（含原因）
    pub removed: Vec<RemovedStock>,
}

/// 被排雷剔除的股票。
#[derive(Debug, Clone)]
pub struct RemovedStock {
    pub ts_code: String,
    pub reason: String,
}

/// 执行排雷检查。
///
/// # Arguments
/// * `df` - 候选池 DataFrame，需包含 `ts_code`
/// * `current_income` - 当年净利润 DataFrame: `ts_code`, `current_net_income`
/// * `current_dividend` - 当年分红 DataFrame: `ts_code`, `current_dividend_total`
/// * `income_10y` - 十年净利润 DataFrame: `ts_code`, `sum_net_income_10y`
/// * `dividend_10y` - 十年分红 DataFrame: `ts_code`, `sum_dividend_10y`
pub fn remove_anomalies(
    df: &DataFrame,
    current_income: &DataFrame,
    current_dividend: &DataFrame,
    income_10y: &DataFrame,
    dividend_10y: &DataFrame,
) -> anyhow::Result<AnomalyResult> {
    if df.height() == 0 {
        return Ok(AnomalyResult {
            kept: df.clone(),
            removed: Vec::new(),
        });
    }

    let ts_codes: Vec<String> = df
        .column("ts_code")?
        .str()?
        .into_iter()
        .filter_map(|v| v.map(|s| s.to_string()))
        .collect();

    let mut removed: Vec<RemovedStock> = Vec::new();
    let mut keep_mask: Vec<bool> = vec![true; df.height()];

    for (i, code) in ts_codes.iter().enumerate() {
        let mut reasons: Vec<String> = Vec::new();

        // 查找当年净利润
        let cur_income = get_f64_by_code(current_income, code, "current_net_income");
        let sum_10y_income = get_f64_by_code(income_10y, code, "sum_net_income_10y");

        // 规则 1a: 当年净利润 > 十年净利润总和（十年总和 > 0）
        if let (Some(cur), Some(sum)) = (cur_income, sum_10y_income) {
            if sum > 0.0 && cur > sum {
                reasons.push(format!(
                    "当年净利润({:.0}) > 十年总和({:.0})",
                    cur, sum
                ));
            }
            // 规则 1b: 十年利润 ≤ 0 但当年暴利（超过十年亏损绝对值）
            if sum <= 0.0 && cur > sum.abs() {
                reasons.push(format!(
                    "十年累计亏损({:.0})但当年暴利({:.0})",
                    sum, cur
                ));
            }
        }

        // 规则 2: 当年分红 > 十年分红总额
        let cur_div = get_f64_by_code(current_dividend, code, "current_dividend_total");
        let sum_10y_div = get_f64_by_code(dividend_10y, code, "sum_dividend_10y");
        if let (Some(cur), Some(sum)) = (cur_div, sum_10y_div) {
            if sum > 0.0 && cur > sum {
                reasons.push(format!(
                    "当年分红({:.0}) > 十年总额({:.0})",
                    cur, sum
                ));
            }
        }

        if !reasons.is_empty() {
            keep_mask[i] = false;
            removed.push(RemovedStock {
                ts_code: code.clone(),
                reason: reasons.join("; "),
            });
        }
    }

    let mask = BooleanChunked::from_slice("mask".into(), &keep_mask);
    let kept = df.filter(&mask)?;

    Ok(AnomalyResult { kept, removed })
}

/// 辅助：从 DataFrame 中根据 ts_code 查找某列的 f64 值。
fn get_f64_by_code(df: &DataFrame, code: &str, col_name: &str) -> Option<f64> {
    if df.height() == 0 {
        return None;
    }

    let ts_col = df.column("ts_code").ok()?.str().ok()?;
    let val_col = df.column(col_name).ok()?.f64().ok()?;

    for i in 0..df.height() {
        if let Some(ts) = ts_col.get(i) {
            if ts == code {
                return val_col.get(i);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_1a_income_anomaly() {
        let candidates = df!("ts_code" => &["A", "B"]).unwrap();
        let income = df!(
            "ts_code" => &["A", "B"],
            "current_net_income" => &[200.0, 50.0],
        )
        .unwrap();
        let income_10y = df!(
            "ts_code" => &["A", "B"],
            "sum_net_income_10y" => &[100.0, 500.0],
        )
        .unwrap();
        let empty_div = df!(
            "ts_code" => Vec::<String>::new(),
            "current_dividend_total" => Vec::<f64>::new(),
        )
        .unwrap();
        let empty_div_10y = df!(
            "ts_code" => Vec::<String>::new(),
            "sum_dividend_10y" => Vec::<f64>::new(),
        )
        .unwrap();

        let result = remove_anomalies(&candidates, &income, &empty_div, &income_10y, &empty_div_10y).unwrap();
        // A: 200 > 100 → 被剔除
        // B: 50 < 500 → 保留
        assert_eq!(result.kept.height(), 1);
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0].ts_code, "A");
    }

    #[test]
    fn test_rule_1b_negative_10y_sudden_profit() {
        let candidates = df!("ts_code" => &["X"]).unwrap();
        let income = df!(
            "ts_code" => &["X"],
            "current_net_income" => &[60.0],
        )
        .unwrap();
        let income_10y = df!(
            "ts_code" => &["X"],
            "sum_net_income_10y" => &[-50.0],
        )
        .unwrap();
        let empty = df!(
            "ts_code" => Vec::<String>::new(),
            "current_dividend_total" => Vec::<f64>::new(),
        )
        .unwrap();
        let empty_10y = df!(
            "ts_code" => Vec::<String>::new(),
            "sum_dividend_10y" => Vec::<f64>::new(),
        )
        .unwrap();

        let result = remove_anomalies(&candidates, &income, &empty, &income_10y, &empty_10y).unwrap();
        // X: sum=-50, cur=60 > |−50|=50 → 剔除
        assert_eq!(result.removed.len(), 1);
        assert!(result.removed[0].reason.contains("十年累计亏损"));
    }

    #[test]
    fn test_rule_2_dividend_anomaly() {
        let candidates = df!("ts_code" => &["D"]).unwrap();
        let empty_income = df!(
            "ts_code" => Vec::<String>::new(),
            "current_net_income" => Vec::<f64>::new(),
        )
        .unwrap();
        let empty_income_10y = df!(
            "ts_code" => Vec::<String>::new(),
            "sum_net_income_10y" => Vec::<f64>::new(),
        )
        .unwrap();
        let div = df!(
            "ts_code" => &["D"],
            "current_dividend_total" => &[500.0],
        )
        .unwrap();
        let div_10y = df!(
            "ts_code" => &["D"],
            "sum_dividend_10y" => &[200.0],
        )
        .unwrap();

        let result = remove_anomalies(&candidates, &empty_income, &div, &empty_income_10y, &div_10y).unwrap();
        assert_eq!(result.removed.len(), 1);
        assert!(result.removed[0].reason.contains("当年分红"));
    }

    #[test]
    fn test_no_anomaly() {
        let candidates = df!("ts_code" => &["GOOD"]).unwrap();
        let income = df!(
            "ts_code" => &["GOOD"],
            "current_net_income" => &[10.0],
        )
        .unwrap();
        let income_10y = df!(
            "ts_code" => &["GOOD"],
            "sum_net_income_10y" => &[100.0],
        )
        .unwrap();
        let div = df!(
            "ts_code" => &["GOOD"],
            "current_dividend_total" => &[5.0],
        )
        .unwrap();
        let div_10y = df!(
            "ts_code" => &["GOOD"],
            "sum_dividend_10y" => &[50.0],
        )
        .unwrap();

        let result = remove_anomalies(&candidates, &income, &div, &income_10y, &div_10y).unwrap();
        assert_eq!(result.kept.height(), 1);
        assert_eq!(result.removed.len(), 0);
    }
}
