//! 九步筛选管线 + 行业 cap + 等权仓位。
//!
//! 对应 Python 版 `orchestrator.py` 中的筛选逻辑和
//! `domain.py:enforce_industry_cap()` / `build_portfolio()`。

use std::collections::HashMap;

use polars::prelude::*;

use super::domain::StepRecord;

/// 行业占比上限约束 + 递补。
///
/// 按 `total_score` 升序（分值越低越好）选取 `top_n` 只股票，
/// 同时保证任一行业占比不超过 `cap`。
///
/// 行业为 `"未分类"` 的股票（通常为港股）不受行业 cap 限制。
pub fn enforce_industry_cap(df: &DataFrame, cap: f64, top_n: usize) -> anyhow::Result<DataFrame> {
    if df.height() == 0 {
        return Ok(df.clone());
    }

    // 按 total_score 升序排序
    let sorted = df.sort(["total_score"], SortMultipleOptions::default())?;

    let max_per_industry = (top_n as f64 * cap).ceil().max(1.0) as usize;

    let _ts_codes = sorted.column("ts_code")?.str()?;
    let industries = sorted.column("industry")?.str()?;

    let mut selected: Vec<u32> = Vec::with_capacity(top_n);
    let mut industry_counts: HashMap<String, usize> = HashMap::new();

    for i in 0..sorted.height() {
        if selected.len() >= top_n {
            break;
        }

        let industry = industries.get(i).unwrap_or("未分类").to_string();
        let industry = if industry.is_empty() {
            "未分类".to_string()
        } else {
            industry
        };

        // "未分类" 不受行业 cap 限制
        if industry == "未分类" {
            selected.push(i as u32);
            continue;
        }

        let count = industry_counts.get(&industry).copied().unwrap_or(0);
        if count < max_per_industry {
            selected.push(i as u32);
            industry_counts.insert(industry, count + 1);
        }
    }

    let idx = IdxCa::from_vec("idx".into(), selected);
    Ok(sorted.take(&idx)?)
}

/// 等权仓位分配。
///
/// 给每只股票分配 `target_equity_weight / n` 的权重。
pub fn build_portfolio(df: &DataFrame, target_equity_weight: f64) -> anyhow::Result<DataFrame> {
    if df.height() == 0 {
        let mut result = df.clone();
        let empty = Column::new("weight".into(), Vec::<f64>::new());
        let _ = result.with_column(empty);
        return Ok(result);
    }

    let n = df.height();
    let weight = target_equity_weight / n as f64;
    let weights = vec![weight; n];

    let mut result = df.clone();
    let _ = result.with_column(Column::new("weight".into(), weights));
    Ok(result)
}

/// 构建筛选步骤记录。
pub fn make_step(
    step_number: u8,
    step_name: &str,
    before: usize,
    after: usize,
    skipped: bool,
    note: &str,
) -> StepRecord {
    StepRecord {
        step_name: step_name.to_string(),
        step_number,
        before_count: before,
        after_count: after,
        degraded: false,
        skipped,
        note: note.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scored_df() -> DataFrame {
        df!(
            "ts_code" => &["A", "B", "C", "D", "E", "F", "G", "H"],
            "industry" => &["银行", "银行", "银行", "地产", "地产", "煤炭", "钢铁", "未分类"],
            "total_score" => &[10.0, 20.0, 30.0, 15.0, 25.0, 12.0, 18.0, 5.0],
        )
        .unwrap()
    }

    #[test]
    fn test_industry_cap_basic() {
        let df = make_scored_df();
        // top_n=5, cap=0.30 → max 2 per industry
        let result = enforce_industry_cap(&df, 0.30, 5).unwrap();
        assert_eq!(result.height(), 5);

        // 检查行业分布
        let industries: Vec<String> = result
            .column("industry")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(|s| s.to_string())
            .collect();

        let bank_count = industries.iter().filter(|i| *i == "银行").count();
        assert!(bank_count <= 2, "银行不应超过 2 只, 实际 {}", bank_count);
    }

    #[test]
    fn test_unclassified_exempt() {
        // "未分类" 不受 cap 限制
        let df = df!(
            "ts_code" => &["A", "B", "C", "D"],
            "industry" => &["未分类", "未分类", "未分类", "银行"],
            "total_score" => &[1.0, 2.0, 3.0, 4.0],
        )
        .unwrap();

        // cap=0.30, top_n=3 → max 1 per industry
        // 但 "未分类" 豁免，所以 3 只都可以入选
        let result = enforce_industry_cap(&df, 0.30, 3).unwrap();
        assert_eq!(result.height(), 3);

        let industries: Vec<String> = result
            .column("industry")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(|s| s.to_string())
            .collect();

        let unclassified = industries.iter().filter(|i| *i == "未分类").count();
        assert_eq!(unclassified, 3);
    }

    #[test]
    fn test_portfolio_weights() {
        let df = df!(
            "ts_code" => &["A", "B", "C"],
            "total_score" => &[10.0, 20.0, 30.0],
        )
        .unwrap();

        let result = build_portfolio(&df, 0.5).unwrap();
        let weights: Vec<f64> = result
            .column("weight")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        // 3 stocks × weight = 0.5/3 ≈ 0.1667
        for w in &weights {
            assert!((*w - 0.5 / 3.0).abs() < 1e-10);
        }

        // weights sum = target_equity_weight
        let sum: f64 = weights.iter().sum();
        assert!((sum - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_empty_portfolio() {
        let df = DataFrame::empty_with_schema(&Schema::from_iter([
            Field::new("ts_code".into(), DataType::String),
            Field::new("total_score".into(), DataType::Float64),
        ]));
        let result = build_portfolio(&df, 0.5).unwrap();
        assert_eq!(result.height(), 0);
        assert!(result.column("weight").is_ok());
    }
}
