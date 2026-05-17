//! PB / PE / 股息率三维排名打分。
//!
//! 对应 Python 版 `domain.py:score_candidates()`。
//!
//! # 打分规则
//! - **PB**: 升序排名（越低越好），PB ≤ 0 记最差分 N
//! - **PE (TTM)**: 盈利股升序排名（越低越好）；亏损（PE ≤ 0）统一记最差分 N
//! - **股息率**: 降序排名（越高越好）；缺失/NaN 记 0 分
//!
//! 总分 = pb_score + pe_score + div_score（越低越好）

use anyhow::Result;
use polars::prelude::*;

/// 对候选池进行三维打分。
///
/// 输入 DataFrame 需包含列: `ts_code`, `pb`, `pe_ttm`, `dv_ratio`。
/// 输出新增列: `pb_score`, `pe_score`, `div_score`, `total_score`。
pub fn score_candidates(df: &DataFrame) -> Result<DataFrame> {
    if df.height() == 0 {
        let mut result = df.clone();
        let empty_f64 = Column::new("pb_score".into(), Vec::<f64>::new());
        let _ = result.with_column(empty_f64.clone());
        let _ = result.with_column(empty_f64.clone().with_name("pe_score".into()));
        let _ = result.with_column(empty_f64.clone().with_name("div_score".into()));
        let _ = result.with_column(empty_f64.with_name("total_score".into()));
        return Ok(result);
    }

    let n = df.height() as f64;

    // 获取列数据
    let pb_col = df.column("pb")?.f64()?;
    let pe_col = df.column("pe_ttm")?.f64()?;
    let dv_col = df.column("dv_ratio")?.f64()?;

    // --- PB 打分 ---
    // PB > 0: 升序排名；PB <= 0 或 NaN: 最差分 n
    let pb_scores = rank_ascending_with_penalty(pb_col, n);

    // --- PE 打分 ---
    // PE > 0: 升序排名；PE <= 0 或 NaN: 最差分 n
    let pe_scores = rank_ascending_with_penalty(pe_col, n);

    // --- 股息率打分 ---
    // 降序排名（越高越好）；NaN → 视为 0
    let dv_scores = rank_descending(dv_col, n);

    // --- 总分 ---
    let total_scores: Vec<f64> = pb_scores
        .iter()
        .zip(pe_scores.iter())
        .zip(dv_scores.iter())
        .map(|((pb, pe), dv)| pb + pe + dv)
        .collect();

    let mut result = df.clone();
    let _ = result.with_column(Column::new("pb_score".into(), &pb_scores));
    let _ = result.with_column(Column::new("pe_score".into(), &pe_scores));
    let _ = result.with_column(Column::new("div_score".into(), &dv_scores));
    let _ = result.with_column(Column::new("total_score".into(), &total_scores));

    Ok(result)
}

/// 升序排名：值越小越好。NaN 和 ≤0 记为最差分（penalty）。
fn rank_ascending_with_penalty(col: &ChunkedArray<Float64Type>, penalty: f64) -> Vec<f64> {
    // 收集 (index, value) 对
    let mut indexed: Vec<(usize, f64)> = col
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| v.filter(|&x| x > 0.0).map(|x| (i, x)))
        .collect();

    // 按值排序
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // 分配排名（min 方法）
    let mut scores = vec![penalty; col.len()];
    let mut rank = 1.0;
    let mut prev_val: Option<f64> = None;

    for (pos, (idx, val)) in indexed.iter().enumerate() {
        if prev_val.is_none_or(|prev| (val - prev).abs() > 1e-12) {
            rank = (pos + 1) as f64;
        }
        scores[*idx] = rank;
        prev_val = Some(*val);
    }

    scores
}

/// 降序排名：值越大越好。NaN 记为最差分（penalty）。
fn rank_descending(col: &ChunkedArray<Float64Type>, penalty: f64) -> Vec<f64> {
    let mut indexed: Vec<(usize, f64)> = col
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i, v.unwrap_or(0.0))) // NaN → 0
        .collect();

    // 按值降序排序
    indexed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut scores = vec![penalty; col.len()];
    let mut rank = 1.0;
    let mut prev_val: Option<f64> = None;

    for (pos, (idx, val)) in indexed.iter().enumerate() {
        if prev_val.is_none_or(|prev| (val - prev).abs() > 1e-12) {
            rank = (pos + 1) as f64;
        }
        scores[*idx] = rank;
        prev_val = Some(*val);
    }

    scores
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_df() -> DataFrame {
        df!(
            "ts_code" => &["A", "B", "C", "D", "E"],
            "pb" => &[0.5, 1.0, 0.3, -0.1, 0.8],
            "pe_ttm" => &[5.0, 10.0, 3.0, -5.0, 8.0],
            "dv_ratio" => &[3.0, 1.0, 5.0, 0.0, 2.0],
        )
        .unwrap()
    }

    #[test]
    fn test_score_basic() {
        let df = make_test_df();
        let scored = score_candidates(&df).unwrap();

        assert!(scored.column("pb_score").is_ok());
        assert!(scored.column("pe_score").is_ok());
        assert!(scored.column("div_score").is_ok());
        assert!(scored.column("total_score").is_ok());
        assert_eq!(scored.height(), 5);
    }

    #[test]
    fn test_pb_ranking() {
        let df = make_test_df();
        let scored = score_candidates(&df).unwrap();
        let pb_scores: Vec<f64> = scored
            .column("pb_score")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        // PB: C(0.3) < A(0.5) < E(0.8) < B(1.0) < D(-0.1→penalty)
        assert_eq!(pb_scores[2], 1.0); // C = rank 1
        assert_eq!(pb_scores[0], 2.0); // A = rank 2
        assert_eq!(pb_scores[4], 3.0); // E = rank 3
        assert_eq!(pb_scores[1], 4.0); // B = rank 4
        assert_eq!(pb_scores[3], 5.0); // D = penalty (n=5)
    }

    #[test]
    fn test_div_ranking() {
        let df = make_test_df();
        let scored = score_candidates(&df).unwrap();
        let div_scores: Vec<f64> = scored
            .column("div_score")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        // dv_ratio: C(5.0) > A(3.0) > E(2.0) > B(1.0) > D(0.0)
        assert_eq!(div_scores[2], 1.0); // C = rank 1
        assert_eq!(div_scores[0], 2.0); // A = rank 2
    }

    #[test]
    fn test_negative_pe_gets_penalty() {
        let df = make_test_df();
        let scored = score_candidates(&df).unwrap();
        let pe_scores: Vec<f64> = scored
            .column("pe_score")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        // D 的 PE = -5.0 → 应该得到最差分 5.0
        assert_eq!(pe_scores[3], 5.0);
    }

    #[test]
    fn test_total_score_is_sum() {
        let df = make_test_df();
        let scored = score_candidates(&df).unwrap();
        let pb = scored.column("pb_score").unwrap().f64().unwrap();
        let pe = scored.column("pe_score").unwrap().f64().unwrap();
        let dv = scored.column("div_score").unwrap().f64().unwrap();
        let total = scored.column("total_score").unwrap().f64().unwrap();

        for i in 0..scored.height() {
            let expected = pb.get(i).unwrap() + pe.get(i).unwrap() + dv.get(i).unwrap();
            assert!((total.get(i).unwrap() - expected).abs() < 1e-10);
        }
    }

    #[test]
    fn test_empty_df() {
        let df = DataFrame::empty_with_schema(&Schema::from_iter([
            Field::new("ts_code".into(), DataType::String),
            Field::new("pb".into(), DataType::Float64),
            Field::new("pe_ttm".into(), DataType::Float64),
            Field::new("dv_ratio".into(), DataType::Float64),
        ]));
        let scored = score_candidates(&df).unwrap();
        assert_eq!(scored.height(), 0);
        assert!(scored.column("total_score").is_ok());
    }
}
