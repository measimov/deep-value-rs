//! 审计机构数据获取 — 对应 Python 版 `get_audit_info`。

use anyhow::{Context, Result};
use polars::prelude::*;
use tracing::info;

use crate::strategy::domain::is_big4;
use crate::tushare::client::TushareClient;

/// 获取审计机构信息。
///
/// 调用 `fina_audit` 接口，判断每家公司是否由四大审计。
/// 返回 DataFrame 包含 `ts_code`, `is_big4` (bool)。
pub async fn get_audit_info(
    client: &TushareClient,
    trade_date: &str,
) -> Result<DataFrame> {
    let safe_year = crate::data::financials::safe_financial_year(trade_date);
    let period = format!("{}1231", safe_year);

    info!(period = %period, "获取审计机构");

    let df = client
        .query(
            "fina_audit",
            &[("period", period.as_str())],
            Some("ts_code,audit_agency"),
        )
        .await
        .context("获取审计信息失败")?;

    if df.height() == 0 {
        let schema = Schema::from_iter([
            Field::new("ts_code".into(), DataType::String),
            Field::new("is_big4".into(), DataType::Boolean),
        ]);
        return Ok(DataFrame::empty_with_schema(&schema));
    }

    // 判断每家公司是否为四大
    let audit_col = df.column("audit_agency")?.str()?;
    let big4_flags: BooleanChunked = audit_col
        .into_iter()
        .map(|opt_val| opt_val.map(is_big4))
        .collect();

    let mut result = df.select(["ts_code"])?.clone();
    let _ = result.with_column(big4_flags.into_column().with_name("is_big4".into()));

    // 去重
    let result = result
        .lazy()
        .unique(Some(vec!["ts_code".into()]), UniqueKeepStrategy::First)
        .collect()?;

    let big4_count = result
        .column("is_big4")?
        .bool()?
        .into_iter()
        .filter(|v| *v == Some(true))
        .count();

    info!(
        rows = result.height(),
        big4 = big4_count,
        "审计机构完成"
    );

    Ok(result)
}
