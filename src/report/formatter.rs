//! 中文格式化输出 — 对应 Python 版 `format_snapshot_result()`。

use crate::strategy::domain::{Holding, SnapshotResult, StepRecord};

/// 格式化快照结果为可打印的中文报告。
pub fn format_snapshot(result: &SnapshotResult) -> String {
    let mut out = String::with_capacity(4096);
    let sep = "=".repeat(60);

    // 标题
    out.push_str(&format!("{sep}\n"));
    out.push_str(&format!(
        "  低估分散不深研 — 快照报告 ({})\n",
        result.trade_date
    ));
    out.push_str(&format!("{sep}\n\n"));

    // 市场前提
    out.push_str("【市场前提判断】\n");
    for (market, pb) in &result.market_pb_map {
        let investable = result
            .is_investable_map
            .get(market)
            .copied()
            .unwrap_or(false);
        let icon = if investable { "✅ 可建仓" } else { "❌ 不建仓" };
        out.push_str(&format!("  {market}: PB 中位数 = {pb:.2}  {icon}\n"));
    }
    out.push('\n');

    // 筛选过程
    out.push_str("【九步筛选过程】\n");
    for step in &result.step_records {
        format_step(&mut out, step);
    }
    out.push('\n');

    // 最终持仓
    out.push_str(&format!(
        "【最终持仓】共 {} 只\n",
        result.holdings.len()
    ));
    if !result.holdings.is_empty() {
        out.push_str("  代码           名称       行业         PB       PE    股息率     净资产(亿)     总分     权重\n");
        out.push_str(&format!("  {}\n", "-".repeat(80)));
        for h in &result.holdings {
            format_holding(&mut out, h);
        }
    }
    out.push('\n');

    // 行业分布
    if !result.industry_dist.is_empty() {
        out.push_str("【行业分布】\n");
        let mut sorted: Vec<_> = result.industry_dist.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (industry, count) in sorted {
            out.push_str(&format!("  {industry}: {count} 只\n"));
        }
        out.push('\n');
    }

    // 数据告警
    if !result.data_warnings.is_empty() {
        out.push_str("【数据告警】\n");
        for w in &result.data_warnings {
            out.push_str(&format!("  ⚠️ {w}\n"));
        }
        out.push('\n');
    }

    out.push_str(&format!("{sep}\n"));
    out
}

fn format_step(out: &mut String, step: &StepRecord) {
    if step.skipped {
        out.push_str(&format!(
            "  Step {}: {}  {} → {} [跳过]\n",
            step.step_number, step.step_name, step.before_count, step.after_count
        ));
        if !step.note.is_empty() {
            out.push_str(&format!("         ↳ {}\n", step.note));
        }
    } else {
        out.push_str(&format!(
            "  Step {}: {}  {} → {}\n",
            step.step_number, step.step_name, step.before_count, step.after_count
        ));
        if !step.note.is_empty() {
            out.push_str(&format!("         ↳ {}\n", step.note));
        }
    }
}

fn format_holding(out: &mut String, h: &Holding) {
    out.push_str(&format!(
        "  {:<13}{:<10}{:<12}{:>6.2}  {:>7.1}  {:>6.2}  {:>10.1}  {:>6.0}  {:>5.2}%\n",
        h.ts_code,
        h.name,
        h.industry,
        h.pb,
        h.pe,
        h.dv_ratio,
        h.net_equity_bn,
        h.total_score,
        h.weight * 100.0
    ));
}
