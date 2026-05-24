//! 中文格式化输出 — 对应 Python 版 `format_snapshot_result()`。

use crate::backtest::engine::BacktestResult;
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
        let icon = if investable {
            "✅ 可建仓"
        } else {
            "❌ 不建仓"
        };
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
    out.push_str(&format!("【最终持仓】共 {} 只\n", result.holdings.len()));
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

/// 格式化回测结果为中文报告。
pub fn format_backtest(result: &BacktestResult) -> String {
    let m = &result.metrics;
    let sep = "=".repeat(60);
    let mut out = String::with_capacity(4096);

    out.push_str(&format!("{sep}\n"));
    out.push_str("  低估分散不深研 — 季度再平衡回测报告\n");
    out.push_str(&format!("{sep}\n\n"));

    // 关键指标
    out.push_str("【核心指标】\n");
    out.push_str(&format!("  累计收益     {:.2}%\n", m.total_return * 100.0));
    out.push_str(&format!("  年化收益     {:.2}%\n", m.annualized_return * 100.0));
    out.push_str(&format!("  基准收益     {:.2}%\n", m.benchmark_total_return * 100.0));
    out.push_str(&format!("  超额收益     {:.2}%\n", m.excess_return * 100.0));
    out.push_str(&format!("  最大回撤     {:.2}%\n", m.max_drawdown * 100.0));
    out.push_str(&format!("  回撤区间     {} .. {}\n", m.max_drawdown_start, m.max_drawdown_end));
    out.push_str(&format!("  年化波动率   {:.2}%\n", m.volatility * 100.0));
    out.push_str(&format!("  夏普比率     {:.2}\n", m.sharpe_ratio));
    out.push_str(&format!("  卡尔马比率   {:.2}\n", m.calmar_ratio));
    out.push_str(&format!("  胜率         {:.1}%\n", m.win_rate * 100.0));
    out.push_str(&format!("  总换手率     {:.0}%\n", m.total_turnover));
    out.push_str(&format!("  总交易成本   {:.2}%\n", m.total_cost));
    out.push_str(&format!("  再平衡次数   {}\n", m.num_rebalances));
    out.push('\n');

    // 各期收益
    out.push_str(&format!("【各期收益】共 {} 期\n", result.period_returns.len()));
    out.push_str(&format!(
        "  {:<12} {:<12} {:>8} {:>8} {:>8} {:>6} {:>5} {:>5}\n",
        "起始", "结束", "总收益%", "净收益%", "换手%", "持仓", "新增", "退出"
    ));
    out.push_str(&format!("  {}\n", "-".repeat(75)));
    for p in &result.period_returns {
        out.push_str(&format!(
            "  {:<12} {:<12} {:>7.2} {:>7.2} {:>7.1} {:>5} {:>5} {:>5}\n",
            p.date,
            p.end_date,
            p.gross_return * 100.0,
            p.net_return * 100.0,
            p.turnover * 100.0,
            p.holdings_count,
            p.added,
            p.removed,
        ));
    }

    out.push_str(&format!("\n{sep}\n"));
    out
}
