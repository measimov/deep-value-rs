//! 核心数据结构 — 对应 Python 版 `domain.py` 中的 dataclass。
//!
//! 所有 struct 都是纯数据类型，不包含 IO 逻辑。

use std::collections::HashMap;

// =====================================================================
// 策略配置
// =====================================================================

/// 深度价值策略参数。
#[derive(Debug, Clone)]
pub struct DeepValueConfig {
    // --- 市场 ---
    /// 目标市场：`"ashare"` / `"hk"` / `"all"`
    pub market: String,

    // --- 市场前提判断 ---
    /// 全市场 PB 中位数 ≥ 此值则禁止建仓。
    pub market_pb_threshold: f64,
    /// true = PB 超阈值不建仓；false = 仅警告，继续选股。
    pub enforce_market_gate: bool,

    // --- 筛选阈值 ---
    /// Step 1: PB > pb_max → 删除。
    pub pb_max: f64,
    /// Step 2: 十年 PB max 从未超过此值 → 删除。
    pub pb_10y_must_exceed: f64,
    /// Step 3: 非四大审计 → 删除。
    pub audit_big4_required: bool,
    /// Step 3: 净资产(亿) > 此值免除审计要求。
    pub audit_exemption_equity_bn: f64,
    /// Step 4: 股息率 < 此值 → 删除。
    pub dv_ratio_min: f64,
    /// Step 5: 净资产(亿) < 此值 → 删除。
    pub net_equity_min_bn: f64,

    // --- 打分 & 选股 ---
    /// Step 9: 最终选取前 N 只。
    pub top_n: usize,
    /// 行业占比上限。
    pub industry_cap: f64,

    // --- 仓位 ---
    /// 目标股票仓位比例（余额为现金/债券）。
    pub target_equity_weight: f64,

    // --- 控制 ---
    /// strict=true: Step 2 (十年 PB) 强制执行。
    pub strict: bool,
    /// 十年回溯年数。
    pub lookback_years: usize,
}

impl Default for DeepValueConfig {
    fn default() -> Self {
        Self {
            market: "ashare".to_string(),
            market_pb_threshold: 2.0,
            enforce_market_gate: true,
            pb_max: 1.5,
            pb_10y_must_exceed: 1.0,
            audit_big4_required: true,
            audit_exemption_equity_bn: 500.0,
            dv_ratio_min: 0.5,
            net_equity_min_bn: 100.0,
            top_n: 30,
            industry_cap: 0.20,
            target_equity_weight: 0.50,
            strict: true,
            lookback_years: 10,
        }
    }
}

// =====================================================================
// 四大审计机构
// =====================================================================

/// 四大审计机构匹配关键词。
pub const BIG4_PATTERNS: &[&str] = &[
    "普华永道",
    "PricewaterhouseCoopers",
    "PwC",
    "德勤",
    "Deloitte",
    "安永",
    "Ernst & Young",
    "EY",
    "毕马威",
    "KPMG",
];

/// 判断审计机构名称是否属于四大。
pub fn is_big4(auditor: &str) -> bool {
    let upper = auditor.to_uppercase();
    BIG4_PATTERNS
        .iter()
        .any(|p| upper.contains(&p.to_uppercase()))
}

// =====================================================================
// 筛选步骤记录
// =====================================================================

/// 单步筛选记录 — 用于输出透明度。
#[derive(Debug, Clone)]
pub struct StepRecord {
    pub step_name: String,
    pub step_number: u8,
    pub before_count: usize,
    pub after_count: usize,
    /// 数据不完整但仍执行了该步骤。
    pub degraded: bool,
    /// 该步骤被完全跳过。
    pub skipped: bool,
    /// 额外说明（如降级原因）。
    pub note: String,
}

// =====================================================================
// 持仓
// =====================================================================

/// 单只持仓。
#[derive(Debug, Clone)]
pub struct Holding {
    pub ts_code: String,
    pub name: String,
    pub market: String,
    pub industry: String,
    pub pb: f64,
    pub pe: f64,
    pub dv_ratio: f64,
    /// 净资产（亿元）。
    pub net_equity_bn: f64,
    pub pb_score: f64,
    pub pe_score: f64,
    pub div_score: f64,
    pub total_score: f64,
    /// 等权仓位权重 (0~1)。
    pub weight: f64,
}

/// 被排雷剔除的股票及原因。
#[derive(Debug, Clone)]
pub struct EliminatedStock {
    pub ts_code: String,
    pub name: String,
    pub reason: String,
}

// =====================================================================
// 快照结果
// =====================================================================

/// 单期快照结果。
#[derive(Debug, Clone)]
pub struct SnapshotResult {
    pub trade_date: String,
    pub market: String,

    /// 各市场 PB 中位数: `{ "ashare": 1.35 }`
    pub market_pb_map: HashMap<String, f64>,
    /// 各市场是否可建仓
    pub is_investable_map: HashMap<String, bool>,

    /// 九步筛选过程
    pub step_records: Vec<StepRecord>,

    /// 最终持仓
    pub holdings: Vec<Holding>,
    /// 被排雷剔除的股票
    pub eliminated: Vec<EliminatedStock>,
    /// 行业分布: `{ "银行": 3, "地产": 2 }`
    pub industry_dist: HashMap<String, usize>,

    /// 数据告警
    pub data_warnings: Vec<String>,
}

// =====================================================================
// 再平衡
// =====================================================================

/// 季度再平衡记录。
#[derive(Debug, Clone)]
pub struct RebalanceRecord {
    pub date: String,
    pub snapshot: SnapshotResult,
    /// 新增股票代码
    pub added: Vec<String>,
    /// 移除股票代码
    pub removed: Vec<String>,
    /// 保留股票代码
    pub retained: Vec<String>,
    /// 重合率 (0~1)
    pub overlap_ratio: f64,
}

// =====================================================================
// 交易成本
// =====================================================================

/// 交易成本参数。
#[derive(Debug, Clone)]
pub struct CostConfig {
    /// A 股佣金费率 (双边, 默认万三)
    pub commission_rate: f64,
    /// 印花税 (卖出, 默认千一)
    pub stamp_tax: f64,
    /// 冲击成本 (默认千一)
    pub slippage: f64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            commission_rate: 0.0003,
            stamp_tax: 0.001,
            slippage: 0.001,
        }
    }
}

// =====================================================================
// 回测指标
// =====================================================================

/// 回测汇总指标。
#[derive(Debug, Clone)]
pub struct BacktestMetrics {
    pub total_return: f64,
    pub annualized_return: f64,
    pub benchmark_total_return: f64,
    pub benchmark_annualized: f64,
    pub excess_return: f64,
    pub max_drawdown: f64,
    pub max_drawdown_start: String,
    pub max_drawdown_end: String,
    pub sharpe_ratio: f64,
    pub calmar_ratio: f64,
    pub volatility: f64,
    pub win_rate: f64,
    pub total_turnover: f64,
    pub total_cost: f64,
    pub num_rebalances: usize,
    pub avg_holding_days: f64,
}

/// 单只持仓的持有期收益。
#[derive(Debug, Clone)]
pub struct HoldingReturn {
    pub ts_code: String,
    pub name: String,
    pub entry_date: String,
    pub exit_date: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub holding_return: f64,
    pub holding_days: u32,
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DeepValueConfig::default();
        assert_eq!(config.market, "ashare");
        assert_eq!(config.top_n, 30);
        assert!((config.market_pb_threshold - 2.0).abs() < f64::EPSILON);
        assert!(config.strict);
    }

    #[test]
    fn test_is_big4() {
        assert!(is_big4("普华永道中天会计师事务所"));
        assert!(is_big4("PricewaterhouseCoopers Zhong Tian"));
        assert!(is_big4("pwc")); // case-insensitive
        assert!(is_big4("德勤华永会计师事务所"));
        assert!(is_big4("Deloitte Touche Tohmatsu"));
        assert!(is_big4("安永华明会计师事务所"));
        assert!(is_big4("EY"));
        assert!(is_big4("毕马威华振"));
        assert!(is_big4("KPMG Huazhen"));
        assert!(!is_big4("立信会计师事务所"));
        assert!(!is_big4("天健会计师事务所"));
        assert!(!is_big4(""));
    }

    #[test]
    fn test_cost_config_default() {
        let cost = CostConfig::default();
        assert!((cost.commission_rate - 0.0003).abs() < f64::EPSILON);
        assert!((cost.stamp_tax - 0.001).abs() < f64::EPSILON);
    }
}
