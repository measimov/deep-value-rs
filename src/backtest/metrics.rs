//! 风险指标计算 — 夏普/回撤/卡尔马/波动率。

/// 计算年化收益率。
pub fn annualized_return(total_return: f64, days: usize) -> f64 {
    if days == 0 {
        return 0.0;
    }
    let years = days as f64 / 365.25;
    (1.0 + total_return).powf(1.0 / years) - 1.0
}

/// 计算年化波动率。
pub fn annualized_volatility(daily_returns: &[f64]) -> f64 {
    if daily_returns.len() < 2 {
        return 0.0;
    }
    let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
    let variance = daily_returns
        .iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>()
        / daily_returns.len() as f64;
    variance.sqrt() * (252.0_f64).sqrt()
}

/// 计算夏普比率 (rf=3%)。
pub fn sharpe_ratio(daily_returns: &[f64], risk_free_rate: f64) -> f64 {
    if daily_returns.len() < 2 {
        return 0.0;
    }
    let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
    let variance = daily_returns
        .iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>()
        / daily_returns.len() as f64;
    let std = variance.sqrt();

    if std < 1e-12 {
        return 0.0;
    }

    let daily_rf = risk_free_rate / 252.0;
    (mean - daily_rf) / std * (252.0_f64).sqrt()
}

/// 计算最大回撤及其起止日期索引。
pub fn max_drawdown(nav: &[f64]) -> (f64, usize, usize) {
    if nav.len() < 2 {
        return (0.0, 0, 0);
    }

    let mut peak = nav[0];
    let mut peak_idx = 0;
    let mut max_dd = 0.0_f64;
    let mut dd_start = 0;
    let mut dd_end = 0;

    for (i, &val) in nav.iter().enumerate() {
        if val > peak {
            peak = val;
            peak_idx = i;
        }
        let dd = val / peak - 1.0;
        if dd < max_dd {
            max_dd = dd;
            dd_start = peak_idx;
            dd_end = i;
        }
    }

    (max_dd, dd_start, dd_end)
}

/// 计算卡尔马比率。
pub fn calmar_ratio(annualized_ret: f64, max_dd: f64) -> f64 {
    if max_dd.abs() < 0.001 {
        return 0.0;
    }
    annualized_ret / max_dd.abs()
}

/// 计算换手率。
pub fn turnover_ratio(added: usize, removed: usize, total: usize) -> f64 {
    if total == 0 {
        return 0.0;
    }
    // 第一期全新建仓 → turnover = 100%
    if removed == 0 && added == total {
        return 1.0;
    }
    (added + removed) as f64 / (2 * total) as f64
}

/// 计算交易成本。
pub fn transaction_cost(
    added: usize,
    removed: usize,
    total: usize,
    commission: f64,
    stamp_tax: f64,
    slippage: f64,
) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let buy_cost = commission + slippage;
    let sell_cost = commission + stamp_tax + slippage;
    let buy_ratio = added as f64 / total as f64;
    let sell_ratio = removed as f64 / total as f64;
    buy_ratio * buy_cost + sell_ratio * sell_cost
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_annualized_return() {
        // 1 year, 10% total return → ~10% annualized
        let ret = annualized_return(0.10, 365);
        assert!((ret - 0.10).abs() < 0.01);
    }

    #[test]
    fn test_annualized_return_2years() {
        // 2 years, 21% total return → ~10% annualized
        let ret = annualized_return(0.21, 730);
        assert!((ret - 0.10).abs() < 0.01);
    }

    #[test]
    fn test_sharpe_positive() {
        let returns = vec![0.01, 0.02, -0.005, 0.015, 0.01, 0.005, 0.02];
        let sharpe = sharpe_ratio(&returns, 0.03);
        assert!(sharpe > 0.0);
    }

    #[test]
    fn test_max_drawdown() {
        let nav = vec![1.0, 1.1, 1.2, 0.9, 0.8, 1.0, 1.3];
        let (dd, start, end) = max_drawdown(&nav);
        // Peak at 1.2, trough at 0.8 → dd = 0.8/1.2 - 1 = -0.333
        assert!((dd - (-1.0 / 3.0)).abs() < 0.01);
        assert_eq!(start, 2); // peak at index 2
        assert_eq!(end, 4);   // trough at index 4
    }

    #[test]
    fn test_max_drawdown_no_drawdown() {
        let nav = vec![1.0, 1.1, 1.2, 1.3];
        let (dd, _, _) = max_drawdown(&nav);
        assert!((dd - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_calmar() {
        let calmar = calmar_ratio(0.15, -0.20);
        assert!((calmar - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_turnover_first_period() {
        // 第一期全新建仓
        let t = turnover_ratio(30, 0, 30);
        assert!((t - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_turnover_regular() {
        // 5 added, 5 removed, 30 total
        let t = turnover_ratio(5, 5, 30);
        assert!((t - 10.0 / 60.0).abs() < 1e-10);
    }

    #[test]
    fn test_volatility() {
        let returns = vec![0.01, -0.01, 0.01, -0.01];
        let vol = annualized_volatility(&returns);
        assert!(vol > 0.0);
    }
}
