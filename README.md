# deep-value-rs 🦀

> 低估分散不深研 — Rust 实现的深度价值策略选股与回测框架

[![Rust](https://img.shields.io/badge/rust-1.95%2B-orange)](https://www.rust-lang.org)

## 功能

- 🔌 **Tushare REST 客户端** — 直接调用 HTTP API，自动 Parquet 缓存
- 📊 **九步选股管线** — PB/PE/股息率三维打分、审计检查、排雷、行业分散
- 📈 **回测引擎** — 季度再平衡净值曲线、基准对比、风险指标
- 🛡️ **前视偏差修复** — `safe_financial_year()` 确保使用已公开财报数据
- 🧪 **44+ 单元测试** — 覆盖打分、排雷、行业 cap、回测指标

## 快速开始

```bash
# 1. 配置 Tushare Token
echo "TUSHARE_TOKEN=your_token_here" > .env

# 2. 测试连通性
cargo run -- ping

# 3. 单次选股 (Phase 3 完成后)
cargo run -- snapshot --date 20250515 --top 10

# 4. 清除缓存
cargo run -- cache clear
```

## 项目结构

```
src/
├── main.rs              # CLI 入口 (clap)
├── lib.rs               # 库根
├── config.rs            # .env 配置加载
├── tushare/             # Tushare REST API 客户端
│   ├── client.rs        # HTTP POST + DataFrame 转换
│   ├── types.rs         # 请求/响应类型 (serde)
│   └── cache.rs         # Parquet 文件缓存
├── data/                # 数据获取层
│   ├── cross_section.rs # A 股横截面 (daily_basic + stock_basic)
│   ├── financials.rs    # 十年财务 + safe_financial_year
│   └── audit.rs         # 四大审计机构检查
├── strategy/            # 策略逻辑
│   ├── domain.rs        # 核心数据结构
│   ├── scoring.rs       # PB/PE/股息率三维打分
│   ├── anomaly.rs       # 排雷 (规则 1a/1b/2)
│   └── screening.rs     # 行业 cap + 等权仓位
├── backtest/            # 回测引擎
│   ├── engine.rs        # 价格获取 + 收益计算
│   └── metrics.rs       # 风险指标 (夏普/回撤/卡尔马)
└── report/              # 报告输出
    └── formatter.rs     # 中文格式化
```

## 技术栈

| 功能 | Crate |
|------|-------|
| HTTP | `reqwest` |
| DataFrame | `polars` |
| CLI | `clap` |
| 序列化 | `serde` + `serde_json` |
| 异步 | `tokio` |
| 日志 | `tracing` |
| 配置 | `dotenvy` |
| 日期 | `chrono` |
| 错误处理 | `anyhow` + `thiserror` |

## License

MIT
