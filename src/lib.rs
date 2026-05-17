//! Deep Value 量化回测框架。
//!
//! 一个用 Rust 实现的深度价值策略选股、回测和稳健性检验工具。
//!
//! # 模块架构
//!
//! - [`tushare`] — Tushare REST API 客户端（HTTP + Parquet 缓存）
//! - [`config`] — 配置加载（.env + 环境变量）
//! - [`data`] — 数据获取（横截面、财务、审计）
//! - [`strategy`] — 策略逻辑（领域类型、打分、排雷、筛选）
//! - [`backtest`] — 回测引擎（价格获取、收益计算、风险指标）
//! - [`report`] — 报告格式化输出

pub mod backtest;
pub mod config;
pub mod data;
pub mod db;
pub mod report;
pub mod strategy;
pub mod tushare;
