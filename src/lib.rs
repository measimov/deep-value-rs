//! Deep Value 量化回测框架。
//!
//! 一个用 Rust 实现的深度价值策略选股、回测和稳健性检验工具。
//!
//! # 模块架构
//!
//! - [`tushare`] — Tushare REST API 客户端（HTTP + Parquet 缓存）
//! - [`config`] — 配置加载（.env + 环境变量）

pub mod config;
pub mod tushare;
