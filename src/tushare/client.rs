//! Tushare REST HTTP 客户端。
//!
//! 封装了对 `http://api.tushare.pro` 的 POST 请求，
//! 并将返回的 JSON 转换为 polars DataFrame。

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use polars::prelude::*;
use reqwest::Client;
use tracing::{debug, info, warn};

use crate::db;

use super::pg_cache::PgCache;
use super::types::{TushareRequest, TushareResponse};

/// Tushare API 端点。
const TUSHARE_API_URL: &str = "http://api.tushare.pro";

/// 可配置的速率限制器。
pub struct RateLimiter {
    min_interval: Duration,
    last_call: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(min_interval_ms: u64) -> Self {
        Self {
            min_interval: Duration::from_millis(min_interval_ms),
            last_call: Mutex::new(Instant::now()),
        }
    }

    /// 如果距离上次调用不足 `min_interval`，则等待剩余时间。
    pub async fn wait_if_needed(&self) {
        let elapsed = {
            let last = self.last_call.lock().unwrap();
            last.elapsed()
        };
        if elapsed < self.min_interval {
            tokio::time::sleep(self.min_interval - elapsed).await;
        }
        let mut last = self.last_call.lock().unwrap();
        *last = Instant::now();
    }
}

/// Tushare REST 客户端。
///
/// # Example
/// ```no_run
/// use deep_value::tushare::client::TushareClient;
///
/// #[tokio::main]
/// async fn main() {
///     let client = TushareClient::new("your_token");
///     let df = client.query("trade_cal", &[("exchange", "SSE")], None).await.unwrap();
///     println!("{}", df);
/// }
/// ```
pub struct TushareClient {
    token: String,
    http: Client,
    pg_cache: Option<PgCache>,
}

impl TushareClient {
    /// 创建客户端。
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            http: Client::new(),
            pg_cache: None,
        }
    }

    /// 创建使用 PostgreSQL raw store 的客户端。
    pub async fn new_with_pg(token: &str, database_url: &str) -> Result<Self> {
        let pool = db::connect(database_url).await?;
        db::init_schema(&pool).await?;
        Ok(Self::with_pg_cache(token, PgCache::new(pool)))
    }

    /// 创建使用指定 PostgreSQL raw store 的客户端。
    pub fn with_pg_cache(token: &str, pg_cache: PgCache) -> Self {
        Self {
            token: token.to_string(),
            http: Client::new(),
            pg_cache: Some(pg_cache),
        }
    }

    /// 获取内部 PgCache 的引用。
    pub fn pg_cache(&self) -> Option<&PgCache> {
        self.pg_cache.as_ref()
    }

    /// 通用查询：调用任意 Tushare API 并返回 DataFrame。
    ///
    /// 优先从 PostgreSQL raw 缓存读取；未命中时发送 HTTP 请求并写入缓存。
    pub async fn query(
        &self,
        api_name: &str,
        params: &[(&str, &str)],
        fields: Option<&str>,
    ) -> Result<DataFrame> {
        let param_map: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let cache_key = Self::build_cache_key(api_name, &param_map, fields);

        // 尝试读 PostgreSQL raw store
        if let Some(pg_cache) = &self.pg_cache {
            if let Some(raw) = pg_cache.load_raw(&cache_key).await? {
                let df = self.response_to_dataframe(&raw.response_fields, &raw.response_items)?;
                debug!(
                    api = api_name,
                    rows = df.height(),
                    "PostgreSQL raw 缓存命中"
                );
                return Ok(df);
            }
        }

        self.execute_and_cache(api_name, &param_map, fields).await
    }

    /// 强制调用 Tushare API（绕过 raw 缓存），仍会写入 raw + typed 缓存。
    ///
    /// 用于 `sync` 命令确保 typed 表始终被刷新。
    pub async fn query_force(
        &self,
        api_name: &str,
        params: &[(&str, &str)],
        fields: Option<&str>,
    ) -> Result<DataFrame> {
        let param_map: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        self.execute_and_cache(api_name, &param_map, fields).await
    }

    /// 发送 HTTP 请求，反序列化，写入 raw + typed 缓存。
    async fn execute_and_cache(
        &self,
        api_name: &str,
        param_map: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<DataFrame> {
        let cache_key = Self::build_cache_key(api_name, param_map, fields);

        let request = match fields {
            Some(f) => TushareRequest::with_fields(api_name, &self.token, param_map.clone(), f),
            None => TushareRequest::new(api_name, &self.token, param_map.clone()),
        };

        debug!(api = api_name, "发送请求...");

        let response = self
            .http
            .post(TUSHARE_API_URL)
            .json(&request)
            .send()
            .await
            .context("HTTP 请求失败")?;

        let body = response
            .json::<TushareResponse>()
            .await
            .context("JSON 解析失败")?;

        if !body.is_ok() {
            bail!("Tushare API 错误 ({}): {}", api_name, body.error_message());
        }

        let data = body.data.context("Tushare 返回数据为空")?;

        if data.has_more.unwrap_or(false) {
            warn!(
                api = api_name,
                "Tushare 返回 has_more=true，当前未实现分页，结果可能不完整"
            );
        }

        let df = self.response_to_dataframe(&data.fields, &data.items)?;

        info!(api = api_name, rows = df.height(), "请求完成");

        if let Some(pg_cache) = &self.pg_cache {
            pg_cache
                .save_raw(
                    &cache_key,
                    api_name,
                    param_map,
                    fields,
                    &data.fields,
                    &data.items,
                )
                .await?;
            pg_cache
                .save_typed(api_name, param_map, &data.fields, &data.items)
                .await?;
        }

        Ok(df)
    }

    /// 查询（不使用缓存）。
    pub async fn query_no_cache(
        &self,
        api_name: &str,
        params: &[(&str, &str)],
        fields: Option<&str>,
    ) -> Result<DataFrame> {
        let param_map: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let request = match fields {
            Some(f) => TushareRequest::with_fields(api_name, &self.token, param_map, f),
            None => TushareRequest::new(api_name, &self.token, param_map),
        };

        let response = self
            .http
            .post(TUSHARE_API_URL)
            .json(&request)
            .send()
            .await
            .context("HTTP 请求失败")?;

        let body = response
            .json::<TushareResponse>()
            .await
            .context("JSON 解析失败")?;

        if !body.is_ok() {
            bail!("Tushare API 错误 ({}): {}", api_name, body.error_message());
        }

        let data = body.data.context("Tushare 返回数据为空")?;
        self.response_to_dataframe(&data.fields, &data.items)
    }

    /// 连通性检查 — 拉取交易日历验证 token 是否有效。
    pub async fn ping(&self) -> Result<String> {
        let df = self
            .query_no_cache(
                "trade_cal",
                &[
                    ("exchange", "SSE"),
                    ("start_date", "20250101"),
                    ("end_date", "20250110"),
                ],
                Some("exchange,cal_date,is_open"),
            )
            .await?;

        Ok(format!(
            "✅ Tushare 连接成功！返回 {} 行交易日历数据",
            df.height()
        ))
    }

    /// 将 Tushare 返回的 fields + items 转换为 polars DataFrame。
    ///
    /// Tushare 返回的 items 是 `Vec<Vec<Value>>`，其中 Value 可能是
    /// string, number, null。我们统一转为 String 列，后续按需转换类型。
    fn response_to_dataframe(
        &self,
        fields: &[String],
        items: &[Vec<serde_json::Value>],
    ) -> Result<DataFrame> {
        if fields.is_empty() || items.is_empty() {
            // 返回空 DataFrame (保留列名)
            let columns: Vec<Column> = fields
                .iter()
                .map(|name| Column::new(PlSmallStr::from(name.as_str()), Vec::<String>::new()))
                .collect();
            return Ok(DataFrame::new(columns)?);
        }

        // 按列构建 Series
        let columns: Vec<Column> = fields
            .iter()
            .enumerate()
            .map(|(col_idx, name)| {
                let values: Vec<Option<String>> = items
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| match v {
                            serde_json::Value::Null => None,
                            serde_json::Value::String(s) => Some(s.clone()),
                            serde_json::Value::Number(n) => Some(n.to_string()),
                            serde_json::Value::Bool(b) => Some(b.to_string()),
                            _ => Some(v.to_string()),
                        })
                    })
                    .collect();

                Column::new(
                    PlSmallStr::from(name.as_str()),
                    values
                        .iter()
                        .map(|v| v.as_deref())
                        .collect::<Vec<Option<&str>>>(),
                )
            })
            .collect();

        Ok(DataFrame::new(columns)?)
    }

    /// 构建缓存 key。
    pub fn cache_key_for(api_name: &str, params: &[(&str, &str)], fields: Option<&str>) -> String {
        let param_map: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self::build_cache_key(api_name, &param_map, fields)
    }

    fn build_cache_key(
        api_name: &str,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> String {
        let mut parts = vec![api_name.to_string()];

        // 按 key 排序确保稳定性
        let mut sorted_params: Vec<_> = params.iter().collect();
        sorted_params.sort_by_key(|(k, _)| (*k).clone());
        for (k, v) in sorted_params {
            parts.push(format!("{k}_{v}"));
        }

        if let Some(f) = fields {
            // 对 fields 做 hash 防止文件名过长
            let hash: u64 = f
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
            parts.push(format!("f{hash:x}"));
        }

        parts.join("__")
    }
}
