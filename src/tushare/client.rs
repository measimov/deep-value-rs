//! Tushare REST HTTP 客户端。
//!
//! 封装了对 `http://api.tushare.pro` 的 POST 请求，
//! 并将返回的 JSON 转换为 polars DataFrame。

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use polars::prelude::*;
use reqwest::Client;
use tracing::{debug, info};

use super::cache::Cache;
use super::types::{TushareRequest, TushareResponse};

/// Tushare API 端点。
const TUSHARE_API_URL: &str = "http://api.tushare.pro";

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
    cache: Cache,
}

impl TushareClient {
    /// 创建客户端。
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            http: Client::new(),
            cache: Cache::new("data/cache"),
        }
    }

    /// 通用查询：调用任意 Tushare API 并返回 DataFrame。
    ///
    /// # Arguments
    /// * `api_name` - 接口名称 (如 "daily_basic", "income" 等)
    /// * `params` - 参数键值对
    /// * `fields` - 可选，指定返回字段 (逗号分隔)
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

        // 构建缓存 key
        let cache_key = self.build_cache_key(api_name, &param_map, fields);

        // 尝试读缓存
        if let Some(df) = self.cache.load(&cache_key)? {
            debug!(api = api_name, rows = df.height(), "缓存命中");
            return Ok(df);
        }

        // 构建请求
        let request = match fields {
            Some(f) => TushareRequest::with_fields(api_name, &self.token, param_map, f),
            None => TushareRequest::new(api_name, &self.token, param_map),
        };

        debug!(api = api_name, "发送请求...");

        // 发送 HTTP POST
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

        // 转换为 DataFrame
        let df = self.response_to_dataframe(&data.fields, &data.items)?;

        info!(api = api_name, rows = df.height(), "请求完成");

        // 写缓存
        if df.height() > 0 {
            self.cache.save(&cache_key, &df)?;
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
    fn build_cache_key(
        &self,
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
