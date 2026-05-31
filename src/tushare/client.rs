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
use tracing::{debug, info};

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

    /// 发送 HTTP 请求，自动处理分页（has_more），写入 raw + typed 缓存。
    ///
    /// 对已知支持 limit/offset 的 endpoint 启用自动分页，每页行数
    /// 由 page_size() 按 endpoint 决定。
    /// 其他 endpoint 若返回 has_more 则直接失败，避免缓存不完整结果。
    async fn execute_and_cache(
        &self,
        api_name: &str,
        param_map: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<DataFrame> {
        let max_pages = pagination_max_pages(api_name);

        let paginate = pagination_page_size(api_name);
        let pg_size = paginate.unwrap_or(0);
        // 分页进度键：endpoint 特定的复合字段，确保跨页唯一。
        // 如 index_daily 同一标的跨日期范围，ts_code 不变但 trade_date 推进。
        let progress_keys: &[&str] = progress_key_fields(api_name);
        let user_fields = fields.map(|f| f.to_string());
        // fields=None 时 Tushare 返回全部默认列，已包含进度键，不注入字段列表。
        let effective_fields = if paginate.is_some() {
            ensure_pagination_fields(user_fields.as_deref(), progress_keys)
        } else {
            fields.map(|f| f.to_string())
        };
        let cache_key = Self::build_cache_key(api_name, param_map, fields);
        let mut all_fields: Vec<String> = Vec::new();
        let mut all_items: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut prev_key: String = String::new();
        let mut guard_broken: Option<&str> = None;
        let mut offset = 0;
        let mut page = 0;

        loop {
            let mut paginated = param_map.clone();
            if paginate.is_some() {
                paginated.insert("limit".to_string(), pg_size.to_string());
                paginated.insert("offset".to_string(), offset.to_string());
            }

            let request = match effective_fields.as_deref() {
                Some(f) => TushareRequest::with_fields(api_name, &self.token, paginated, f),
                None => TushareRequest::new(api_name, &self.token, paginated),
            };

            if page == 0 {
                debug!(api = api_name, "发送请求...");
            } else {
                debug!(api = api_name, page, offset, "分页请求...");
            }

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

            if page == 0 {
                all_fields = data.fields.clone();
            }

            let row_count = data.items.len();
            if row_count == 0 {
                break;
            }

            // 进度守卫：端点特定复合键，检测 offset 是否实际推进
            let this_key = progress_key_str(&data.items[0], &all_fields, progress_keys);
            if page > 0 && this_key == prev_key {
                guard_broken = Some("分页首行复合键重复，offset 可能未被 endpoint 支持");
                break;
            }
            prev_key = this_key;

            all_items.extend(data.items);

            let has_more = data.has_more.unwrap_or(false);

            if unsupported_has_more(paginate, has_more) {
                tracing::error!(
                    api = api_name,
                    rows = row_count,
                    "Tushare 返回 has_more=true，但此 endpoint 不在分页白名单中，缓存未写入"
                );
                guard_broken = Some("非分页白名单 endpoint 返回 has_more=true");
                break;
            }

            if has_more {
                if page >= max_pages {
                    guard_broken = Some("超过最大分页数");
                    break;
                }
                page += 1;
                offset += pg_size;
                info!(
                    api = api_name,
                    page,
                    offset,
                    accumulated = all_items.len(),
                    "分页继续"
                );
            } else {
                break;
            }
        }

        if let Some(reason) = guard_broken {
            bail!(
                "Tushare 分页失败 ({}): {}（已累计 {} 行，page={}）。缓存未写入。",
                api_name,
                reason,
                all_items.len(),
                page
            );
        }

        // 移除内部分页追加的字段（若调用方未请求）
        if let Some(orig) = user_fields
            .as_deref()
            .filter(|fields| !fields.trim().is_empty())
        {
            let orig_set: Vec<&str> = orig
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            let strip: Vec<usize> = all_fields
                .iter()
                .enumerate()
                .filter(|(_, f)| !orig_set.contains(&f.as_str()))
                .map(|(i, _)| i)
                .rev()
                .collect();
            for pos in strip {
                all_fields.remove(pos);
                for row in &mut all_items {
                    row.remove(pos);
                }
            }
        }

        let df = self.response_to_dataframe(&all_fields, &all_items)?;

        if page > 0 {
            info!(
                api = api_name,
                pages = page + 1,
                rows = df.height(),
                "分页请求完成"
            );
        } else {
            info!(api = api_name, rows = df.height(), "请求完成");
        }

        // 保存合并后的完整结果（仅分页成功时到达此处）
        if let Some(pg_cache) = &self.pg_cache {
            if should_persist_raw_response(api_name) {
                pg_cache
                    .save_raw(
                        &cache_key,
                        api_name,
                        param_map,
                        fields,
                        &all_fields,
                        &all_items,
                    )
                    .await?;
            }
            pg_cache
                .save_typed(api_name, param_map, &all_fields, &all_items)
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
        if data.has_more.unwrap_or(false) {
            tracing::warn!(
                api = api_name,
                rows = data.items.len(),
                "query_no_cache: has_more=true，单页结果可能不完整。使用 query() 或 query_force() 以自动分页"
            );
        }
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

// ---- 分页辅助函数（模块级，便于单元测试） ----

fn pagination_page_size(api: &str) -> Option<usize> {
    match api {
        "disclosure_date" => Some(3000),
        "stock_basic" | "daily_basic" | "daily" | "balancesheet_vip" | "income_vip"
        | "fina_indicator_vip" | "cashflow_vip" | "forecast_vip" | "express_vip"
        | "index_weight" | "top10_holders" | "top10_floatholders" | "repurchase" | "adj_factor"
        | "index_daily" | "stk_limit" | "hk_daily" => Some(5000),
        "us_basic" | "us_daily" | "hk_daily_adj" | "hk_adjfactor" => Some(6000),
        "us_daily_adj" => Some(8000),
        "us_adjfactor" => Some(15000),
        "hk_income" | "hk_balancesheet" | "hk_cashflow" | "hk_fina_indicator" | "us_income"
        | "us_balancesheet" | "us_cashflow" | "us_fina_indicator" => Some(10000),
        "fina_mainbz_vip" => Some(100),
        "pledge_stat" => Some(1000),
        _ => None,
    }
}

fn pagination_max_pages(api: &str) -> usize {
    match api {
        // fina_mainbz_vip 单页上限为 100 行，但总量不限。
        // 全市场季度主营构成可能超过 2 万行，所以保留复合进度键守卫，
        // 同时允许更深的分页。
        "fina_mainbz_vip" => 2_000,
        _ => 200,
    }
}

fn progress_key_fields(api: &str) -> &[&str] {
    match api {
        "daily" | "daily_basic" | "adj_factor" | "index_daily" | "stk_limit" | "hk_daily"
        | "hk_daily_adj" | "hk_adjfactor" | "us_daily" | "us_daily_adj" => {
            &["ts_code", "trade_date"]
        }
        "us_adjfactor" => &["ts_code", "trade_date", "exchange"],
        "income_vip" | "balancesheet_vip" | "cashflow_vip" | "fina_indicator_vip"
        | "forecast_vip" | "express_vip" => &["ts_code", "end_date"],
        "hk_income" | "hk_balancesheet" | "hk_cashflow" | "hk_fina_indicator" | "us_income"
        | "us_balancesheet" | "us_cashflow" | "us_fina_indicator" => {
            &["ts_code", "end_date", "ind_name", "report_type", "ind_type"]
        }
        "fina_mainbz_vip" => &["ts_code", "end_date", "bz_item"],
        "index_weight" => &["index_code", "con_code", "trade_date"],
        "top10_holders" | "top10_floatholders" => &["ts_code", "end_date", "holder_name"],
        "pledge_stat" => &["ts_code", "end_date"],
        "repurchase" => &["ts_code", "ann_date", "end_date"],
        "disclosure_date" => &["ts_code", "end_date"],
        "us_basic" => &["ts_code", "enname", "classify", "list_date"],
        _ => &["ts_code"],
    }
}

fn unsupported_has_more(paginate: Option<usize>, has_more: bool) -> bool {
    paginate.is_none() && has_more
}

fn should_persist_raw_response(api: &str) -> bool {
    !matches!(
        api,
        "hk_daily"
            | "hk_daily_adj"
            | "hk_adjfactor"
            | "hk_income"
            | "hk_balancesheet"
            | "hk_cashflow"
            | "hk_fina_indicator"
            | "us_daily"
            | "us_daily_adj"
            | "us_adjfactor"
            | "us_income"
            | "us_balancesheet"
            | "us_cashflow"
            | "us_fina_indicator"
    )
}

fn progress_key_str(row: &[serde_json::Value], resp_fields: &[String], keys: &[&str]) -> String {
    keys.iter()
        .filter_map(|k| {
            resp_fields
                .iter()
                .position(|f| f == *k)
                .and_then(|idx| row.get(idx))
                .map(value_repr)
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn ensure_pagination_fields(fields: Option<&str>, keys: &[&str]) -> Option<String> {
    let base = match fields {
        Some(f) => f,
        None => return None,
    };
    if base.trim().is_empty() {
        return Some(base.to_string());
    }
    let existing: Vec<&str> = base
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let mut out = base.to_string();
    for k in keys {
        if !existing.contains(k) {
            if !out.is_empty() {
                out.push(',');
            }
            out.push_str(k);
        }
    }
    Some(out)
}

fn value_repr(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => format!("{v}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_page_size_endpoints() {
        assert_eq!(pagination_page_size("disclosure_date"), Some(3000));
        assert_eq!(pagination_page_size("stock_basic"), Some(5000));
        assert_eq!(pagination_page_size("daily"), Some(5000));
        assert_eq!(pagination_page_size("income_vip"), Some(5000));
        assert_eq!(pagination_page_size("cashflow_vip"), Some(5000));
        assert_eq!(pagination_page_size("forecast_vip"), Some(5000));
        assert_eq!(pagination_page_size("express_vip"), Some(5000));
        assert_eq!(pagination_page_size("fina_mainbz_vip"), Some(100));
        assert_eq!(pagination_page_size("index_weight"), Some(5000));
        assert_eq!(pagination_page_size("top10_holders"), Some(5000));
        assert_eq!(pagination_page_size("top10_floatholders"), Some(5000));
        assert_eq!(pagination_page_size("pledge_stat"), Some(1000));
        assert_eq!(pagination_page_size("repurchase"), Some(5000));
        assert_eq!(pagination_page_size("stk_limit"), Some(5000));
        assert_eq!(pagination_page_size("hk_daily"), Some(5000));
        assert_eq!(pagination_page_size("hk_daily_adj"), Some(6000));
        assert_eq!(pagination_page_size("hk_adjfactor"), Some(6000));
        assert_eq!(pagination_page_size("us_basic"), Some(6000));
        assert_eq!(pagination_page_size("us_daily"), Some(6000));
        assert_eq!(pagination_page_size("us_daily_adj"), Some(8000));
        assert_eq!(pagination_page_size("us_adjfactor"), Some(15000));
        assert_eq!(pagination_page_size("us_income"), Some(10000));
        assert_eq!(pagination_page_size("fina_audit"), None); // non-paginated
    }

    #[test]
    fn test_max_pages_endpoints() {
        assert_eq!(pagination_max_pages("daily_basic"), 200);
        assert_eq!(pagination_max_pages("stk_limit"), 200);
        assert_eq!(pagination_max_pages("fina_mainbz_vip"), 2_000);
    }

    #[test]
    fn test_progress_key_fields_per_endpoint() {
        // Market series: ts_code + trade_date
        assert_eq!(progress_key_fields("daily"), &["ts_code", "trade_date"]);
        assert_eq!(
            progress_key_fields("daily_basic"),
            &["ts_code", "trade_date"]
        );
        assert_eq!(
            progress_key_fields("index_daily"),
            &["ts_code", "trade_date"]
        );
        assert_eq!(progress_key_fields("stk_limit"), &["ts_code", "trade_date"]);
        // Financial VIP: ts_code + end_date
        assert_eq!(progress_key_fields("income_vip"), &["ts_code", "end_date"]);
        assert_eq!(
            progress_key_fields("cashflow_vip"),
            &["ts_code", "end_date"]
        );
        assert_eq!(
            progress_key_fields("fina_mainbz_vip"),
            &["ts_code", "end_date", "bz_item"]
        );
        assert_eq!(
            progress_key_fields("index_weight"),
            &["index_code", "con_code", "trade_date"]
        );
        assert_eq!(
            progress_key_fields("top10_holders"),
            &["ts_code", "end_date", "holder_name"]
        );
        assert_eq!(progress_key_fields("pledge_stat"), &["ts_code", "end_date"]);
        assert_eq!(
            progress_key_fields("repurchase"),
            &["ts_code", "ann_date", "end_date"]
        );
        // Unique per stock: ts_code only
        assert_eq!(progress_key_fields("stock_basic"), &["ts_code"]);
        assert_eq!(
            progress_key_fields("us_basic"),
            &["ts_code", "enname", "classify", "list_date"]
        );
        assert_eq!(
            progress_key_fields("us_adjfactor"),
            &["ts_code", "trade_date", "exchange"]
        );
        assert_eq!(
            progress_key_fields("us_income"),
            &["ts_code", "end_date", "ind_name", "report_type", "ind_type"]
        );
    }

    #[test]
    fn test_unsupported_has_more() {
        assert!(unsupported_has_more(None, true));
        assert!(!unsupported_has_more(Some(5000), true));
        assert!(!unsupported_has_more(None, false));
    }

    #[test]
    fn test_should_persist_raw_response() {
        assert!(should_persist_raw_response("daily"));
        assert!(should_persist_raw_response("hk_basic"));
        assert!(!should_persist_raw_response("hk_daily"));
        assert!(!should_persist_raw_response("us_income"));
    }

    #[test]
    fn test_progress_key_str_compound() {
        let fields = vec![
            "ts_code".to_string(),
            "trade_date".to_string(),
            "close".to_string(),
        ];
        let row = vec![json!("000001.SZ"), json!("20250515"), json!(12.5)];
        let keys: &[&str] = &["ts_code", "trade_date"];
        let key = progress_key_str(&row, &fields, keys);
        assert_eq!(key, "000001.SZ|20250515");
    }

    #[test]
    fn test_progress_key_str_missing_field_omitted() {
        let fields = vec!["trade_date".to_string(), "close".to_string()];
        let row = vec![json!("20250515"), json!(12.5)];
        let keys: &[&str] = &["ts_code", "trade_date"]; // ts_code missing from fields
        let key = progress_key_str(&row, &fields, keys);
        assert_eq!(key, "20250515"); // ts_code skipped, only trade_date used
    }

    #[test]
    fn test_ensure_pagination_fields_none() {
        // fields=None → return None (Tushare defaults include key fields)
        let result = ensure_pagination_fields(None, &["ts_code"]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_ensure_pagination_fields_blank_preserves_all_fields_request() {
        let result = ensure_pagination_fields(Some(""), &["ts_code", "end_date"]);
        assert_eq!(result.as_deref(), Some(""));
    }

    #[test]
    fn test_ensure_pagination_fields_injects_missing() {
        let result = ensure_pagination_fields(Some("end_date,n_income"), &["ts_code", "end_date"]);
        assert_eq!(result.as_deref(), Some("end_date,n_income,ts_code"));
    }

    #[test]
    fn test_ensure_pagination_fields_all_present() {
        let result =
            ensure_pagination_fields(Some("ts_code,end_date,n_income"), &["ts_code", "end_date"]);
        assert_eq!(result.as_deref(), Some("ts_code,end_date,n_income"));
    }
}
