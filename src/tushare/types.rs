//! Tushare API 请求/响应类型定义。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tushare API 请求体。
///
/// 所有 Tushare Pro 接口均通过 HTTP POST 发送 JSON，格式固定：
/// ```json
/// {
///   "api_name": "daily_basic",
///   "token": "your_token",
///   "params": {"trade_date": "20250515"},
///   "fields": "ts_code,pb,pe"
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct TushareRequest {
    pub api_name: String,
    pub token: String,
    pub params: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<String>,
}

/// Tushare API 响应体。
///
/// 成功时 `code == 0`，数据在 `data` 中。
/// 失败时 `code != 0`，错误信息在 `msg` 中。
#[derive(Debug, Clone, Deserialize)]
pub struct TushareResponse {
    pub code: i32,
    #[serde(default)]
    pub msg: Option<String>,
    #[serde(default)]
    pub data: Option<TushareData>,
}

/// Tushare 返回的数据载体。
///
/// `fields` 是列名列表，`items` 是二维数组（行 × 列）。
/// 与 pandas DataFrame 的 columns + values 类似。
#[derive(Debug, Clone, Deserialize)]
pub struct TushareData {
    pub fields: Vec<String>,
    pub items: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    pub has_more: Option<bool>,
}

impl TushareRequest {
    /// 创建请求（无字段过滤）。
    pub fn new(api_name: &str, token: &str, params: HashMap<String, String>) -> Self {
        Self {
            api_name: api_name.to_string(),
            token: token.to_string(),
            params,
            fields: None,
        }
    }

    /// 创建请求（指定返回字段）。
    pub fn with_fields(
        api_name: &str,
        token: &str,
        params: HashMap<String, String>,
        fields: &str,
    ) -> Self {
        Self {
            api_name: api_name.to_string(),
            token: token.to_string(),
            params,
            fields: Some(fields.to_string()),
        }
    }
}

impl TushareResponse {
    /// 检查是否成功。
    pub fn is_ok(&self) -> bool {
        self.code == 0
    }

    /// 获取错误信息。
    pub fn error_message(&self) -> String {
        self.msg
            .clone()
            .unwrap_or_else(|| format!("unknown error (code={})", self.code))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let mut params = HashMap::new();
        params.insert("trade_date".into(), "20250515".into());
        let req = TushareRequest::with_fields("daily_basic", "test_token", params, "ts_code,pb");

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("daily_basic"));
        assert!(json.contains("test_token"));
        assert!(json.contains("ts_code,pb"));
    }

    #[test]
    fn test_response_deserialization() {
        let json = r#"{
            "code": 0,
            "msg": null,
            "data": {
                "fields": ["ts_code", "pb"],
                "items": [["000001.SZ", 0.51], ["600016.SH", 0.33]]
            }
        }"#;
        let resp: TushareResponse = serde_json::from_str(json).unwrap();
        assert!(resp.is_ok());
        let data = resp.data.unwrap();
        assert_eq!(data.fields, vec!["ts_code", "pb"]);
        assert_eq!(data.items.len(), 2);
    }

    #[test]
    fn test_error_response() {
        let json = r#"{"code": -2001, "msg": "Invalid token"}"#;
        let resp: TushareResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.is_ok());
        assert_eq!(resp.error_message(), "Invalid token");
    }
}
