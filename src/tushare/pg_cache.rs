//! PostgreSQL storage for raw Tushare responses.

use std::collections::HashMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

/// Raw Tushare response restored from PostgreSQL.
#[derive(Debug, Clone, PartialEq)]
pub struct RawTushareResponse {
    pub cache_key: String,
    pub api_name: String,
    pub params: HashMap<String, String>,
    pub requested_fields: Option<String>,
    pub response_fields: Vec<String>,
    pub response_items: Vec<Vec<Value>>,
    pub row_count: i32,
}

/// PostgreSQL-backed raw response store.
#[derive(Clone)]
pub struct PgCache {
    pool: PgPool,
}

impl PgCache {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Load a raw Tushare response by cache key.
    pub async fn load_raw(&self, cache_key: &str) -> Result<Option<RawTushareResponse>> {
        let row = sqlx::query(
            r#"
            select cache_key, api_name, params, requested_fields,
                   response_fields, response_items, row_count
            from deep_value.tushare_raw_responses
            where cache_key = $1
            "#,
        )
        .bind(cache_key)
        .fetch_optional(&self.pool)
        .await
        .context("读取 PostgreSQL raw Tushare 响应失败")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let params_value: Value = row.try_get("params")?;
        let params: HashMap<String, String> =
            serde_json::from_value(params_value).context("解析 raw params 失败")?;

        let response_items_value: Value = row.try_get("response_items")?;
        let response_items: Vec<Vec<Value>> =
            serde_json::from_value(response_items_value).context("解析 raw response_items 失败")?;

        Ok(Some(RawTushareResponse {
            cache_key: row.try_get("cache_key")?,
            api_name: row.try_get("api_name")?,
            params,
            requested_fields: row.try_get("requested_fields")?,
            response_fields: row.try_get("response_fields")?,
            response_items,
            row_count: row.try_get("row_count")?,
        }))
    }

    /// Save or replace a raw Tushare response.
    pub async fn save_raw(
        &self,
        cache_key: &str,
        api_name: &str,
        params: &HashMap<String, String>,
        requested_fields: Option<&str>,
        response_fields: &[String],
        response_items: &[Vec<Value>],
    ) -> Result<()> {
        let params_json = serde_json::to_value(params).context("序列化 raw params 失败")?;
        let items_json =
            serde_json::to_value(response_items).context("序列化 raw response_items 失败")?;
        let row_count = i32::try_from(response_items.len()).context("raw row_count 超出 i32")?;

        sqlx::query(
            r#"
            insert into deep_value.tushare_raw_responses (
                cache_key, api_name, params, requested_fields,
                response_fields, response_items, row_count
            )
            values ($1, $2, $3, $4, $5, $6, $7)
            on conflict (cache_key) do update set
                api_name = excluded.api_name,
                params = excluded.params,
                requested_fields = excluded.requested_fields,
                response_fields = excluded.response_fields,
                response_items = excluded.response_items,
                row_count = excluded.row_count,
                updated_at = now()
            "#,
        )
        .bind(cache_key)
        .bind(api_name)
        .bind(params_json)
        .bind(requested_fields)
        .bind(response_fields)
        .bind(items_json)
        .bind(row_count)
        .execute(&self.pool)
        .await
        .context("写入 PostgreSQL raw Tushare 响应失败")?;

        Ok(())
    }

    /// Delete one raw response by cache key. Used by tests.
    pub async fn delete_raw(&self, cache_key: &str) -> Result<u64> {
        let result = sqlx::query(
            r#"
            delete from deep_value.tushare_raw_responses
            where cache_key = $1
            "#,
        )
        .bind(cache_key)
        .execute(&self.pool)
        .await
        .context("删除 PostgreSQL raw Tushare 响应失败")?;

        Ok(result.rows_affected())
    }

    /// Delete all raw responses. Used by the cache clear CLI.
    pub async fn clear_all(&self) -> Result<u64> {
        let result = sqlx::query("delete from deep_value.tushare_raw_responses")
            .execute(&self.pool)
            .await
            .context("清空 PostgreSQL raw Tushare 响应失败")?;

        Ok(result.rows_affected())
    }
}
