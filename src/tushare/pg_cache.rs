//! PostgreSQL storage for raw Tushare responses.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use polars::prelude::*;
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

    /// Return all distinct trade_dates present in daily_basic typed table.
    pub async fn existing_daily_basic_dates(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct trade_date from deep_value.tushare_daily_basic order by trade_date"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 daily_basic 已有日期失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("trade_date").ok())
            .collect())
    }

    /// Return all distinct end_dates present in income typed table.
    pub async fn existing_income_periods(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct end_date from deep_value.tushare_income order by end_date"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 income 已有期间失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("end_date").ok())
            .collect())
    }

    /// Return all distinct end_dates present in balancesheet typed table.
    pub async fn existing_balancesheet_periods(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct end_date from deep_value.tushare_balancesheet order by end_date"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 balancesheet 已有期间失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("end_date").ok())
            .collect())
    }

    /// Return all distinct end_dates present in fina_indicator typed table.
    pub async fn existing_fina_indicator_periods(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct end_date from deep_value.tushare_fina_indicator order by end_date"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 fina_indicator 已有期间失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("end_date").ok())
            .collect())
    }

    /// Return ts_codes that have fina_audit records for a given period.
    pub async fn existing_audit_codes(&self, period: &str) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select ts_code from deep_value.tushare_fina_audit where period = $1"#,
        )
        .bind(period)
        .fetch_all(&self.pool)
        .await
        .context("查询 fina_audit 已有代码失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("ts_code").ok())
            .collect())
    }

    /// Return all distinct end_dates present in cashflow typed table.
    pub async fn existing_cashflow_periods(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct end_date from deep_value.tushare_cashflow order by end_date"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 cashflow 已有期间失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("end_date").ok())
            .collect())
    }

    /// Return ts_codes that have dividend records.
    pub async fn existing_dividend_codes(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct ts_code from deep_value.tushare_dividend"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 dividend 已有代码失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("ts_code").ok())
            .collect())
    }

    /// Save supported Tushare responses into typed tables.
    pub async fn save_typed(
        &self,
        api_name: &str,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        match api_name {
            "trade_cal" => self.save_trade_cal(params, fields, items).await,
            "stock_basic" => self.save_stock_basic(params, fields, items).await,
            "daily_basic" => self.save_daily_basic(params, fields, items).await,
            "income" | "income_vip" => self.save_income(params, fields, items).await,
            "dividend" => self.save_dividend(params, fields, items).await,
            "balancesheet" | "balancesheet_vip" => self.save_balancesheet(params, fields, items).await,
            "fina_audit" => self.save_fina_audit(params, fields, items).await,
            "daily" => {
                self.save_market_series("tushare_daily", "close", params, fields, items)
                    .await
            }
            "adj_factor" => {
                self.save_market_series("tushare_adj_factor", "adj_factor", params, fields, items)
                    .await
            }
            "index_daily" => {
                self.save_market_series("tushare_index_daily", "close", params, fields, items)
                    .await
            }
            "fina_indicator" | "fina_indicator_vip" => {
                self.save_fina_indicator(params, fields, items).await
            }
            "cashflow" | "cashflow_vip" => self.save_cashflow(params, fields, items).await,
            "disclosure_date" => self.save_disclosure_date(params, fields, items).await,
            _ => Ok(()),
        }
    }

    /// Load supported typed data as a DataFrame.
    pub async fn load_typed(
        &self,
        api_name: &str,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        match api_name {
            "trade_cal" => self.load_trade_cal(params, fields).await,
            "stock_basic" => self.load_stock_basic(params, fields).await,
            "daily_basic" => self.load_daily_basic(params, fields).await,
            "income" | "income_vip" => self.load_income(params, fields).await,
            "dividend" => self.load_dividend(params, fields).await,
            "balancesheet" | "balancesheet_vip" => self.load_balancesheet(params, fields).await,
            "fina_audit" => self.load_fina_audit(params, fields).await,
            "daily" => {
                self.load_market_series(
                    "tushare_daily",
                    "close",
                    params,
                    fields,
                    &["ts_code", "trade_date", "close"],
                )
                .await
            }
            "adj_factor" => {
                self.load_market_series(
                    "tushare_adj_factor",
                    "adj_factor",
                    params,
                    fields,
                    &["ts_code", "trade_date", "adj_factor"],
                )
                .await
            }
            "index_daily" => {
                self.load_market_series(
                    "tushare_index_daily",
                    "close",
                    params,
                    fields,
                    &["trade_date", "close"],
                )
                .await
            }
            "fina_indicator" | "fina_indicator_vip" => {
                self.load_fina_indicator(params, fields).await
            }
            "cashflow" | "cashflow_vip" => self.load_cashflow(params, fields).await,
            "disclosure_date" => self.load_disclosure_date(params, fields).await,
            _ => Ok(None),
        }
    }

    async fn save_trade_cal(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(cal_date_idx) = field_index(fields, "cal_date") else {
            return Ok(());
        };
        let exchange_idx = field_index(fields, "exchange");
        let is_open_idx = field_index(fields, "is_open");

        for row in items {
            let Some(cal_date) = cell_string(row, cal_date_idx) else {
                continue;
            };
            let exchange = exchange_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("exchange").cloned())
                .unwrap_or_default();
            let is_open = is_open_idx.and_then(|idx| cell_string(row, idx));

            sqlx::query(
                r#"
                insert into deep_value.tushare_trade_cal (exchange, cal_date, is_open)
                values ($1, $2, $3)
                on conflict (exchange, cal_date) do update set
                    is_open = excluded.is_open,
                    updated_at = now()
                "#,
            )
            .bind(exchange)
            .bind(cal_date)
            .bind(is_open)
            .execute(&self.pool)
            .await
            .context("写入 tushare_trade_cal 失败")?;
        }

        Ok(())
    }

    async fn save_stock_basic(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let name_idx = field_index(fields, "name");
        let industry_idx = field_index(fields, "industry");
        let list_status_idx = field_index(fields, "list_status");
        let list_date_idx = field_index(fields, "list_date");

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let name = name_idx.and_then(|idx| cell_string(row, idx));
            let industry = industry_idx.and_then(|idx| cell_string(row, idx));
            let list_status = list_status_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("list_status").cloned());
            let list_date = list_date_idx.and_then(|idx| cell_string(row, idx));

            sqlx::query(
                r#"
                insert into deep_value.tushare_stock_basic (ts_code, name, industry, list_status, list_date)
                values ($1, $2, $3, $4, $5)
                on conflict (ts_code) do update set
                    name = coalesce(excluded.name, deep_value.tushare_stock_basic.name),
                    industry = coalesce(excluded.industry, deep_value.tushare_stock_basic.industry),
                    list_status = coalesce(excluded.list_status, deep_value.tushare_stock_basic.list_status),
                    list_date = coalesce(excluded.list_date, deep_value.tushare_stock_basic.list_date),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(name)
            .bind(industry)
            .bind(list_status)
            .bind(list_date)
            .execute(&self.pool)
            .await
            .context("写入 tushare_stock_basic 失败")?;
        }

        Ok(())
    }

    async fn save_daily_basic(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let trade_date_idx = field_index(fields, "trade_date");
        let pb_idx = field_index(fields, "pb");
        let pe_idx = field_index(fields, "pe");
        let pe_ttm_idx = field_index(fields, "pe_ttm");
        let dv_ratio_idx = field_index(fields, "dv_ratio");
        let total_mv_idx = field_index(fields, "total_mv");

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let trade_date = trade_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("trade_date").cloned());
            let Some(trade_date) = trade_date else {
                continue;
            };

            sqlx::query(
                r#"
                insert into deep_value.tushare_daily_basic (
                    ts_code, trade_date, pb, pe, pe_ttm, dv_ratio, total_mv
                )
                values ($1, $2, $3, $4, $5, $6, $7)
                on conflict (ts_code, trade_date) do update set
                    pb = coalesce(excluded.pb, deep_value.tushare_daily_basic.pb),
                    pe = coalesce(excluded.pe, deep_value.tushare_daily_basic.pe),
                    pe_ttm = coalesce(excluded.pe_ttm, deep_value.tushare_daily_basic.pe_ttm),
                    dv_ratio = coalesce(excluded.dv_ratio, deep_value.tushare_daily_basic.dv_ratio),
                    total_mv = coalesce(excluded.total_mv, deep_value.tushare_daily_basic.total_mv),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(trade_date)
            .bind(pb_idx.and_then(|idx| cell_f64(row, idx)))
            .bind(pe_idx.and_then(|idx| cell_f64(row, idx)))
            .bind(pe_ttm_idx.and_then(|idx| cell_f64(row, idx)))
            .bind(dv_ratio_idx.and_then(|idx| cell_f64(row, idx)))
            .bind(total_mv_idx.and_then(|idx| cell_f64(row, idx)))
            .execute(&self.pool)
            .await
            .context("写入 tushare_daily_basic 失败")?;
        }

        Ok(())
    }

    async fn load_trade_cal(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let exchange = params.get("exchange").map(String::as_str).unwrap_or("SSE");
        let Some(start_date) = params.get("start_date") else {
            return Ok(None);
        };
        let Some(end_date) = params.get("end_date") else {
            return Ok(None);
        };
        let is_open = params.get("is_open").map(String::as_str);

        let rows = sqlx::query(
            r#"
            select exchange, cal_date, is_open
            from deep_value.tushare_trade_cal
            where exchange = $1
              and cal_date >= $2
              and cal_date <= $3
              and ($4::text is null or is_open = $4)
            order by cal_date
            "#,
        )
        .bind(exchange)
        .bind(start_date)
        .bind(end_date)
        .bind(is_open)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_trade_cal 失败")?;

        rows_to_dataframe(
            &requested_fields(fields, &["exchange", "cal_date", "is_open"]),
            &rows,
        )
    }

    async fn load_stock_basic(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let list_status = params.get("list_status").map(String::as_str);
        let rows = sqlx::query(
            r#"
            select ts_code, name, industry, list_status, list_date
            from deep_value.tushare_stock_basic
            where ($1::text is null or list_status = $1)
            order by ts_code
            "#,
        )
        .bind(list_status)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_stock_basic 失败")?;

        rows_to_dataframe(
            &requested_fields(fields, &["ts_code", "name", "industry", "list_status", "list_date"]),
            &rows,
        )
    }

    async fn load_daily_basic(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(trade_date) = params.get("trade_date") else {
            return Ok(None);
        };
        let rows = sqlx::query(
            r#"
            select ts_code, trade_date, pb, pe, pe_ttm, dv_ratio, total_mv
            from deep_value.tushare_daily_basic
            where trade_date = $1
            order by ts_code
            "#,
        )
        .bind(trade_date)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_daily_basic 失败")?;

        rows_to_dataframe(
            &requested_fields(
                fields,
                &[
                    "ts_code",
                    "trade_date",
                    "pb",
                    "pe",
                    "pe_ttm",
                    "dv_ratio",
                    "total_mv",
                ],
            ),
            &rows,
        )
    }

    async fn save_income(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let end_date_idx = field_index(fields, "end_date");
        let n_income_idx = field_index(fields, "n_income");
        let report_type = params.get("report_type").cloned().unwrap_or_default();

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let end_date = end_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("period").cloned());
            let Some(end_date) = end_date else {
                continue;
            };

            sqlx::query(
                r#"
                insert into deep_value.tushare_income (ts_code, end_date, report_type, n_income)
                values ($1, $2, $3, $4)
                on conflict (ts_code, end_date, report_type) do update set
                    n_income = coalesce(excluded.n_income, deep_value.tushare_income.n_income),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(end_date)
            .bind(&report_type)
            .bind(n_income_idx.and_then(|idx| cell_f64(row, idx)))
            .execute(&self.pool)
            .await
            .context("写入 tushare_income 失败")?;
        }

        Ok(())
    }

    async fn save_dividend(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let end_date_idx = field_index(fields, "end_date");
        let cash_div_tax_idx = field_index(fields, "cash_div_tax");
        let stk_div_idx = field_index(fields, "stk_div");

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let end_date = end_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("end_date").cloned());
            let cash_div_tax = cash_div_tax_idx.and_then(|idx| cell_f64(row, idx));
            let stk_div = stk_div_idx.and_then(|idx| cell_f64(row, idx));
            let row_hash = stable_row_hash("dividend", fields, row);

            sqlx::query(
                r#"
                insert into deep_value.tushare_dividend (
                    row_hash, ts_code, end_date, cash_div_tax, stk_div
                )
                values ($1, $2, $3, $4, $5)
                on conflict (row_hash) do update set
                    ts_code = excluded.ts_code,
                    end_date = excluded.end_date,
                    cash_div_tax = excluded.cash_div_tax,
                    stk_div = excluded.stk_div,
                    updated_at = now()
                "#,
            )
            .bind(row_hash)
            .bind(ts_code)
            .bind(end_date)
            .bind(cash_div_tax)
            .bind(stk_div)
            .execute(&self.pool)
            .await
            .context("写入 tushare_dividend 失败")?;
        }

        Ok(())
    }

    async fn save_balancesheet(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let end_date_idx = field_index(fields, "end_date");
        let total_eqy_idx = field_index(fields, "total_hldr_eqy_exc_min_int");
        let report_type = params.get("report_type").cloned().unwrap_or_default();

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let end_date = end_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("period").cloned());
            let Some(end_date) = end_date else {
                continue;
            };

            sqlx::query(
                r#"
                insert into deep_value.tushare_balancesheet (
                    ts_code, end_date, report_type, total_hldr_eqy_exc_min_int
                )
                values ($1, $2, $3, $4)
                on conflict (ts_code, end_date, report_type) do update set
                    total_hldr_eqy_exc_min_int = coalesce(
                        excluded.total_hldr_eqy_exc_min_int,
                        deep_value.tushare_balancesheet.total_hldr_eqy_exc_min_int
                    ),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(end_date)
            .bind(&report_type)
            .bind(total_eqy_idx.and_then(|idx| cell_f64(row, idx)))
            .execute(&self.pool)
            .await
            .context("写入 tushare_balancesheet 失败")?;
        }

        Ok(())
    }

    async fn save_fina_audit(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let audit_agency_idx = field_index(fields, "audit_agency");
        let Some(period) = params.get("period") else {
            return Ok(());
        };

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let audit_agency = audit_agency_idx.and_then(|idx| cell_string(row, idx));

            sqlx::query(
                r#"
                insert into deep_value.tushare_fina_audit (ts_code, period, audit_agency)
                values ($1, $2, $3)
                on conflict (ts_code, period) do update set
                    audit_agency = coalesce(
                        excluded.audit_agency,
                        deep_value.tushare_fina_audit.audit_agency
                    ),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(period)
            .bind(audit_agency)
            .execute(&self.pool)
            .await
            .context("写入 tushare_fina_audit 失败")?;
        }

        Ok(())
    }

    async fn load_income(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(period) = params.get("period") else {
            return Ok(None);
        };
        let report_type = params.get("report_type").map(String::as_str).unwrap_or("");
        let rows = sqlx::query(
            r#"
            select ts_code, end_date, n_income
            from deep_value.tushare_income
            where end_date = $1 and report_type = $2
            order by ts_code
            "#,
        )
        .bind(period)
        .bind(report_type)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_income 失败")?;

        rows_to_dataframe(
            &requested_fields(fields, &["ts_code", "end_date", "n_income"]),
            &rows,
        )
    }

    async fn load_dividend(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(end_date) = params.get("end_date") else {
            return Ok(None);
        };
        let rows = sqlx::query(
            r#"
            select ts_code, end_date, cash_div_tax, stk_div
            from deep_value.tushare_dividend
            where end_date = $1
            order by ts_code, row_hash
            "#,
        )
        .bind(end_date)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_dividend 失败")?;

        rows_to_dataframe(
            &requested_fields(fields, &["ts_code", "end_date", "cash_div_tax", "stk_div"]),
            &rows,
        )
    }

    async fn load_balancesheet(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(period) = params.get("period") else {
            return Ok(None);
        };
        let report_type = params.get("report_type").map(String::as_str).unwrap_or("");
        let rows = sqlx::query(
            r#"
            select ts_code, end_date, total_hldr_eqy_exc_min_int
            from deep_value.tushare_balancesheet
            where end_date = $1 and report_type = $2
            order by ts_code
            "#,
        )
        .bind(period)
        .bind(report_type)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_balancesheet 失败")?;

        rows_to_dataframe(
            &requested_fields(
                fields,
                &["ts_code", "end_date", "total_hldr_eqy_exc_min_int"],
            ),
            &rows,
        )
    }

    async fn load_fina_audit(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(period) = params.get("period") else {
            return Ok(None);
        };
        let rows = sqlx::query(
            r#"
            select ts_code, audit_agency
            from deep_value.tushare_fina_audit
            where period = $1
            order by ts_code
            "#,
        )
        .bind(period)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_fina_audit 失败")?;

        rows_to_dataframe(
            &requested_fields(fields, &["ts_code", "audit_agency"]),
            &rows,
        )
    }

    async fn save_fina_indicator(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let end_date_idx = field_index(fields, "end_date");

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let end_date = end_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("period").cloned());
            let Some(end_date) = end_date else {
                continue;
            };

            sqlx::query(
                r#"
                insert into deep_value.tushare_fina_indicator (
                    ts_code, end_date, roe, roa, grossprofit_margin,
                    netprofit_margin, debt_to_assets, current_ratio,
                    bps, eps, cfps, or_yoy, profit_dedt
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                on conflict (ts_code, end_date) do update set
                    roe = coalesce(excluded.roe, deep_value.tushare_fina_indicator.roe),
                    roa = coalesce(excluded.roa, deep_value.tushare_fina_indicator.roa),
                    grossprofit_margin = coalesce(excluded.grossprofit_margin, deep_value.tushare_fina_indicator.grossprofit_margin),
                    netprofit_margin = coalesce(excluded.netprofit_margin, deep_value.tushare_fina_indicator.netprofit_margin),
                    debt_to_assets = coalesce(excluded.debt_to_assets, deep_value.tushare_fina_indicator.debt_to_assets),
                    current_ratio = coalesce(excluded.current_ratio, deep_value.tushare_fina_indicator.current_ratio),
                    bps = coalesce(excluded.bps, deep_value.tushare_fina_indicator.bps),
                    eps = coalesce(excluded.eps, deep_value.tushare_fina_indicator.eps),
                    cfps = coalesce(excluded.cfps, deep_value.tushare_fina_indicator.cfps),
                    or_yoy = coalesce(excluded.or_yoy, deep_value.tushare_fina_indicator.or_yoy),
                    profit_dedt = coalesce(excluded.profit_dedt, deep_value.tushare_fina_indicator.profit_dedt),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(end_date)
            .bind(field_f64_opt(fields, row, "roe"))
            .bind(field_f64_opt(fields, row, "roa"))
            .bind(field_f64_opt(fields, row, "grossprofit_margin"))
            .bind(field_f64_opt(fields, row, "netprofit_margin"))
            .bind(field_f64_opt(fields, row, "debt_to_assets"))
            .bind(field_f64_opt(fields, row, "current_ratio"))
            .bind(field_f64_opt(fields, row, "bps"))
            .bind(field_f64_opt(fields, row, "eps"))
            .bind(field_f64_opt(fields, row, "cfps"))
            .bind(field_f64_opt(fields, row, "or_yoy"))
            .bind(field_f64_opt(fields, row, "profit_dedt"))
            .execute(&self.pool)
            .await
            .context("写入 tushare_fina_indicator 失败")?;
        }

        Ok(())
    }

    async fn load_fina_indicator(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(period) = params.get("period") else {
            return Ok(None);
        };
        let rows = sqlx::query(
            r#"
            select ts_code, end_date, roe, roa, grossprofit_margin,
                   netprofit_margin, debt_to_assets, current_ratio,
                   bps, eps, cfps, or_yoy, profit_dedt
            from deep_value.tushare_fina_indicator
            where end_date = $1
            order by ts_code
            "#,
        )
        .bind(period)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_fina_indicator 失败")?;

        rows_to_dataframe(
            &requested_fields(
                fields,
                &[
                    "ts_code", "end_date", "roe", "roa", "grossprofit_margin",
                    "netprofit_margin", "debt_to_assets", "current_ratio",
                    "bps", "eps", "cfps", "or_yoy", "profit_dedt",
                ],
            ),
            &rows,
        )
    }

    async fn save_cashflow(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let end_date_idx = field_index(fields, "end_date");
        let n_cf_idx = field_index(fields, "n_cashflow_act");
        let report_type = params.get("report_type").cloned().unwrap_or_default();

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let end_date = end_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("period").cloned());
            let Some(end_date) = end_date else {
                continue;
            };
            sqlx::query(
                r#"
                insert into deep_value.tushare_cashflow (ts_code, end_date, report_type, n_cashflow_act)
                values ($1, $2, $3, $4)
                on conflict (ts_code, end_date, report_type) do update set
                    n_cashflow_act = coalesce(excluded.n_cashflow_act, deep_value.tushare_cashflow.n_cashflow_act),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(end_date)
            .bind(&report_type)
            .bind(n_cf_idx.and_then(|idx| cell_f64(row, idx)))
            .execute(&self.pool)
            .await
            .context("写入 tushare_cashflow 失败")?;
        }
        Ok(())
    }

    async fn load_cashflow(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(period) = params.get("period") else {
            return Ok(None);
        };
        let report_type = params.get("report_type").map(String::as_str).unwrap_or("");
        let rows = sqlx::query(
            r#"
            select ts_code, end_date, n_cashflow_act
            from deep_value.tushare_cashflow
            where end_date = $1 and report_type = $2
            order by ts_code
            "#,
        )
        .bind(period)
        .bind(report_type)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_cashflow 失败")?;
        rows_to_dataframe(
            &requested_fields(fields, &["ts_code", "end_date", "n_cashflow_act"]),
            &rows,
        )
    }

    async fn save_disclosure_date(
        &self,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let end_date_idx = field_index(fields, "end_date");
        let ann_date_idx = field_index(fields, "ann_date");
        let actual_date_idx = field_index(fields, "actual_date");

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let end_date = end_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("end_date").cloned());
            let Some(end_date) = end_date else {
                continue;
            };
            sqlx::query(
                r#"
                insert into deep_value.tushare_disclosure_date (ts_code, end_date, ann_date, actual_date)
                values ($1, $2, $3, $4)
                on conflict (ts_code, end_date) do update set
                    ann_date = coalesce(excluded.ann_date, deep_value.tushare_disclosure_date.ann_date),
                    actual_date = coalesce(excluded.actual_date, deep_value.tushare_disclosure_date.actual_date),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(end_date)
            .bind(ann_date_idx.and_then(|idx| cell_string(row, idx)))
            .bind(actual_date_idx.and_then(|idx| cell_string(row, idx)))
            .execute(&self.pool)
            .await
            .context("写入 tushare_disclosure_date 失败")?;
        }
        Ok(())
    }

    async fn load_disclosure_date(
        &self,
        params: &HashMap<String, String>,
        fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(end_date) = params.get("end_date") else {
            return Ok(None);
        };
        let rows = sqlx::query(
            r#"
            select ts_code, end_date, ann_date, actual_date
            from deep_value.tushare_disclosure_date
            where end_date = $1
            order by ts_code
            "#,
        )
        .bind(end_date)
        .fetch_all(&self.pool)
        .await
        .context("读取 tushare_disclosure_date 失败")?;
        rows_to_dataframe(
            &requested_fields(fields, &["ts_code", "end_date", "ann_date", "actual_date"]),
            &rows,
        )
    }

    async fn save_market_series(
        &self,
        table: &str,
        value_field: &str,
        params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else {
            return Ok(());
        };
        let trade_date_idx = field_index(fields, "trade_date");
        let value_idx = field_index(fields, value_field);
        let sql = format!(
            r#"
            insert into deep_value.{table} (ts_code, trade_date, {value_field})
            values ($1, $2, $3)
            on conflict (ts_code, trade_date) do update set
                {value_field} = coalesce(excluded.{value_field}, deep_value.{table}.{value_field}),
                updated_at = now()
            "#
        );

        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else {
                continue;
            };
            let trade_date = trade_date_idx
                .and_then(|idx| cell_string(row, idx))
                .or_else(|| params.get("trade_date").cloned());
            let Some(trade_date) = trade_date else {
                continue;
            };

            sqlx::query(&sql)
                .bind(ts_code)
                .bind(trade_date)
                .bind(value_idx.and_then(|idx| cell_f64(row, idx)))
                .execute(&self.pool)
                .await
                .with_context(|| format!("写入 {table} 失败"))?;
        }

        Ok(())
    }

    async fn load_market_series(
        &self,
        table: &str,
        value_field: &str,
        params: &HashMap<String, String>,
        fields: Option<&str>,
        default_fields: &[&str],
    ) -> Result<Option<DataFrame>> {
        let Some(ts_code) = params.get("ts_code") else {
            return Ok(None);
        };
        let Some(start_date) = params.get("start_date") else {
            return Ok(None);
        };
        let Some(end_date) = params.get("end_date") else {
            return Ok(None);
        };
        let sql = format!(
            r#"
            select ts_code, trade_date, {value_field}
            from deep_value.{table}
            where ts_code = $1
              and trade_date >= $2
              and trade_date <= $3
            order by trade_date
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(ts_code)
            .bind(start_date)
            .bind(end_date)
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("读取 {table} 失败"))?;

        rows_to_dataframe(&requested_fields(fields, default_fields), &rows)
    }
}

fn field_index(fields: &[String], name: &str) -> Option<usize> {
    fields.iter().position(|field| field == name)
}

fn cell_string(row: &[Value], idx: usize) -> Option<String> {
    match row.get(idx)? {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        value => Some(value.to_string()),
    }
}

fn field_f64_opt(fields: &[String], row: &[Value], name: &str) -> Option<f64> {
    field_index(fields, name).and_then(|idx| cell_f64(row, idx))
}

fn cell_f64(row: &[Value], idx: usize) -> Option<f64> {
    match row.get(idx)? {
        Value::Null => None,
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse().ok(),
        Value::Bool(_) => None,
        value => value.to_string().parse().ok(),
    }
}

fn requested_fields(fields: Option<&str>, default_fields: &[&str]) -> Vec<String> {
    fields
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|field| !field.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_else(|| {
            default_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect()
        })
}

fn rows_to_dataframe(
    fields: &[String],
    rows: &[sqlx::postgres::PgRow],
) -> Result<Option<DataFrame>> {
    if rows.is_empty() {
        return Ok(None);
    }

    let columns: Result<Vec<Column>> = fields
        .iter()
        .map(|field| {
            let values: Result<Vec<Option<String>>> = rows
                .iter()
                .map(|row| row_value_as_string(row, field))
                .collect();
            Ok(Column::new(
                PlSmallStr::from(field.as_str()),
                values?
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            ))
        })
        .collect();

    Ok(Some(DataFrame::new(columns?)?))
}

fn row_value_as_string(row: &sqlx::postgres::PgRow, field: &str) -> Result<Option<String>> {
    let value = match field {
        "exchange" | "cal_date" | "is_open" | "ts_code" | "trade_date" | "name" | "industry"
        | "list_status" | "list_date" | "end_date" | "audit_agency"
        | "ann_date" | "actual_date" => row.try_get::<Option<String>, _>(field)?,
        "pb"
        | "pe"
        | "pe_ttm"
        | "dv_ratio"
        | "total_mv"
        | "n_income"
        | "cash_div_tax"
        | "stk_div"
        | "total_hldr_eqy_exc_min_int"
        | "close"
        | "adj_factor"
        | "roe"
        | "roa"
        | "grossprofit_margin"
        | "netprofit_margin"
        | "debt_to_assets"
        | "current_ratio"
        | "bps"
        | "eps"
        | "cfps"
        | "or_yoy"
        | "profit_dedt"
        | "n_cashflow_act" => row
            .try_get::<Option<f64>, _>(field)?
            .map(|value| trim_float_string(value)),
        _ => None,
    };

    Ok(value)
}

fn trim_float_string(value: f64) -> String {
    let mut text = value.to_string();
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    text
}

fn stable_row_hash(api_name: &str, fields: &[String], row: &[Value]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in api_name.as_bytes() {
        hash = fnv1a(hash, *byte);
    }
    for field in fields {
        for byte in field.as_bytes() {
            hash = fnv1a(hash, *byte);
        }
        hash = fnv1a(hash, 0xff);
    }
    for value in row {
        let text = match value {
            Value::Null => "null".to_string(),
            Value::String(value) => value.clone(),
            Value::Number(value) => value.to_string(),
            Value::Bool(value) => value.to_string(),
            value => value.to_string(),
        };
        for byte in text.as_bytes() {
            hash = fnv1a(hash, *byte);
        }
        hash = fnv1a(hash, 0xfe);
    }
    format!("{hash:016x}")
}

fn fnv1a(hash: u64, byte: u8) -> u64 {
    (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
}
