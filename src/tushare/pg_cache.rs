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

    /// Return all distinct trade_dates present in daily typed table.
    pub async fn existing_daily_dates(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct trade_date from deep_value.tushare_daily order by trade_date"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 daily 已有日期失败")?;
        Ok(rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("trade_date").ok())
            .collect())
    }

    /// Return all distinct trade_dates present in adj_factor typed table.
    pub async fn existing_adj_factor_dates(&self) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            r#"select distinct trade_date from deep_value.tushare_adj_factor order by trade_date"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("查询 adj_factor 已有日期失败")?;
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
                self.save_daily_ohlc(params, fields, items).await
            }
            "adj_factor" => {
                self.save_market_series("tushare_adj_factor", "adj_factor", params, fields, items)
                    .await
            }
            "index_daily" => {
                self.save_index_daily_ohlc(params, fields, items).await
            }
            "fina_indicator" | "fina_indicator_vip" => {
                self.save_fina_indicator(params, fields, items).await
            }
            "cashflow" | "cashflow_vip" => self.save_cashflow(params, fields, items).await,
            "disclosure_date" => self.save_disclosure_date(params, fields, items).await,
            "suspend_d" => self.save_suspend_d(params, fields, items).await,
            "stk_limit" => self.save_stk_limit(params, fields, items).await,
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
            "daily" => self.load_daily_ohlc(params, fields).await,
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
            "index_daily" => self.load_index_daily_ohlc(params, fields).await,
            "fina_indicator" | "fina_indicator_vip" => {
                self.load_fina_indicator(params, fields).await
            }
            "cashflow" | "cashflow_vip" => self.load_cashflow(params, fields).await,
            "disclosure_date" => self.load_disclosure_date(params, fields).await,
            "suspend_d" => self.load_suspend_d(params, fields).await,
            "stk_limit" => self.load_stk_limit(params, fields).await,
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
        _params: &HashMap<String, String>,
        fields: &[String],
        items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else { return Ok(()); };
        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else { continue; };
            sqlx::query(r#"
                insert into deep_value.tushare_stock_basic (ts_code, name, industry, list_status, list_date, symbol, area, market, is_hs)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                on conflict (ts_code) do update set
                    name = coalesce(excluded.name, deep_value.tushare_stock_basic.name),
                    industry = coalesce(excluded.industry, deep_value.tushare_stock_basic.industry),
                    list_status = coalesce(excluded.list_status, deep_value.tushare_stock_basic.list_status),
                    list_date = coalesce(excluded.list_date, deep_value.tushare_stock_basic.list_date),
                    symbol = coalesce(excluded.symbol, deep_value.tushare_stock_basic.symbol),
                    area = coalesce(excluded.area, deep_value.tushare_stock_basic.area),
                    market = coalesce(excluded.market, deep_value.tushare_stock_basic.market),
                    is_hs = coalesce(excluded.is_hs, deep_value.tushare_stock_basic.is_hs),
                    updated_at = now()
            "#)
            .bind(ts_code)
            .bind(field_str_opt(fields, row, "name"))
            .bind(field_str_opt(fields, row, "industry"))
            .bind(field_str_opt(fields, row, "list_status"))
            .bind(field_str_opt(fields, row, "list_date"))
            .bind(field_str_opt(fields, row, "symbol"))
            .bind(field_str_opt(fields, row, "area"))
            .bind(field_str_opt(fields, row, "market"))
            .bind(field_str_opt(fields, row, "is_hs"))
            .execute(&self.pool).await.context("写入 tushare_stock_basic 失败")?;
        }
        Ok(())
    }

    async fn save_daily_basic(
        &self, params: &HashMap<String, String>, fields: &[String], items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else { return Ok(()); };
        let td = field_index(fields, "trade_date");
        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else { continue; };
            let Some(trade_date) = td.and_then(|i| cell_string(row, i)).or_else(|| params.get("trade_date").cloned()) else { continue; };
            sqlx::query(r#"
                insert into deep_value.tushare_daily_basic (ts_code, trade_date, pb, pe, pe_ttm, dv_ratio, dv_ttm, total_mv, close, turnover_rate, turnover_rate_f, volume_ratio, ps, ps_ttm, total_share, float_share, free_share, circ_mv)
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
                on conflict (ts_code, trade_date) do update set
                    pb = coalesce(excluded.pb, deep_value.tushare_daily_basic.pb),
                    pe = coalesce(excluded.pe, deep_value.tushare_daily_basic.pe),
                    pe_ttm = coalesce(excluded.pe_ttm, deep_value.tushare_daily_basic.pe_ttm),
                    dv_ratio = coalesce(excluded.dv_ratio, deep_value.tushare_daily_basic.dv_ratio),
                    dv_ttm = coalesce(excluded.dv_ttm, deep_value.tushare_daily_basic.dv_ttm),
                    total_mv = coalesce(excluded.total_mv, deep_value.tushare_daily_basic.total_mv),
                    close = coalesce(excluded.close, deep_value.tushare_daily_basic.close),
                    turnover_rate = coalesce(excluded.turnover_rate, deep_value.tushare_daily_basic.turnover_rate),
                    turnover_rate_f = coalesce(excluded.turnover_rate_f, deep_value.tushare_daily_basic.turnover_rate_f),
                    volume_ratio = coalesce(excluded.volume_ratio, deep_value.tushare_daily_basic.volume_ratio),
                    ps = coalesce(excluded.ps, deep_value.tushare_daily_basic.ps),
                    ps_ttm = coalesce(excluded.ps_ttm, deep_value.tushare_daily_basic.ps_ttm),
                    total_share = coalesce(excluded.total_share, deep_value.tushare_daily_basic.total_share),
                    float_share = coalesce(excluded.float_share, deep_value.tushare_daily_basic.float_share),
                    free_share = coalesce(excluded.free_share, deep_value.tushare_daily_basic.free_share),
                    circ_mv = coalesce(excluded.circ_mv, deep_value.tushare_daily_basic.circ_mv),
                    updated_at = now()
            "#).bind(ts_code).bind(trade_date)
            .bind(field_f64_opt(fields, row, "pb")).bind(field_f64_opt(fields, row, "pe"))
            .bind(field_f64_opt(fields, row, "pe_ttm")).bind(field_f64_opt(fields, row, "dv_ratio"))
            .bind(field_f64_opt(fields, row, "dv_ttm")).bind(field_f64_opt(fields, row, "total_mv"))
            .bind(field_f64_opt(fields, row, "close"))
            .bind(field_f64_opt(fields, row, "turnover_rate")).bind(field_f64_opt(fields, row, "turnover_rate_f"))
            .bind(field_f64_opt(fields, row, "volume_ratio")).bind(field_f64_opt(fields, row, "ps"))
            .bind(field_f64_opt(fields, row, "ps_ttm")).bind(field_f64_opt(fields, row, "total_share"))
            .bind(field_f64_opt(fields, row, "float_share")).bind(field_f64_opt(fields, row, "free_share"))
            .bind(field_f64_opt(fields, row, "circ_mv"))
            .execute(&self.pool).await.context("写入 tushare_daily_basic 失败")?;
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
            select ts_code, symbol, name, area, industry, market, list_status, list_date, is_hs
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
            &requested_fields(fields, &["ts_code", "symbol", "name", "area", "industry", "market", "list_status", "list_date", "is_hs"]),
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
            select ts_code, trade_date, pb, pe, pe_ttm, dv_ratio, dv_ttm, total_mv, close, turnover_rate, turnover_rate_f, volume_ratio, ps, ps_ttm, total_share, float_share, free_share, circ_mv
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
            &requested_fields(fields, &[
                "ts_code", "trade_date", "pb", "pe", "pe_ttm", "dv_ratio", "total_mv",
                "close", "turnover_rate", "turnover_rate_f", "volume_ratio",
                "ps", "ps_ttm", "total_share", "float_share", "free_share", "circ_mv", "dv_ttm",
            ]),
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
        let _n_income_idx = field_index(fields, "n_income");
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

            sqlx::query(r#"
                insert into deep_value.tushare_income (ts_code, end_date, report_type, n_income, total_revenue, revenue, oper_cost, sell_exp, admin_exp, fin_exp)
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                on conflict (ts_code, end_date, report_type) do update set
                    n_income = coalesce(excluded.n_income, deep_value.tushare_income.n_income),
                    total_revenue = coalesce(excluded.total_revenue, deep_value.tushare_income.total_revenue),
                    revenue = coalesce(excluded.revenue, deep_value.tushare_income.revenue),
                    oper_cost = coalesce(excluded.oper_cost, deep_value.tushare_income.oper_cost),
                    sell_exp = coalesce(excluded.sell_exp, deep_value.tushare_income.sell_exp),
                    admin_exp = coalesce(excluded.admin_exp, deep_value.tushare_income.admin_exp),
                    fin_exp = coalesce(excluded.fin_exp, deep_value.tushare_income.fin_exp),
                    updated_at = now()
            "#)
            .bind(ts_code).bind(end_date).bind(&report_type)
            .bind(field_f64_opt(fields, row, "n_income"))
            .bind(field_f64_opt(fields, row, "total_revenue"))
            .bind(field_f64_opt(fields, row, "revenue"))
            .bind(field_f64_opt(fields, row, "oper_cost"))
            .bind(field_f64_opt(fields, row, "sell_exp"))
            .bind(field_f64_opt(fields, row, "admin_exp"))
            .bind(field_f64_opt(fields, row, "fin_exp"))
            .execute(&self.pool).await.context("写入 tushare_income 失败")?;
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

            sqlx::query(r#"
                insert into deep_value.tushare_dividend (row_hash, ts_code, end_date, cash_div_tax, stk_div, record_date, ex_date, ann_date, div_proc)
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                on conflict (row_hash) do update set
                    ts_code = excluded.ts_code, end_date = excluded.end_date,
                    cash_div_tax = excluded.cash_div_tax, stk_div = excluded.stk_div,
                    record_date = coalesce(excluded.record_date, deep_value.tushare_dividend.record_date),
                    ex_date = coalesce(excluded.ex_date, deep_value.tushare_dividend.ex_date),
                    ann_date = coalesce(excluded.ann_date, deep_value.tushare_dividend.ann_date),
                    div_proc = coalesce(excluded.div_proc, deep_value.tushare_dividend.div_proc),
                    updated_at = now()
            "#)
            .bind(row_hash).bind(ts_code).bind(end_date).bind(cash_div_tax).bind(stk_div)
            .bind(field_str_opt(fields, row, "record_date"))
            .bind(field_str_opt(fields, row, "ex_date"))
            .bind(field_str_opt(fields, row, "ann_date"))
            .bind(field_str_opt(fields, row, "div_proc"))
            .execute(&self.pool).await.context("写入 tushare_dividend 失败")?;
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
        let _total_eqy_idx = field_index(fields, "total_hldr_eqy_exc_min_int");
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
                    ts_code, end_date, report_type, total_hldr_eqy_exc_min_int, total_assets, total_cur_assets, total_cur_liab, total_liab
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8)
                on conflict (ts_code, end_date, report_type) do update set
                    total_hldr_eqy_exc_min_int = coalesce(excluded.total_hldr_eqy_exc_min_int, deep_value.tushare_balancesheet.total_hldr_eqy_exc_min_int),
                    total_assets = coalesce(excluded.total_assets, deep_value.tushare_balancesheet.total_assets),
                    total_cur_assets = coalesce(excluded.total_cur_assets, deep_value.tushare_balancesheet.total_cur_assets),
                    total_cur_liab = coalesce(excluded.total_cur_liab, deep_value.tushare_balancesheet.total_cur_liab),
                    total_liab = coalesce(excluded.total_liab, deep_value.tushare_balancesheet.total_liab),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(end_date)
            .bind(&report_type)
            .bind(field_f64_opt(fields, row, "total_hldr_eqy_exc_min_int"))
            .bind(field_f64_opt(fields, row, "total_assets"))
            .bind(field_f64_opt(fields, row, "total_cur_assets"))
            .bind(field_f64_opt(fields, row, "total_cur_liab"))
            .bind(field_f64_opt(fields, row, "total_liab"))
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
            let _audit_agency = audit_agency_idx.and_then(|idx| cell_string(row, idx));

            sqlx::query(
                r#"
                insert into deep_value.tushare_fina_audit (ts_code, period, audit_agency, ann_date, end_date, audit_result, audit_fees, audit_sign)
                values ($1, $2, $3, $4, $5, $6, $7, $8)
                on conflict (ts_code, period) do update set
                    audit_agency = coalesce(excluded.audit_agency, deep_value.tushare_fina_audit.audit_agency),
                    ann_date = coalesce(excluded.ann_date, deep_value.tushare_fina_audit.ann_date),
                    end_date = coalesce(excluded.end_date, deep_value.tushare_fina_audit.end_date),
                    audit_result = coalesce(excluded.audit_result, deep_value.tushare_fina_audit.audit_result),
                    audit_fees = coalesce(excluded.audit_fees, deep_value.tushare_fina_audit.audit_fees),
                    audit_sign = coalesce(excluded.audit_sign, deep_value.tushare_fina_audit.audit_sign),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(period)
            .bind(field_str_opt(fields, row, "audit_agency"))
            .bind(field_str_opt(fields, row, "ann_date"))
            .bind(field_str_opt(fields, row, "end_date"))
            .bind(field_str_opt(fields, row, "audit_result"))
            .bind(field_f64_opt(fields, row, "audit_fees"))
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
            select ts_code, end_date, n_income, total_revenue, revenue, oper_cost, sell_exp, admin_exp, fin_exp
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
            &requested_fields(fields, &["ts_code", "end_date", "n_income", "total_revenue", "revenue", "oper_cost", "sell_exp", "admin_exp", "fin_exp"]),
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
            select ts_code, end_date, cash_div_tax, stk_div, record_date, ex_date, ann_date, div_proc
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
            &requested_fields(fields, &["ts_code", "end_date", "cash_div_tax", "stk_div", "record_date", "ex_date", "ann_date", "div_proc"]),
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
            select ts_code, end_date, total_hldr_eqy_exc_min_int, total_assets, total_cur_assets, total_cur_liab, total_liab
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
            &requested_fields(fields, &["ts_code", "end_date", "total_hldr_eqy_exc_min_int", "total_assets", "total_cur_assets", "total_cur_liab", "total_liab"]),
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
            select ts_code, audit_agency, ann_date, end_date, audit_result, audit_fees, audit_sign
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
            &requested_fields(fields, &["ts_code", "audit_agency", "ann_date", "end_date", "audit_result", "audit_fees", "audit_sign"]),
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
        let _n_cf_idx = field_index(fields, "n_cashflow_act");
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
                insert into deep_value.tushare_cashflow (ts_code, end_date, report_type, n_cashflow_act, n_cashflow_inv_act, n_cash_flows_fnc_act)
                values ($1,$2,$3,$4,$5,$6)
                on conflict (ts_code, end_date, report_type) do update set
                    n_cashflow_act = coalesce(excluded.n_cashflow_act, deep_value.tushare_cashflow.n_cashflow_act),
                    n_cashflow_inv_act = coalesce(excluded.n_cashflow_inv_act, deep_value.tushare_cashflow.n_cashflow_inv_act),
                    n_cash_flows_fnc_act = coalesce(excluded.n_cash_flows_fnc_act, deep_value.tushare_cashflow.n_cash_flows_fnc_act),
                    updated_at = now()
                "#,
            )
            .bind(ts_code)
            .bind(end_date)
            .bind(&report_type)
            .bind(field_f64_opt(fields, row, "n_cashflow_act"))
            .bind(field_f64_opt(fields, row, "n_cashflow_inv_act"))
            .bind(field_f64_opt(fields, row, "n_cash_flows_fnc_act"))
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
            select ts_code, end_date, n_cashflow_act, n_cashflow_inv_act, n_cash_flows_fnc_act
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
            &requested_fields(fields, &["ts_code", "end_date", "n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act"]),
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
                insert into deep_value.tushare_disclosure_date (ts_code, end_date, ann_date, actual_date, pre_date, modify_date)
                values ($1, $2, $3, $4, $5, $6)
                on conflict (ts_code, end_date) do update set
                    ann_date = coalesce(excluded.ann_date, deep_value.tushare_disclosure_date.ann_date),
                    actual_date = coalesce(excluded.actual_date, deep_value.tushare_disclosure_date.actual_date),
                    pre_date = coalesce(excluded.pre_date, deep_value.tushare_disclosure_date.pre_date),
                    modify_date = coalesce(excluded.modify_date, deep_value.tushare_disclosure_date.modify_date),
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
            select ts_code, end_date, ann_date, actual_date, pre_date, modify_date
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
            &requested_fields(fields, &["ts_code", "end_date", "ann_date", "actual_date", "pre_date", "modify_date"]),
            &rows,
        )
    }

    async fn save_daily_ohlc(
        &self, params: &HashMap<String, String>, fields: &[String], items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else { return Ok(()); };
        let td = field_index(fields, "trade_date");
        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else { continue; };
            let Some(trade_date) = td.and_then(|i| cell_string(row, i)).or_else(|| params.get("trade_date").cloned()) else { continue; };
            sqlx::query(r#"
                insert into deep_value.tushare_daily (ts_code, trade_date, close, open, high, low, pre_close, pct_chg, change, vol, amount)
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                on conflict (ts_code, trade_date) do update set
                    close = coalesce(excluded.close, deep_value.tushare_daily.close),
                    open = coalesce(excluded.open, deep_value.tushare_daily.open),
                    high = coalesce(excluded.high, deep_value.tushare_daily.high),
                    low = coalesce(excluded.low, deep_value.tushare_daily.low),
                    pre_close = coalesce(excluded.pre_close, deep_value.tushare_daily.pre_close),
                    pct_chg = coalesce(excluded.pct_chg, deep_value.tushare_daily.pct_chg),
                    change = coalesce(excluded.change, deep_value.tushare_daily.change),
                    vol = coalesce(excluded.vol, deep_value.tushare_daily.vol),
                    amount = coalesce(excluded.amount, deep_value.tushare_daily.amount),
                    updated_at = now()
            "#)
            .bind(ts_code).bind(trade_date)
            .bind(field_f64_opt(fields, row, "close"))
            .bind(field_f64_opt(fields, row, "open"))
            .bind(field_f64_opt(fields, row, "high"))
            .bind(field_f64_opt(fields, row, "low"))
            .bind(field_f64_opt(fields, row, "pre_close"))
            .bind(field_f64_opt(fields, row, "pct_chg"))
            .bind(field_f64_opt(fields, row, "change"))
            .bind(field_f64_opt(fields, row, "vol"))
            .bind(field_f64_opt(fields, row, "amount"))
            .execute(&self.pool).await.context("写入 tushare_daily 失败")?;
        }
        Ok(())
    }

    async fn save_index_daily_ohlc(
        &self, params: &HashMap<String, String>, fields: &[String], items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else { return Ok(()); };
        let td = field_index(fields, "trade_date");
        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else { continue; };
            let Some(trade_date) = td.and_then(|i| cell_string(row, i)).or_else(|| params.get("trade_date").cloned()) else { continue; };
            sqlx::query(r#"
                insert into deep_value.tushare_index_daily (ts_code, trade_date, close, open, high, low, pre_close, pct_chg, change, vol, amount)
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                on conflict (ts_code, trade_date) do update set
                    close = coalesce(excluded.close, deep_value.tushare_index_daily.close),
                    open = coalesce(excluded.open, deep_value.tushare_index_daily.open),
                    high = coalesce(excluded.high, deep_value.tushare_index_daily.high),
                    low = coalesce(excluded.low, deep_value.tushare_index_daily.low),
                    pre_close = coalesce(excluded.pre_close, deep_value.tushare_index_daily.pre_close),
                    pct_chg = coalesce(excluded.pct_chg, deep_value.tushare_index_daily.pct_chg),
                    change = coalesce(excluded.change, deep_value.tushare_index_daily.change),
                    vol = coalesce(excluded.vol, deep_value.tushare_index_daily.vol),
                    amount = coalesce(excluded.amount, deep_value.tushare_index_daily.amount),
                    updated_at = now()
            "#)
            .bind(ts_code).bind(trade_date)
            .bind(field_f64_opt(fields, row, "close"))
            .bind(field_f64_opt(fields, row, "open"))
            .bind(field_f64_opt(fields, row, "high"))
            .bind(field_f64_opt(fields, row, "low"))
            .bind(field_f64_opt(fields, row, "pre_close"))
            .bind(field_f64_opt(fields, row, "pct_chg"))
            .bind(field_f64_opt(fields, row, "change"))
            .bind(field_f64_opt(fields, row, "vol"))
            .bind(field_f64_opt(fields, row, "amount"))
            .execute(&self.pool).await.context("写入 tushare_index_daily 失败")?;
        }
        Ok(())
    }

    async fn load_daily_ohlc(
        &self, params: &HashMap<String, String>, fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(ts_code) = params.get("ts_code") else { return Ok(None); };
        let Some(start_date) = params.get("start_date") else { return Ok(None); };
        let Some(end_date) = params.get("end_date") else { return Ok(None); };
        let rows = sqlx::query(r#"
            select ts_code, trade_date, open, high, low, close, pre_close, pct_chg, change, vol, amount
            from deep_value.tushare_daily
            where ts_code = $1 and trade_date >= $2 and trade_date <= $3
            order by trade_date
        "#).bind(ts_code).bind(start_date).bind(end_date)
            .fetch_all(&self.pool).await.context("读取 tushare_daily 失败")?;
        rows_to_dataframe(&requested_fields(fields, &[
            "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "change", "vol", "amount",
        ]), &rows)
    }

    async fn load_index_daily_ohlc(
        &self, params: &HashMap<String, String>, fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(ts_code) = params.get("ts_code") else { return Ok(None); };
        let Some(start_date) = params.get("start_date") else { return Ok(None); };
        let Some(end_date) = params.get("end_date") else { return Ok(None); };
        let rows = sqlx::query(r#"
            select ts_code, trade_date, open, high, low, close, pre_close, pct_chg, change, vol, amount
            from deep_value.tushare_index_daily
            where ts_code = $1 and trade_date >= $2 and trade_date <= $3
            order by trade_date
        "#).bind(ts_code).bind(start_date).bind(end_date)
            .fetch_all(&self.pool).await.context("读取 tushare_index_daily 失败")?;
        rows_to_dataframe(&requested_fields(fields, &[
            "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "change", "vol", "amount",
        ]), &rows)
    }

    async fn save_suspend_d(
        &self, params: &HashMap<String, String>, fields: &[String], items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else { return Ok(()); };
        let td = field_index(fields, "trade_date");
        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else { continue; };
            let Some(trade_date) = td.and_then(|i| cell_string(row, i)).or_else(|| params.get("trade_date").cloned()) else { continue; };
            sqlx::query(r#"
                insert into deep_value.tushare_suspend_d (ts_code, trade_date, suspend_type, suspend_timing)
                values ($1,$2,$3,$4)
                on conflict (ts_code, trade_date) do update set
                    suspend_type = coalesce(excluded.suspend_type, deep_value.tushare_suspend_d.suspend_type),
                    suspend_timing = coalesce(excluded.suspend_timing, deep_value.tushare_suspend_d.suspend_timing),
                    updated_at = now()
            "#).bind(ts_code).bind(trade_date)
            .bind(field_str_opt(fields, row, "suspend_type")).bind(field_str_opt(fields, row, "suspend_timing"))
            .execute(&self.pool).await.context("写入 tushare_suspend_d 失败")?;
        }
        Ok(())
    }

    async fn load_suspend_d(
        &self, params: &HashMap<String, String>, fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(trade_date) = params.get("trade_date") else { return Ok(None); };
        let rows = sqlx::query(r#"
            select ts_code, trade_date, suspend_type, suspend_timing
            from deep_value.tushare_suspend_d where trade_date = $1 order by ts_code
        "#).bind(trade_date).fetch_all(&self.pool).await.context("读取 tushare_suspend_d 失败")?;
        rows_to_dataframe(&requested_fields(fields, &["ts_code", "trade_date", "suspend_type", "suspend_timing"]), &rows)
    }

    async fn save_stk_limit(
        &self, params: &HashMap<String, String>, fields: &[String], items: &[Vec<Value>],
    ) -> Result<()> {
        let Some(ts_code_idx) = field_index(fields, "ts_code") else { return Ok(()); };
        let td = field_index(fields, "trade_date");
        for row in items {
            let Some(ts_code) = cell_string(row, ts_code_idx) else { continue; };
            let Some(trade_date) = td.and_then(|i| cell_string(row, i)).or_else(|| params.get("trade_date").cloned()) else { continue; };
            sqlx::query(r#"
                insert into deep_value.tushare_stk_limit (ts_code, trade_date, up_limit, down_limit)
                values ($1,$2,$3,$4)
                on conflict (ts_code, trade_date) do update set
                    up_limit = coalesce(excluded.up_limit, deep_value.tushare_stk_limit.up_limit),
                    down_limit = coalesce(excluded.down_limit, deep_value.tushare_stk_limit.down_limit),
                    updated_at = now()
            "#).bind(ts_code).bind(trade_date)
            .bind(field_f64_opt(fields, row, "up_limit")).bind(field_f64_opt(fields, row, "down_limit"))
            .execute(&self.pool).await.context("写入 tushare_stk_limit 失败")?;
        }
        Ok(())
    }

    async fn load_stk_limit(
        &self, params: &HashMap<String, String>, fields: Option<&str>,
    ) -> Result<Option<DataFrame>> {
        let Some(trade_date) = params.get("trade_date") else { return Ok(None); };
        let rows = sqlx::query(r#"
            select ts_code, trade_date, up_limit, down_limit
            from deep_value.tushare_stk_limit where trade_date = $1 order by ts_code
        "#).bind(trade_date).fetch_all(&self.pool).await.context("读取 tushare_stk_limit 失败")?;
        rows_to_dataframe(&requested_fields(fields, &["ts_code", "trade_date", "up_limit", "down_limit"]), &rows)
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

fn field_str_opt(fields: &[String], row: &[Value], name: &str) -> Option<String> {
    field_index(fields, name).and_then(|idx| cell_string(row, idx))
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
        | "ann_date" | "actual_date" | "symbol" | "area" | "market" | "is_hs"
        | "record_date" | "ex_date" | "div_proc" | "audit_result" | "audit_sign"
        | "suspend_type" | "suspend_timing" | "pre_date" | "modify_date" => row.try_get::<Option<String>, _>(field)?,
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
        | "n_cashflow_act" | "n_cashflow_inv_act" | "n_cash_flows_fnc_act"
        | "open" | "high" | "low" | "pre_close" | "pct_chg" | "vol" | "amount"
        | "turnover_rate" | "turnover_rate_f" | "volume_ratio" | "ps" | "ps_ttm"
        | "total_share" | "float_share" | "free_share" | "circ_mv"
        | "total_revenue" | "revenue" | "oper_cost" | "sell_exp" | "admin_exp" | "fin_exp"
        | "total_assets" | "total_cur_assets" | "total_cur_liab" | "total_liab"
        | "audit_fees" | "dv_ttm" | "change" | "up_limit" | "down_limit" => row
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
