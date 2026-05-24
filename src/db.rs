//! PostgreSQL connection helpers.

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Create a PostgreSQL connection pool.
pub async fn connect(database_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .context("连接 PostgreSQL 失败")
}

/// Verify PostgreSQL connectivity with a minimal query.
pub async fn health_check(pool: &PgPool) -> Result<()> {
    sqlx::query("select 1")
        .execute(pool)
        .await
        .context("PostgreSQL 健康检查失败")?;
    Ok(())
}

/// Initialize the PostgreSQL schema used by this application.
pub async fn init_schema(pool: &PgPool) -> Result<()> {
    const STATEMENTS: &[&str] = &[
        r#"create schema if not exists deep_value"#,
        r#"
        create table if not exists deep_value.tushare_raw_responses (
            cache_key text primary key,
            api_name text not null,
            params jsonb not null,
            requested_fields text,
            response_fields text[] not null,
            response_items jsonb not null,
            row_count integer not null,
            fetched_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_raw_responses_api_name
            on deep_value.tushare_raw_responses(api_name)
        "#,
        r#"
        create index if not exists idx_tushare_raw_responses_updated_at
            on deep_value.tushare_raw_responses(updated_at)
        "#,
        r#"
        create table if not exists deep_value.tushare_sync_jobs (
            job_key text primary key,
            api_name text not null,
            params jsonb not null,
            requested_fields text,
            status text not null default 'pending',
            row_count integer,
            attempts integer not null default 0,
            last_error text,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now(),
            finished_at timestamptz,
            check (status in ('pending', 'running', 'done', 'failed'))
        )
        "#,
        r#"
        create index if not exists idx_tushare_sync_jobs_status
            on deep_value.tushare_sync_jobs(status)
        "#,
        r#"
        create index if not exists idx_tushare_sync_jobs_api_name
            on deep_value.tushare_sync_jobs(api_name)
        "#,
        r#"
        create table if not exists deep_value.tushare_trade_cal (
            exchange text not null,
            cal_date text not null,
            is_open text,
            pretrade_date text,
            updated_at timestamptz not null default now(),
            primary key (exchange, cal_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_trade_cal_cal_date
            on deep_value.tushare_trade_cal(cal_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_stock_basic (
            ts_code text primary key,
            symbol text,
            name text,
            area text,
            industry text,
            fullname text,
            enname text,
            cnspell text,
            market text,
            exchange text,
            curr_type text,
            list_status text,
            list_date text,
            delist_date text,
            is_hs text,
            act_name text,
            act_ent_type text,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_stock_basic_list_status
            on deep_value.tushare_stock_basic(list_status)
        "#,
        r#"
        create table if not exists deep_value.tushare_daily_basic (
            ts_code text not null,
            trade_date text not null,
            pb double precision,
            pe double precision,
            pe_ttm double precision,
            dv_ratio double precision,
            dv_ttm double precision,
            total_mv double precision,
            close double precision,
            turnover_rate double precision,
            turnover_rate_f double precision,
            volume_ratio double precision,
            ps double precision,
            ps_ttm double precision,
            total_share double precision,
            float_share double precision,
            free_share double precision,
            circ_mv double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, trade_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_daily_basic_trade_date
            on deep_value.tushare_daily_basic(trade_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_income (
            ts_code text not null,
            end_date text not null,
            report_type text not null,
            n_income double precision,
            total_revenue double precision,
            revenue double precision,
            oper_cost double precision,
            sell_exp double precision,
            admin_exp double precision,
            fin_exp double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, end_date, report_type)
        )
        "#,
        r#"
        create index if not exists idx_tushare_income_end_date
            on deep_value.tushare_income(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_dividend (
            row_hash text primary key,
            ts_code text not null,
            end_date text,
            cash_div_tax double precision,
            stk_div double precision,
            record_date text,
            ex_date text,
            ann_date text,
            div_proc text,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_dividend_end_date
            on deep_value.tushare_dividend(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_balancesheet (
            ts_code text not null,
            end_date text not null,
            report_type text not null,
            total_hldr_eqy_exc_min_int double precision,
            total_assets double precision,
            total_cur_assets double precision,
            total_cur_liab double precision,
            total_liab double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, end_date, report_type)
        )
        "#,
        r#"
        create index if not exists idx_tushare_balancesheet_end_date
            on deep_value.tushare_balancesheet(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_fina_audit (
            ts_code text not null,
            period text not null,
            audit_agency text,
            ann_date text,
            end_date text,
            audit_result text,
            audit_fees double precision,
            audit_sign text,
            updated_at timestamptz not null default now(),
            primary key (ts_code, period)
        )
        "#,
        r#"
        create index if not exists idx_tushare_fina_audit_period
            on deep_value.tushare_fina_audit(period)
        "#,
        r#"
        create table if not exists deep_value.tushare_daily (
            ts_code text not null,
            trade_date text not null,
            close double precision,
            open double precision,
            high double precision,
            low double precision,
            pre_close double precision,
            pct_chg double precision,
            change double precision,
            vol double precision,
            amount double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, trade_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_daily_trade_date
            on deep_value.tushare_daily(trade_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_adj_factor (
            ts_code text not null,
            trade_date text not null,
            adj_factor double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, trade_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_adj_factor_trade_date
            on deep_value.tushare_adj_factor(trade_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_index_daily (
            ts_code text not null,
            trade_date text not null,
            close double precision,
            open double precision,
            high double precision,
            low double precision,
            pre_close double precision,
            pct_chg double precision,
            change double precision,
            vol double precision,
            amount double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, trade_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_index_daily_trade_date
            on deep_value.tushare_index_daily(trade_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_fina_indicator (
            ts_code text not null,
            end_date text not null,
            roe double precision,
            roa double precision,
            grossprofit_margin double precision,
            netprofit_margin double precision,
            debt_to_assets double precision,
            current_ratio double precision,
            bps double precision,
            eps double precision,
            cfps double precision,
            or_yoy double precision,
            profit_dedt double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, end_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_fina_indicator_end_date
            on deep_value.tushare_fina_indicator(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_cashflow (
            ts_code text not null,
            end_date text not null,
            report_type text not null,
            n_cashflow_act double precision,
            n_cashflow_inv_act double precision,
            n_cash_flows_fnc_act double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, end_date, report_type)
        )
        "#,
        r#"
        create index if not exists idx_tushare_cashflow_end_date
            on deep_value.tushare_cashflow(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_disclosure_date (
            ts_code text not null,
            end_date text not null,
            ann_date text,
            actual_date text,
            pre_date text,
            modify_date text,
            updated_at timestamptz not null default now(),
            primary key (ts_code, end_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_disclosure_date_end_date
            on deep_value.tushare_disclosure_date(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_suspend_d (
            ts_code text not null,
            trade_date text not null,
            suspend_type text,
            suspend_timing text,
            updated_at timestamptz not null default now(),
            primary key (ts_code, trade_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_suspend_d_trade_date
            on deep_value.tushare_suspend_d(trade_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_stk_limit (
            ts_code text not null,
            trade_date text not null,
            pre_close double precision,
            up_limit double precision,
            down_limit double precision,
            updated_at timestamptz not null default now(),
            primary key (ts_code, trade_date)
        )
        "#,
        r#"
        create index if not exists idx_tushare_stk_limit_trade_date
            on deep_value.tushare_stk_limit(trade_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_forecast (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_forecast_end_date
            on deep_value.tushare_forecast(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_express (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_express_end_date
            on deep_value.tushare_express(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_fina_mainbz (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_fina_mainbz_end_date
            on deep_value.tushare_fina_mainbz(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_index_weight (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_index_weight_trade_date
            on deep_value.tushare_index_weight(trade_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_top10_holders (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_top10_holders_end_date
            on deep_value.tushare_top10_holders(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_top10_floatholders (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_top10_floatholders_end_date
            on deep_value.tushare_top10_floatholders(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_pledge_stat (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_pledge_stat_end_date
            on deep_value.tushare_pledge_stat(end_date)
        "#,
        r#"
        create table if not exists deep_value.tushare_repurchase (
            row_hash text primary key,
            ts_code text,
            end_date text,
            ann_date text,
            trade_date text,
            payload jsonb not null,
            updated_at timestamptz not null default now()
        )
        "#,
        r#"
        create index if not exists idx_tushare_repurchase_ann_date
            on deep_value.tushare_repurchase(ann_date)
        "#,
    ];

    let mut tx = pool.begin().await.context("启动 schema 初始化事务失败")?;

    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(7_563_009_001_i64)
        .execute(&mut *tx)
        .await
        .context("获取 schema 初始化锁失败")?;

    for statement in STATEMENTS {
        sqlx::query(statement)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("执行数据库 schema 初始化失败: {statement}"))?;
    }

    tx.commit().await.context("提交 schema 初始化事务失败")?;

    Ok(())
}
