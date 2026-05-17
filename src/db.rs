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
