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
