//! PostgreSQL raw Tushare cache integration tests.

use std::collections::HashMap;

use deep_value::config::AppConfig;
use deep_value::db;
use deep_value::tushare::pg_cache::PgCache;
use serde_json::json;

#[tokio::test]
async fn test_raw_cache_roundtrip() {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");
    db::init_schema(&pool).await.expect("应能初始化 schema");

    let cache = PgCache::new(pool);
    let cache_key = "test_raw_cache_roundtrip";
    let _ = cache.delete_raw(cache_key).await;

    let params = HashMap::from([
        ("exchange".to_string(), "SSE".to_string()),
        ("start_date".to_string(), "20250101".to_string()),
    ]);
    let fields = vec![
        "exchange".to_string(),
        "cal_date".to_string(),
        "is_open".to_string(),
    ];
    let items = vec![
        vec![json!("SSE"), json!("20250101"), json!("0")],
        vec![json!("SSE"), json!("20250102"), json!("1")],
    ];

    cache
        .save_raw(
            cache_key,
            "trade_cal",
            &params,
            Some("exchange,cal_date,is_open"),
            &fields,
            &items,
        )
        .await
        .expect("应能写入 raw cache");

    let loaded = cache
        .load_raw(cache_key)
        .await
        .expect("应能读取 raw cache")
        .expect("raw cache 应存在");

    assert_eq!(loaded.cache_key, cache_key);
    assert_eq!(loaded.api_name, "trade_cal");
    assert_eq!(loaded.params, params);
    assert_eq!(
        loaded.requested_fields.as_deref(),
        Some("exchange,cal_date,is_open")
    );
    assert_eq!(loaded.response_fields, fields);
    assert_eq!(loaded.response_items, items);
    assert_eq!(loaded.row_count, 2);

    let deleted = cache.delete_raw(cache_key).await.expect("应能清理测试数据");
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn test_partial_typed_financial_period_does_not_complete_sync_job() {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");
    db::init_schema(&pool).await.expect("应能初始化 schema");

    let cache = PgCache::new(pool.clone());
    let job_key = "test_partial_income_period_job";
    let params = HashMap::from([
        ("period".to_string(), "20981231".to_string()),
        ("report_type".to_string(), "1".to_string()),
    ]);
    let fields = vec![
        "ts_code".to_string(),
        "end_date".to_string(),
        "n_income".to_string(),
    ];
    let items = vec![vec![json!("TSTCHK.SZ"), json!("20981231"), json!(123.0)]];

    sqlx::query(
        "delete from deep_value.tushare_income where ts_code = 'TSTCHK.SZ' and end_date = '20981231'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("delete from deep_value.tushare_sync_jobs where job_key = $1")
        .bind(job_key)
        .execute(&pool)
        .await
        .unwrap();

    cache
        .save_typed("income_vip", &params, &fields, &items)
        .await
        .unwrap();
    assert!(cache
        .existing_income_periods()
        .await
        .unwrap()
        .contains("20981231"));

    assert!(cache
        .ensure_sync_job(
            job_key,
            "income_vip",
            &params,
            Some("ts_code,end_date,n_income")
        )
        .await
        .unwrap());

    sqlx::query(
        "delete from deep_value.tushare_income where ts_code = 'TSTCHK.SZ' and end_date = '20981231'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("delete from deep_value.tushare_sync_jobs where job_key = $1")
        .bind(job_key)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_schema_initialization_is_idempotent() {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");

    db::init_schema(&pool)
        .await
        .expect("首次 schema 初始化应成功");
    db::init_schema(&pool)
        .await
        .expect("重复 schema 初始化应成功");
}

#[tokio::test]
async fn test_sync_job_checkpoint_roundtrip() {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");
    db::init_schema(&pool).await.expect("应能初始化 schema");

    let cache = PgCache::new(pool.clone());
    let params = HashMap::from([("trade_date".to_string(), "20990105".to_string())]);
    let done_key = "test_sync_job_done";
    let failed_key = "test_sync_job_failed";
    sqlx::query("delete from deep_value.tushare_sync_jobs where job_key = any($1)")
        .bind(vec![done_key, failed_key])
        .execute(&pool)
        .await
        .unwrap();

    assert!(cache
        .ensure_sync_job(done_key, "daily", &params, Some("ts_code,trade_date,close"))
        .await
        .unwrap());
    cache.mark_sync_job_running(done_key).await.unwrap();
    cache.mark_sync_job_done(done_key, 0).await.unwrap();
    assert!(!cache
        .ensure_sync_job(done_key, "daily", &params, Some("ts_code,trade_date,close"))
        .await
        .unwrap());

    assert!(cache
        .ensure_sync_job(
            failed_key,
            "daily",
            &params,
            Some("ts_code,trade_date,close")
        )
        .await
        .unwrap());
    cache.mark_sync_job_running(failed_key).await.unwrap();
    cache
        .mark_sync_job_failed(failed_key, "temporary frequency limit")
        .await
        .unwrap();
    assert!(cache
        .ensure_sync_job(
            failed_key,
            "daily",
            &params,
            Some("ts_code,trade_date,close")
        )
        .await
        .unwrap());

    sqlx::query("delete from deep_value.tushare_sync_jobs where job_key = any($1)")
        .bind(vec![done_key, failed_key])
        .execute(&pool)
        .await
        .unwrap();
}
