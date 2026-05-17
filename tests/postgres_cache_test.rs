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
