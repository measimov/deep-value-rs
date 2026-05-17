//! 端到端集成测试 — 验证 Tushare API 查询 + PostgreSQL raw 缓存完整链路。

use deep_value::config::AppConfig;
use deep_value::db;
use deep_value::tushare::client::TushareClient;
use deep_value::tushare::pg_cache::PgCache;

async fn client_from_env() -> (TushareClient, PgCache) {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");
    db::init_schema(&pool).await.expect("应能初始化 schema");
    let cache = PgCache::new(pool);
    let client = TushareClient::with_pg_cache(&config.tushare_token, cache.clone());
    (client, cache)
}

/// 测试 1: ping 连通性
#[tokio::test]
async fn test_ping() {
    let (client, _) = client_from_env().await;
    let result = client.ping().await;
    assert!(result.is_ok(), "ping 应成功: {:?}", result.err());
    let msg = result.unwrap();
    assert!(msg.contains("连接成功"), "应包含成功提示: {msg}");
}

/// 测试 PostgreSQL 连通性
#[tokio::test]
async fn test_postgres_health_check() {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");
    db::health_check(&pool)
        .await
        .expect("PostgreSQL 健康检查应成功");
}

/// 测试 2: 查询 trade_cal 并验证 DataFrame 结构
#[tokio::test]
async fn test_query_trade_cal() {
    let (client, _) = client_from_env().await;

    let df = client
        .query(
            "trade_cal",
            &[
                ("exchange", "SSE"),
                ("start_date", "20250101"),
                ("end_date", "20250131"),
            ],
            Some("exchange,cal_date,is_open"),
        )
        .await
        .expect("trade_cal 查询应成功");

    // 验证列名
    let col_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(col_names, vec!["exchange", "cal_date", "is_open"]);

    // 1月有31天
    assert_eq!(df.height(), 31, "1月应返回 31 行");
}

/// 测试 3: 缓存写入/读取循环
#[tokio::test]
async fn test_cache_write_and_read() {
    let (client, cache) = client_from_env().await;
    let params = [
        ("exchange", "SSE"),
        ("start_date", "20250201"),
        ("end_date", "20250210"),
    ];
    let fields = Some("exchange,cal_date,is_open");
    let cache_key = TushareClient::cache_key_for("trade_cal", &params, fields);
    let _ = cache.delete_raw(&cache_key).await;

    // 第一次查询 (从 API)
    let df1 = client
        .query("trade_cal", &params, fields)
        .await
        .expect("首次查询应成功");

    // 验证 PostgreSQL raw cache 记录存在
    let raw = cache
        .load_raw(&cache_key)
        .await
        .expect("应能读取 PostgreSQL raw cache")
        .expect("应生成 PostgreSQL raw cache 记录");
    assert_eq!(raw.api_name, "trade_cal");
    assert_eq!(raw.row_count as usize, df1.height());

    // 第二次查询 (应该命中缓存)
    let df2 = client
        .query("trade_cal", &params, fields)
        .await
        .expect("缓存查询应成功");

    // 两次结果应一致
    assert_eq!(df1.height(), df2.height());
    assert_eq!(df1.width(), df2.width());
}

/// 测试 4: 查询 daily_basic 真实财务数据
#[tokio::test]
async fn test_query_daily_basic() {
    let (client, _) = client_from_env().await;

    let df = client
        .query(
            "daily_basic",
            &[("trade_date", "20250515")],
            Some("ts_code,trade_date,pb,pe"),
        )
        .await
        .expect("daily_basic 查询应成功");

    // 应返回数据
    assert!(df.height() > 0, "daily_basic 应返回非空结果");

    // 验证列名
    let col_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(
        col_names.iter().any(|n| n == "ts_code"),
        "应包含 ts_code 列"
    );
    assert!(col_names.iter().any(|n| n == "pb"), "应包含 pb 列");
}

/// 测试 5: query_no_cache 不写入缓存
#[tokio::test]
async fn test_query_no_cache() {
    let (client, cache) = client_from_env().await;
    let params = [
        ("exchange", "SSE"),
        ("start_date", "20250301"),
        ("end_date", "20250305"),
    ];
    let fields = Some("exchange,cal_date,is_open");
    let cache_key = TushareClient::cache_key_for("trade_cal", &params, fields);
    let _ = cache.delete_raw(&cache_key).await;

    let df = client
        .query_no_cache("trade_cal", &params, fields)
        .await
        .expect("no_cache 查询应成功");

    assert_eq!(df.height(), 5, "3月1-5日应返回 5 行");

    let raw = cache
        .load_raw(&cache_key)
        .await
        .expect("应能查询 PostgreSQL raw cache");
    assert!(
        raw.is_none(),
        "query_no_cache 不应写入 PostgreSQL raw cache"
    );
}
