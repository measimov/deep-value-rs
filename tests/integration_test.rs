//! 端到端集成测试 — 验证 Tushare API 查询 + Parquet 缓存完整链路。

use deep_value::config::AppConfig;
use deep_value::tushare::cache::Cache;
use deep_value::tushare::client::TushareClient;

/// 测试 1: ping 连通性
#[tokio::test]
async fn test_ping() {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let client = TushareClient::new(&config.tushare_token);
    let result = client.ping().await;
    assert!(result.is_ok(), "ping 应成功: {:?}", result.err());
    let msg = result.unwrap();
    assert!(msg.contains("连接成功"), "应包含成功提示: {msg}");
}

/// 测试 2: 查询 trade_cal 并验证 DataFrame 结构
#[tokio::test]
async fn test_query_trade_cal() {
    let config = AppConfig::load().unwrap();
    let client = TushareClient::new(&config.tushare_token);

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
    let col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    assert_eq!(col_names, vec!["exchange", "cal_date", "is_open"]);

    // 1月有31天
    assert_eq!(df.height(), 31, "1月应返回 31 行");
}

/// 测试 3: 缓存写入/读取循环
#[tokio::test]
async fn test_cache_write_and_read() {
    // 清除旧缓存
    let cache = Cache::new("data/cache");
    let _ = cache.clear();

    let config = AppConfig::load().unwrap();
    let client = TushareClient::new(&config.tushare_token);

    // 第一次查询 (从 API)
    let df1 = client
        .query(
            "trade_cal",
            &[
                ("exchange", "SSE"),
                ("start_date", "20250201"),
                ("end_date", "20250210"),
            ],
            Some("exchange,cal_date,is_open"),
        )
        .await
        .expect("首次查询应成功");

    // 验证缓存文件存在
    let cache_dir = std::path::Path::new("data/cache");
    let parquet_files: Vec<_> = std::fs::read_dir(cache_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    assert!(!parquet_files.is_empty(), "应生成 parquet 缓存文件");

    // 第二次查询 (应该命中缓存)
    let df2 = client
        .query(
            "trade_cal",
            &[
                ("exchange", "SSE"),
                ("start_date", "20250201"),
                ("end_date", "20250210"),
            ],
            Some("exchange,cal_date,is_open"),
        )
        .await
        .expect("缓存查询应成功");

    // 两次结果应一致
    assert_eq!(df1.height(), df2.height());
    assert_eq!(df1.width(), df2.width());
}

/// 测试 4: 查询 daily_basic 真实财务数据
#[tokio::test]
async fn test_query_daily_basic() {
    let config = AppConfig::load().unwrap();
    let client = TushareClient::new(&config.tushare_token);

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
    let col_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    assert!(col_names.iter().any(|n| n == "ts_code"), "应包含 ts_code 列");
    assert!(col_names.iter().any(|n| n == "pb"), "应包含 pb 列");
}

/// 测试 5: query_no_cache 不写入缓存
#[tokio::test]
async fn test_query_no_cache() {
    // 先清除缓存
    let cache = Cache::new("data/cache");
    let _ = cache.clear();

    let config = AppConfig::load().unwrap();
    let client = TushareClient::new(&config.tushare_token);

    let df = client
        .query_no_cache(
            "trade_cal",
            &[
                ("exchange", "SSE"),
                ("start_date", "20250301"),
                ("end_date", "20250305"),
            ],
            Some("exchange,cal_date,is_open"),
        )
        .await
        .expect("no_cache 查询应成功");

    assert_eq!(df.height(), 5, "3月1-5日应返回 5 行");
}
