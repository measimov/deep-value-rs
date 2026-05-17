//! PostgreSQL typed Tushare table integration tests.

use std::collections::HashMap;

use deep_value::config::AppConfig;
use deep_value::db;
use deep_value::tushare::pg_cache::PgCache;
use serde_json::json;

async fn typed_cache() -> (sqlx::PgPool, PgCache) {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");
    db::init_schema(&pool).await.expect("应能初始化 schema");
    let cache = PgCache::new(pool.clone());
    (pool, cache)
}

#[tokio::test]
async fn test_trade_cal_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query(
        "delete from deep_value.tushare_trade_cal where exchange = 'TST' and cal_date like '209901%'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let params = HashMap::from([
        ("exchange".to_string(), "TST".to_string()),
        ("start_date".to_string(), "20990101".to_string()),
        ("end_date".to_string(), "20990102".to_string()),
    ]);
    let fields = vec![
        "exchange".to_string(),
        "cal_date".to_string(),
        "is_open".to_string(),
    ];
    let items = vec![
        vec![json!("TST"), json!("20990101"), json!("0")],
        vec![json!("TST"), json!("20990102"), json!("1")],
    ];

    cache
        .save_typed("trade_cal", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed("trade_cal", &params, Some("exchange,cal_date,is_open"))
        .await
        .unwrap()
        .expect("typed trade_cal 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(
        loaded.column("cal_date").unwrap().str().unwrap().get(0),
        Some("20990101")
    );

    sqlx::query(
        "delete from deep_value.tushare_trade_cal where exchange = 'TST' and cal_date like '209901%'",
    )
    .execute(&pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn test_stock_basic_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_stock_basic where ts_code like 'TST%.SZ'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([("list_status".to_string(), "T".to_string())]);
    let fields = vec![
        "ts_code".to_string(),
        "name".to_string(),
        "industry".to_string(),
        "list_status".to_string(),
    ];
    let items = vec![
        vec![
            json!("TST001.SZ"),
            json!("测试一"),
            json!("银行"),
            json!("T"),
        ],
        vec![
            json!("TST002.SZ"),
            json!("测试二"),
            json!("煤炭"),
            json!("T"),
        ],
    ];

    cache
        .save_typed("stock_basic", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed("stock_basic", &params, Some("ts_code,name,industry"))
        .await
        .unwrap()
        .expect("typed stock_basic 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(loaded.width(), 3);
    assert_eq!(
        loaded.column("industry").unwrap().str().unwrap().get(1),
        Some("煤炭")
    );

    sqlx::query("delete from deep_value.tushare_stock_basic where ts_code like 'TST%.SZ'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_daily_basic_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_daily_basic where trade_date = '20990103'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([("trade_date".to_string(), "20990103".to_string())]);
    let fields = vec![
        "ts_code".to_string(),
        "trade_date".to_string(),
        "pb".to_string(),
        "pe_ttm".to_string(),
        "dv_ratio".to_string(),
        "total_mv".to_string(),
    ];
    let items = vec![
        vec![
            json!("TST001.SZ"),
            json!("20990103"),
            json!(0.51),
            json!(5.2),
            json!(3.1),
            json!(1000.0),
        ],
        vec![
            json!("TST002.SZ"),
            json!("20990103"),
            json!(0.33),
            json!(4.8),
            json!(4.0),
            json!(2000.0),
        ],
    ];

    cache
        .save_typed("daily_basic", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(
            "daily_basic",
            &params,
            Some("ts_code,trade_date,pb,pe_ttm,total_mv"),
        )
        .await
        .unwrap()
        .expect("typed daily_basic 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(loaded.width(), 5);
    assert_eq!(
        loaded.column("pb").unwrap().str().unwrap().get(0),
        Some("0.51")
    );

    sqlx::query("delete from deep_value.tushare_daily_basic where trade_date = '20990103'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_income_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_income where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([
        ("period".to_string(), "20981231".to_string()),
        ("report_type".to_string(), "1".to_string()),
    ]);
    let fields = vec![
        "ts_code".to_string(),
        "end_date".to_string(),
        "n_income".to_string(),
    ];
    let items = vec![
        vec![json!("TST001.SZ"), json!("20981231"), json!(100.5)],
        vec![json!("TST002.SZ"), json!("20981231"), json!(-20.0)],
    ];

    cache
        .save_typed("income", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed("income", &params, Some("ts_code,end_date,n_income"))
        .await
        .unwrap()
        .expect("typed income 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(
        loaded.column("n_income").unwrap().str().unwrap().get(0),
        Some("100.5")
    );

    sqlx::query("delete from deep_value.tushare_income where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dividend_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_dividend where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([("end_date".to_string(), "20981231".to_string())]);
    let fields = vec![
        "ts_code".to_string(),
        "end_date".to_string(),
        "cash_div_tax".to_string(),
        "stk_div".to_string(),
    ];
    let items = vec![
        vec![
            json!("TST001.SZ"),
            json!("20981231"),
            json!(1.2),
            json!(0.0),
        ],
        vec![
            json!("TST001.SZ"),
            json!("20981231"),
            json!(0.8),
            json!(0.1),
        ],
    ];

    cache
        .save_typed("dividend", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(
            "dividend",
            &params,
            Some("ts_code,end_date,cash_div_tax,stk_div"),
        )
        .await
        .unwrap()
        .expect("typed dividend 应存在");

    assert_eq!(loaded.height(), 2);

    sqlx::query("delete from deep_value.tushare_dividend where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_balancesheet_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_balancesheet where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([
        ("period".to_string(), "20981231".to_string()),
        ("report_type".to_string(), "1".to_string()),
    ]);
    let fields = vec![
        "ts_code".to_string(),
        "end_date".to_string(),
        "total_hldr_eqy_exc_min_int".to_string(),
    ];
    let items = vec![vec![
        json!("TST001.SZ"),
        json!("20981231"),
        json!(123456789.0),
    ]];

    cache
        .save_typed("balancesheet", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(
            "balancesheet",
            &params,
            Some("ts_code,end_date,total_hldr_eqy_exc_min_int"),
        )
        .await
        .unwrap()
        .expect("typed balancesheet 应存在");

    assert_eq!(loaded.height(), 1);
    assert_eq!(
        loaded
            .column("total_hldr_eqy_exc_min_int")
            .unwrap()
            .str()
            .unwrap()
            .get(0),
        Some("123456789")
    );

    sqlx::query("delete from deep_value.tushare_balancesheet where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_fina_audit_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_fina_audit where period = '20981231'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([("period".to_string(), "20981231".to_string())]);
    let fields = vec!["ts_code".to_string(), "audit_agency".to_string()];
    let items = vec![vec![json!("TST001.SZ"), json!("普华永道中天会计师事务所")]];

    cache
        .save_typed("fina_audit", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed("fina_audit", &params, Some("ts_code,audit_agency"))
        .await
        .unwrap()
        .expect("typed fina_audit 应存在");

    assert_eq!(loaded.height(), 1);
    assert_eq!(
        loaded.column("audit_agency").unwrap().str().unwrap().get(0),
        Some("普华永道中天会计师事务所")
    );

    sqlx::query("delete from deep_value.tushare_fina_audit where period = '20981231'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_daily_price_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_daily where ts_code = 'TST001.SZ'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([
        ("ts_code".to_string(), "TST001.SZ".to_string()),
        ("start_date".to_string(), "20990101".to_string()),
        ("end_date".to_string(), "20990102".to_string()),
    ]);
    let fields = vec![
        "ts_code".to_string(),
        "trade_date".to_string(),
        "close".to_string(),
    ];
    let items = vec![
        vec![json!("TST001.SZ"), json!("20990101"), json!(10.5)],
        vec![json!("TST001.SZ"), json!("20990102"), json!(11.0)],
    ];

    cache
        .save_typed("daily", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed("daily", &params, Some("ts_code,trade_date,close"))
        .await
        .unwrap()
        .expect("typed daily 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(
        loaded.column("close").unwrap().str().unwrap().get(0),
        Some("10.5")
    );

    sqlx::query("delete from deep_value.tushare_daily where ts_code = 'TST001.SZ'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_adj_factor_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_adj_factor where ts_code = 'TST001.SZ'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([
        ("ts_code".to_string(), "TST001.SZ".to_string()),
        ("start_date".to_string(), "20990101".to_string()),
        ("end_date".to_string(), "20990102".to_string()),
    ]);
    let fields = vec![
        "ts_code".to_string(),
        "trade_date".to_string(),
        "adj_factor".to_string(),
    ];
    let items = vec![
        vec![json!("TST001.SZ"), json!("20990101"), json!(1.01)],
        vec![json!("TST001.SZ"), json!("20990102"), json!(1.02)],
    ];

    cache
        .save_typed("adj_factor", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed("adj_factor", &params, Some("ts_code,trade_date,adj_factor"))
        .await
        .unwrap()
        .expect("typed adj_factor 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(
        loaded.column("adj_factor").unwrap().str().unwrap().get(1),
        Some("1.02")
    );

    sqlx::query("delete from deep_value.tushare_adj_factor where ts_code = 'TST001.SZ'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_index_daily_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_index_daily where ts_code = 'TSTIDX.SH'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([
        ("ts_code".to_string(), "TSTIDX.SH".to_string()),
        ("start_date".to_string(), "20990101".to_string()),
        ("end_date".to_string(), "20990102".to_string()),
    ]);
    let fields = vec![
        "ts_code".to_string(),
        "trade_date".to_string(),
        "close".to_string(),
    ];
    let items = vec![
        vec![json!("TSTIDX.SH"), json!("20990101"), json!(3000.0)],
        vec![json!("TSTIDX.SH"), json!("20990102"), json!(3010.5)],
    ];

    cache
        .save_typed("index_daily", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed("index_daily", &params, Some("trade_date,close"))
        .await
        .unwrap()
        .expect("typed index_daily 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(loaded.width(), 2);
    assert_eq!(
        loaded.column("close").unwrap().str().unwrap().get(1),
        Some("3010.5")
    );

    sqlx::query("delete from deep_value.tushare_index_daily where ts_code = 'TSTIDX.SH'")
        .execute(&pool)
        .await
        .unwrap();
}
