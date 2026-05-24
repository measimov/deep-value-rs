//! PostgreSQL typed Tushare table integration tests.

use std::collections::HashMap;

use deep_value::config::AppConfig;
use deep_value::db;
use deep_value::tushare::pg_cache::PgCache;
use serde_json::{json, Value};

async fn typed_cache() -> (sqlx::PgPool, PgCache) {
    let config = AppConfig::load().expect("应能加载 .env 配置");
    let pool = db::connect(&config.database_url)
        .await
        .expect("应能连接 PostgreSQL");
    db::init_schema(&pool).await.expect("应能初始化 schema");
    let cache = PgCache::new(pool.clone());
    (pool, cache)
}

fn field_names(fields: &[&str]) -> Vec<String> {
    fields.iter().map(|field| (*field).to_string()).collect()
}

async fn assert_payload_roundtrip(
    cache: &PgCache,
    api_name: &str,
    params: HashMap<String, String>,
    fields: &[&str],
    items: Vec<Vec<Value>>,
    probe_field: &str,
    expected: &str,
) {
    let requested = fields.join(",");
    let field_names = field_names(fields);

    cache
        .save_typed(api_name, &params, &field_names, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(api_name, &params, Some(requested.as_str()))
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("typed {api_name} 应存在"));

    assert_eq!(loaded.height(), items.len());
    assert_eq!(loaded.width(), fields.len());
    assert_eq!(
        loaded.column(probe_field).unwrap().str().unwrap().get(0),
        Some(expected)
    );
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
        "pretrade_date".to_string(),
    ];
    let items = vec![
        vec![json!("TST"), json!("20990101"), json!("0"), json!(null)],
        vec![
            json!("TST"),
            json!("20990102"),
            json!("1"),
            json!("20990101"),
        ],
    ];

    cache
        .save_typed("trade_cal", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(
            "trade_cal",
            &params,
            Some("exchange,cal_date,is_open,pretrade_date"),
        )
        .await
        .unwrap()
        .expect("typed trade_cal 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(
        loaded.column("cal_date").unwrap().str().unwrap().get(0),
        Some("20990101")
    );
    assert_eq!(
        loaded
            .column("pretrade_date")
            .unwrap()
            .str()
            .unwrap()
            .get(1),
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
        "symbol".to_string(),
        "name".to_string(),
        "area".to_string(),
        "industry".to_string(),
        "fullname".to_string(),
        "enname".to_string(),
        "cnspell".to_string(),
        "market".to_string(),
        "exchange".to_string(),
        "curr_type".to_string(),
        "list_status".to_string(),
        "list_date".to_string(),
        "delist_date".to_string(),
        "is_hs".to_string(),
        "act_name".to_string(),
        "act_ent_type".to_string(),
    ];
    let items = vec![
        vec![
            json!("TST001.SZ"),
            json!("TST001"),
            json!("测试一"),
            json!("深圳"),
            json!("银行"),
            json!("测试银行股份有限公司"),
            json!("Test Bank Co Ltd"),
            json!("ceshiyi"),
            json!("主板"),
            json!("SZSE"),
            json!("CNY"),
            json!("T"),
            json!("20900101"),
            json!(null),
            json!("S"),
            json!("测试实控人"),
            json!("民营企业"),
        ],
        vec![
            json!("TST002.SZ"),
            json!("TST002"),
            json!("测试二"),
            json!("山西"),
            json!("煤炭"),
            json!("测试煤炭股份有限公司"),
            json!("Test Coal Co Ltd"),
            json!("ceshier"),
            json!("主板"),
            json!("SZSE"),
            json!("CNY"),
            json!("T"),
            json!("20900102"),
            json!("20991231"),
            json!("N"),
            json!("测试集团"),
            json!("地方国企"),
        ],
    ];

    cache
        .save_typed("stock_basic", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(
            "stock_basic",
            &params,
            Some("ts_code,name,industry,fullname,exchange,curr_type,delist_date,act_name,act_ent_type"),
        )
        .await
        .unwrap()
        .expect("typed stock_basic 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(loaded.width(), 9);
    assert_eq!(
        loaded.column("industry").unwrap().str().unwrap().get(1),
        Some("煤炭")
    );
    assert_eq!(
        loaded.column("act_ent_type").unwrap().str().unwrap().get(1),
        Some("地方国企")
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
        "dv_ttm".to_string(),
        "total_mv".to_string(),
    ];
    let items = vec![
        vec![
            json!("TST001.SZ"),
            json!("20990103"),
            json!(0.51),
            json!(5.2),
            json!(3.1),
            json!(2.8),
            json!(1000.0),
        ],
        vec![
            json!("TST002.SZ"),
            json!("20990103"),
            json!(0.33),
            json!(4.8),
            json!(4.0),
            json!(3.6),
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
            Some("ts_code,trade_date,pb,pe_ttm,dv_ttm,total_mv"),
        )
        .await
        .unwrap()
        .expect("typed daily_basic 应存在");

    assert_eq!(loaded.height(), 2);
    assert_eq!(loaded.width(), 6);
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
async fn test_daily_basic_on_or_before_uses_previous_trade_day() {
    let (pool, cache) = typed_cache().await;
    sqlx::query(
        "delete from deep_value.tushare_daily_basic where ts_code like 'TSTPB%' and trade_date like '209902%'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let params = HashMap::from([("trade_date".to_string(), "20990203".to_string())]);
    let fields = vec![
        "ts_code".to_string(),
        "trade_date".to_string(),
        "pb".to_string(),
    ];
    let items = vec![vec![json!("TSTPB1.SZ"), json!("20990203"), json!(1.23)]];
    cache
        .save_typed("daily_basic", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_daily_basic_on_or_before("20990204", Some("ts_code,trade_date,pb"))
        .await
        .unwrap()
        .expect("应能用上一交易日 daily_basic");

    assert_eq!(loaded.height(), 1);
    assert_eq!(
        loaded.column("trade_date").unwrap().str().unwrap().get(0),
        Some("20990203")
    );
    assert_eq!(
        loaded.column("pb").unwrap().str().unwrap().get(0),
        Some("1.23")
    );

    sqlx::query(
        "delete from deep_value.tushare_daily_basic where ts_code like 'TSTPB%' and trade_date like '209902%'",
    )
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

#[tokio::test]
async fn test_disclosure_date_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_disclosure_date where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([("end_date".to_string(), "20981231".to_string())]);
    let fields = vec![
        "ts_code".to_string(),
        "end_date".to_string(),
        "ann_date".to_string(),
        "actual_date".to_string(),
        "pre_date".to_string(),
        "modify_date".to_string(),
    ];
    let items = vec![vec![
        json!("TST001.SZ"),
        json!("20981231"),
        json!("20990330"),
        json!("20990331"),
        json!("20990329"),
        json!("20990401"),
    ]];

    cache
        .save_typed("disclosure_date", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(
            "disclosure_date",
            &params,
            Some("ts_code,end_date,ann_date,actual_date,pre_date,modify_date"),
        )
        .await
        .unwrap()
        .expect("typed disclosure_date 应存在");

    assert_eq!(loaded.height(), 1);
    assert_eq!(
        loaded.column("pre_date").unwrap().str().unwrap().get(0),
        Some("20990329")
    );
    assert_eq!(
        loaded.column("modify_date").unwrap().str().unwrap().get(0),
        Some("20990401")
    );

    sqlx::query("delete from deep_value.tushare_disclosure_date where end_date = '20981231'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_stk_limit_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    sqlx::query("delete from deep_value.tushare_stk_limit where trade_date = '20990104'")
        .execute(&pool)
        .await
        .unwrap();

    let params = HashMap::from([("trade_date".to_string(), "20990104".to_string())]);
    let fields = vec![
        "ts_code".to_string(),
        "trade_date".to_string(),
        "pre_close".to_string(),
        "up_limit".to_string(),
        "down_limit".to_string(),
    ];
    let items = vec![vec![
        json!("TST001.SZ"),
        json!("20990104"),
        json!(10.0),
        json!(11.0),
        json!(9.0),
    ]];

    cache
        .save_typed("stk_limit", &params, &fields, &items)
        .await
        .unwrap();

    let loaded = cache
        .load_typed(
            "stk_limit",
            &params,
            Some("ts_code,trade_date,pre_close,up_limit,down_limit"),
        )
        .await
        .unwrap()
        .expect("typed stk_limit 应存在");

    assert_eq!(loaded.height(), 1);
    assert_eq!(
        loaded.column("pre_close").unwrap().str().unwrap().get(0),
        Some("10")
    );
    assert_eq!(
        loaded.column("down_limit").unwrap().str().unwrap().get(0),
        Some("9")
    );

    sqlx::query("delete from deep_value.tushare_stk_limit where trade_date = '20990104'")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_issue_21_research_endpoints_typed_roundtrip() {
    let (pool, cache) = typed_cache().await;
    for table in [
        "tushare_forecast",
        "tushare_express",
        "tushare_fina_mainbz",
        "tushare_index_weight",
        "tushare_top10_holders",
        "tushare_top10_floatholders",
        "tushare_pledge_stat",
        "tushare_repurchase",
    ] {
        sqlx::query(&format!(
            "delete from deep_value.{table} where ts_code like 'TST%' or payload->>'ts_code' like 'TST%' or payload->>'index_code' like 'TST%' or payload->>'con_code' like 'TST%'"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }

    assert_payload_roundtrip(
        &cache,
        "forecast_vip",
        HashMap::from([("period".to_string(), "20981231".to_string())]),
        &[
            "ts_code",
            "ann_date",
            "end_date",
            "type",
            "p_change_min",
            "p_change_max",
            "net_profit_min",
            "net_profit_max",
            "last_parent_net",
            "first_ann_date",
            "summary",
            "change_reason",
        ],
        vec![vec![
            json!("TST001.SZ"),
            json!("20990131"),
            json!("20981231"),
            json!("预增"),
            json!(10.5),
            json!(20.5),
            json!(100.0),
            json!(200.0),
            json!(90.0),
            json!("20990115"),
            json!("摘要"),
            json!("原因"),
        ]],
        "change_reason",
        "原因",
    )
    .await;

    assert_payload_roundtrip(
        &cache,
        "express_vip",
        HashMap::from([("period".to_string(), "20981231".to_string())]),
        &[
            "ts_code",
            "ann_date",
            "end_date",
            "revenue",
            "operate_profit",
            "total_profit",
            "n_income",
            "total_assets",
            "total_hldr_eqy_exc_min_int",
            "diluted_eps",
            "diluted_roe",
            "yoy_net_profit",
            "bps",
            "yoy_sales",
            "yoy_op",
            "yoy_tp",
            "yoy_dedu_np",
            "yoy_eps",
            "yoy_roe",
            "growth_assets",
            "yoy_equity",
            "growth_bps",
            "or_last_year",
            "op_last_year",
            "tp_last_year",
            "np_last_year",
            "eps_last_year",
            "open_net_assets",
            "open_bps",
            "perf_summary",
            "is_audit",
            "remark",
        ],
        vec![vec![
            json!("TST001.SZ"),
            json!("20990201"),
            json!("20981231"),
            json!(1000.0),
            json!(100.0),
            json!(110.0),
            json!(90.0),
            json!(5000.0),
            json!(3000.0),
            json!(1.2),
            json!(10.0),
            json!(8.0),
            json!(5.0),
            json!(6.0),
            json!(7.0),
            json!(8.0),
            json!(9.0),
            json!(10.0),
            json!(11.0),
            json!(12.0),
            json!(13.0),
            json!(14.0),
            json!(900.0),
            json!(80.0),
            json!(85.0),
            json!(70.0),
            json!(1.0),
            json!(2800.0),
            json!(4.5),
            json!("快报说明"),
            json!(1),
            json!("备注"),
        ]],
        "remark",
        "备注",
    )
    .await;

    assert_payload_roundtrip(
        &cache,
        "fina_mainbz_vip",
        HashMap::from([
            ("period".to_string(), "20981231".to_string()),
            ("type".to_string(), "P".to_string()),
        ]),
        &[
            "ts_code",
            "end_date",
            "bz_item",
            "bz_code",
            "bz_sales",
            "bz_profit",
            "bz_cost",
            "curr_type",
            "update_flag",
        ],
        vec![vec![
            json!("TST001.SZ"),
            json!("20981231"),
            json!("主营产品"),
            json!("P"),
            json!(100.0),
            json!(30.0),
            json!(70.0),
            json!("CNY"),
            json!("1"),
        ]],
        "update_flag",
        "1",
    )
    .await;

    assert_payload_roundtrip(
        &cache,
        "index_weight",
        HashMap::from([
            ("index_code".to_string(), "TSTIDX.SH".to_string()),
            ("start_date".to_string(), "20990101".to_string()),
            ("end_date".to_string(), "20990131".to_string()),
        ]),
        &["index_code", "con_code", "trade_date", "weight"],
        vec![vec![
            json!("TSTIDX.SH"),
            json!("TST001.SZ"),
            json!("20990115"),
            json!(0.88),
        ]],
        "weight",
        "0.88",
    )
    .await;

    for api_name in ["top10_holders", "top10_floatholders"] {
        assert_payload_roundtrip(
            &cache,
            api_name,
            HashMap::from([
                ("ts_code".to_string(), "TST001.SZ".to_string()),
                ("start_date".to_string(), "20980101".to_string()),
                ("end_date".to_string(), "20981231".to_string()),
            ]),
            &[
                "ts_code",
                "ann_date",
                "end_date",
                "holder_name",
                "hold_amount",
                "hold_ratio",
                "hold_float_ratio",
                "hold_change",
                "holder_type",
            ],
            vec![vec![
                json!("TST001.SZ"),
                json!("20990430"),
                json!("20981231"),
                json!("测试股东"),
                json!(1000.0),
                json!(12.3),
                json!(10.1),
                json!(5.0),
                json!("机构"),
            ]],
            "holder_type",
            "机构",
        )
        .await;
    }

    assert_payload_roundtrip(
        &cache,
        "pledge_stat",
        HashMap::from([("ts_code".to_string(), "TST001.SZ".to_string())]),
        &[
            "ts_code",
            "end_date",
            "pledge_count",
            "unrest_pledge",
            "rest_pledge",
            "total_share",
            "pledge_ratio",
        ],
        vec![vec![
            json!("TST001.SZ"),
            json!("20981231"),
            json!(3),
            json!(10.0),
            json!(5.0),
            json!(100.0),
            json!(15.0),
        ]],
        "pledge_ratio",
        "15",
    )
    .await;

    assert_payload_roundtrip(
        &cache,
        "repurchase",
        HashMap::from([
            ("start_date".to_string(), "20990101".to_string()),
            ("end_date".to_string(), "20991231".to_string()),
        ]),
        &[
            "ts_code",
            "ann_date",
            "end_date",
            "proc",
            "exp_date",
            "vol",
            "amount",
            "high_limit",
            "low_limit",
        ],
        vec![vec![
            json!("TST001.SZ"),
            json!("20990110"),
            json!("20990109"),
            json!("实施"),
            json!("20991231"),
            json!(100.0),
            json!(200.0),
            json!(12.0),
            json!(8.0),
        ]],
        "low_limit",
        "8",
    )
    .await;

    for table in [
        "tushare_forecast",
        "tushare_express",
        "tushare_fina_mainbz",
        "tushare_index_weight",
        "tushare_top10_holders",
        "tushare_top10_floatholders",
        "tushare_pledge_stat",
        "tushare_repurchase",
    ] {
        sqlx::query(&format!(
            "delete from deep_value.{table} where ts_code like 'TST%' or payload->>'ts_code' like 'TST%' or payload->>'index_code' like 'TST%' or payload->>'con_code' like 'TST%'"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
}
