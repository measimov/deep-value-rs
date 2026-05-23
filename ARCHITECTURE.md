# deep-value-rs Architecture

```mermaid
graph TB
    subgraph CLI["CLI (src/main.rs)"]
        direction TB
        PING["ping"]
        DB_PING["db ping"]
        CACHE_CLEAR["cache clear"]
        SYNC["sync"]
        SYNC_INC["sync --incremental"]
        SNAPSHOT["snapshot"]
        SNAPSHOT_LOCAL["snapshot --local"]
    end

    subgraph STRATEGY["Strategy Pipeline"]
        direction TB
        S1["Step 1: PB ≤ 1.5"]
        S2["Step 2: 10y PB max"]
        S3["Step 3: Big4 audit / equity exemption"]
        S4["Step 4: Dividend yield ≥ 0.5%"]
        S5["Step 5: Net equity ≥ 100B"]
        S6["Step 6: Anomaly removal (1a/1b/2)"]
        S7["Step 7: PB/PE/Dividend scoring"]
        S8["Step 8: Industry cap (≤ 20%)"]
        S9["Step 9: Equal-weight portfolio"]
        H["Holdings + Eliminated + StepRecords"]
    end

    subgraph DATA_ONLINE["Data Layer — Online (API)"]
        direction TB
        CS["cross_section::build_cross_section()"]
        CS_M["cross_section::get_market_pb_median()"]
        FIN_I["financials::get_*_income()"]
        FIN_EQ["financials::get_net_equity()"]
        AUDIT["main::get_audit_for_codes()"]
        DIV["main::get_*_dividend_for_codes()"]
        PB10["main::get_10y_pb_max()"]
    end

    subgraph DATA_LOCAL["Data Layer — Local (PG typed tables)"]
        direction TB
        L_CS["local::build_cross_section()"]
        L_CSM["local::get_market_pb_median()"]
        L_I10["local::get_10y_income()"]
        L_EQ["local::get_net_equity()"]
        L_AU["local::get_audit_info()"]
        L_DIV["local::get_*_dividend()"]
        L_PB10["local::get_10y_pb_max()"]
        L_FI["local::get_fina_indicator()"]
    end

    subgraph SYNC_PIPE["Sync Pipeline (src/data/sync.rs)"]
        direction TB
        FS["run_sync() — full backfill"]
        INC_DAILY["sync_daily_incremental()"]
        INC_FIN["sync_financial_incremental()"]
        INC_META["sync_meta_incremental()"]
        RL["RateLimiter"]
    end

    subgraph BACKTEST["Backtest Engine"]
        direction TB
        FP["fetch_ashare_prices()"]
        FB["fetch_benchmark()"]
        CPR["compute_period_return()"]
        CM["compute_metrics_from_nav()"]
    end

    subgraph TUSHARE["Tushare Client (src/tushare/)"]
        direction TB
        Q["query() — raw-cache hit → return"]
        QF["query_force() — skip cache, always HTTP"]
        QNC["query_no_cache() — HTTP only"]
        EX["execute_and_cache()"]

        subgraph CACHE["Cache Layer"]
            direction TB
            RAW["PgCache::save_raw() / load_raw()<br/>tushare_raw_responses"]
            TYPED["PgCache::save_typed() / load_typed()<br/>11 typed tables"]
            COV["PgCache coverage checks<br/>existing_*_dates/periods/codes()"]
        end
    end

    subgraph PG["PostgreSQL (deep_value schema)"]
        direction TB
        T_RAW["tushare_raw_responses<br/>cache_key (PK), items, fields"]
        T_CAL["tushare_trade_cal<br/>exchange, cal_date (PK)"]
        T_SB["tushare_stock_basic<br/>ts_code (PK), name, industry"]
        T_DB["tushare_daily_basic<br/>ts_code, trade_date (PK), pb, pe, dv"]
        T_INC["tushare_income<br/>ts_code, end_date, report_type (PK)"]
        T_BS["tushare_balancesheet<br/>ts_code, end_date, report_type (PK)"]
        T_AU["tushare_fina_audit<br/>ts_code, period (PK), audit_agency"]
        T_DIV["tushare_dividend<br/>row_hash (PK), ts_code, cash_div_tax"]
        T_DP["tushare_daily<br/>ts_code, trade_date (PK), close"]
        T_AF["tushare_adj_factor<br/>ts_code, trade_date (PK), adj_factor"]
        T_ID["tushare_index_daily<br/>ts_code, trade_date (PK), close"]
        T_FI["tushare_fina_indicator<br/>ts_code, end_date (PK), roe, roa, ..."]
    end

    subgraph METRICS["Metrics"]
        direction TB
        AR["annualized_return()"]
        AV["annualized_volatility()"]
        SR["sharpe_ratio()"]
        MD["max_drawdown()"]
        CR["calmar_ratio()"]
        TR["turnover_ratio()"]
        TC["transaction_cost()"]
    end

    subgraph REPORT["Report"]
        FMT["formatter::format_snapshot()"]
    end

    %% CLI → execution paths
    PING --> QNC
    DB_PING --> PG
    CACHE_CLEAR --> PG
    SYNC --> FS
    SYNC_INC --> INC_DAILY
    SYNC_INC --> INC_FIN
    SYNC_INC --> INC_META
    SYNC_INC --> COV
    SNAPSHOT --> DATA_ONLINE
    SNAPSHOT_LOCAL --> DATA_LOCAL

    %% Sync → Tushare → PG
    FS --> QF
    INC_DAILY --> QF
    INC_FIN --> QF
    INC_META --> QF
    INC_DAILY --> COV
    INC_FIN --> COV
    FS --> RL
    INC_DAILY --> RL
    INC_FIN --> RL
    INC_META --> RL

    QF --> EX
    Q --> EX
    EX --> RAW
    EX --> TYPED

    Q --> RAW

    RAW --> PG
    TYPED --> PG
    COV --> PG

    %% Online data → Tushare
    DATA_ONLINE --> Q

    %% Local data → PG typed via PgCache::load_typed()
    DATA_LOCAL --> TYPED

    %% Strategy pipeline
    SNAPSHOT --> STRATEGY
    SNAPSHOT_LOCAL --> STRATEGY
    S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7 --> S8 --> S9 --> H

    SNAPSHOT --> REPORT
    SNAPSHOT_LOCAL --> REPORT

    %% Backtest
    FP --> Q
    FB --> Q
    CPR --> METRICS
    CM --> METRICS

    %% Tushare HTTP
    EX --> |"HTTP POST"| TUSHARE_API["api.tushare.pro"]
