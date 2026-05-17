//! 配置加载 — .env 文件 + 环境变量。

use anyhow::{bail, Context, Result};

/// 应用配置。
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// Tushare API Token。
    pub tushare_token: String,
    /// PostgreSQL connection URL.
    pub database_url: String,
}

impl AppConfig {
    /// 从环境变量加载配置。
    ///
    /// 优先从 `.env` 文件读取，然后从系统环境变量补充。
    pub fn load() -> Result<Self> {
        // 加载 .env (忽略不存在的情况)
        let _ = dotenvy::dotenv();

        Self::from_env()
    }

    /// 从当前进程环境变量加载配置，不主动读取 `.env`。
    pub fn from_env() -> Result<Self> {
        let token = std::env::var("TUSHARE_TOKEN")
            .context("未找到 TUSHARE_TOKEN。请在 .env 文件中设置或导出环境变量")?;

        if token.is_empty() {
            bail!("TUSHARE_TOKEN 为空");
        }

        let database_url = std::env::var("DATABASE_URL")
            .context("未找到 DATABASE_URL。请在 .env 文件中设置或导出环境变量")?;

        if database_url.is_empty() {
            bail!("DATABASE_URL 为空");
        }

        Ok(Self {
            tushare_token: token,
            database_url,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env<F>(tushare_token: Option<&str>, database_url: Option<&str>, f: F)
    where
        F: FnOnce(),
    {
        let _guard = env_lock().lock().unwrap();
        let old_token = std::env::var("TUSHARE_TOKEN").ok();
        let old_database_url = std::env::var("DATABASE_URL").ok();

        match tushare_token {
            Some(value) => std::env::set_var("TUSHARE_TOKEN", value),
            None => std::env::remove_var("TUSHARE_TOKEN"),
        }
        match database_url {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
        }

        f();

        match old_token {
            Some(value) => std::env::set_var("TUSHARE_TOKEN", value),
            None => std::env::remove_var("TUSHARE_TOKEN"),
        }
        match old_database_url {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
        }
    }

    #[test]
    fn test_from_env_loads_required_values() {
        with_env(
            Some("test_tushare_token"),
            Some("postgresql://user:pass@localhost:5432/db"),
            || {
                let config = AppConfig::from_env().unwrap();
                assert_eq!(config.tushare_token, "test_tushare_token");
                assert_eq!(
                    config.database_url,
                    "postgresql://user:pass@localhost:5432/db"
                );
            },
        );
    }

    #[test]
    fn test_from_env_requires_database_url() {
        with_env(Some("test_tushare_token"), None, || {
            let err = AppConfig::from_env().unwrap_err().to_string();
            assert!(err.contains("DATABASE_URL"));
        });
    }
}
