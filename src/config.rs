//! 配置加载 — .env 文件 + 环境变量。

use anyhow::{bail, Context, Result};

/// 应用配置。
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// Tushare API Token。
    pub tushare_token: String,
}

impl AppConfig {
    /// 从环境变量加载配置。
    ///
    /// 优先从 `.env` 文件读取，然后从系统环境变量补充。
    pub fn load() -> Result<Self> {
        // 加载 .env (忽略不存在的情况)
        let _ = dotenvy::dotenv();

        let token = std::env::var("TUSHARE_TOKEN")
            .context("未找到 TUSHARE_TOKEN。请在 .env 文件中设置或导出环境变量")?;

        if token.is_empty() {
            bail!("TUSHARE_TOKEN 为空");
        }

        Ok(Self {
            tushare_token: token,
        })
    }
}
