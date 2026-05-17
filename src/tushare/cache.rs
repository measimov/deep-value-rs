//! Parquet 文件缓存层。
//!
//! 将 API 返回的 DataFrame 序列化为 Parquet 文件存储在本地，
//! 后续相同请求直接从文件读取，避免重复调用 API。

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use polars::prelude::*;
use tracing::debug;

/// 文件系统缓存。
pub struct Cache {
    base_dir: PathBuf,
}

impl Cache {
    /// 创建缓存实例。
    pub fn new(base_dir: &str) -> Self {
        Self {
            base_dir: PathBuf::from(base_dir),
        }
    }

    /// 尝试从缓存加载 DataFrame。
    ///
    /// 缓存 key 会被转换为文件名 `{key}.parquet`。
    /// 返回 `Ok(None)` 表示缓存未命中。
    pub fn load(&self, key: &str) -> Result<Option<DataFrame>> {
        let path = self.path_for(key);
        if !path.exists() {
            return Ok(None);
        }

        let file = fs::File::open(&path)
            .with_context(|| format!("无法打开缓存文件: {}", path.display()))?;

        let df = ParquetReader::new(file)
            .finish()
            .with_context(|| format!("无法读取 Parquet: {}", path.display()))?;

        debug!(key = key, rows = df.height(), "缓存读取");
        Ok(Some(df))
    }

    /// 将 DataFrame 写入缓存。
    pub fn save(&self, key: &str, df: &DataFrame) -> Result<()> {
        let path = self.path_for(key);

        // 确保目录存在
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("无法创建缓存目录: {}", parent.display()))?;
        }

        let file = fs::File::create(&path)
            .with_context(|| format!("无法创建缓存文件: {}", path.display()))?;

        let mut df_clone = df.clone();
        ParquetWriter::new(file)
            .finish(&mut df_clone)
            .with_context(|| format!("无法写入 Parquet: {}", path.display()))?;

        debug!(key = key, rows = df.height(), "缓存写入");
        Ok(())
    }

    /// 清除所有缓存文件。
    pub fn clear(&self) -> Result<usize> {
        if !self.base_dir.exists() {
            return Ok(0);
        }

        let mut count = 0;
        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                fs::remove_file(&path)?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// 获取缓存文件路径。
    fn path_for(&self, key: &str) -> PathBuf {
        // 清理 key 中的非法文件名字符
        let safe_key: String = key
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        self.base_dir.join(format!("{safe_key}.parquet"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_roundtrip() {
        let cache = Cache::new("data/cache/test");
        let df = df!(
            "ts_code" => &["000001.SZ", "600016.SH"],
            "pb" => &["0.51", "0.33"],
        )
        .unwrap();

        // 写入
        cache.save("test_roundtrip", &df).unwrap();

        // 读取
        let loaded = cache.load("test_roundtrip").unwrap().unwrap();
        assert_eq!(loaded.height(), 2);
        assert_eq!(loaded.width(), 2);

        // 清理
        let _ = std::fs::remove_file("data/cache/test/test_roundtrip.parquet");
        let _ = std::fs::remove_dir("data/cache/test");
    }

    #[test]
    fn test_cache_miss() {
        let cache = Cache::new("data/cache/test");
        let result = cache.load("nonexistent_key").unwrap();
        assert!(result.is_none());
    }
}
