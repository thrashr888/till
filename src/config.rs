use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Config {
    pub browser: BrowserConfig,
    pub sources: HashMap<String, SourceConfig>,
}

impl Config {
    /// Parse config from a TOML table, separating known sections from source sections.
    pub fn from_toml(table: toml::Table) -> Result<Self> {
        let mut sources = HashMap::new();
        let mut browser = BrowserConfig::default();

        for (key, value) in table {
            match key.as_str() {
                "browser" => {
                    if let Ok(b) = value.try_into() {
                        browser = b;
                    }
                }
                _ => {
                    // Everything else is a source
                    match value.try_into::<SourceConfig>() {
                        Ok(sc) => {
                            sources.insert(key, sc);
                        }
                        Err(e) => {
                            eprintln!("Warning: failed to parse source '{key}': {e}");
                        }
                    }
                }
            }
        }

        Ok(Self { browser, sources })
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct BrowserConfig {
    #[serde(default = "default_false")]
    pub headless: bool,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default)]
    pub user_data_dir: Option<String>,
}

impl Default for BrowserConfig {
    fn default() -> Self {
        Self {
            headless: false,
            timeout: 600,
            user_data_dir: None,
        }
    }
}

fn default_false() -> bool {
    false
}

fn default_timeout() -> u64 {
    600
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SourceConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(flatten)]
    pub extra: HashMap<String, toml::Value>,
}

fn default_true() -> bool {
    true
}

/// Config directory: `~/.config/till/`
pub fn config_dir() -> Result<PathBuf> {
    let dir = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?
        .join(".config")
        .join("till");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Database path: `~/.config/till/till.db`
pub fn db_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("till.db"))
}

/// Config file path: `~/.config/till/config.toml`
pub fn config_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.toml"))
}

/// Load config from `~/.config/till/config.toml`.
pub fn load_config() -> Result<Config> {
    let path = config_path()?;
    load_config_from(&path)
}

/// Load config from a specific path. Returns default if file doesn't exist.
pub fn load_config_from(path: &Path) -> Result<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    let content = std::fs::read_to_string(path)?;
    let table: toml::Table = toml::from_str(&content)?;
    Config::from_toml(table)
}

/// Get the browser user data directory, creating it if needed.
#[allow(dead_code)]
pub fn browser_data_dir(config: &Config) -> Result<PathBuf> {
    let dir = if let Some(ref custom) = config.browser.user_data_dir {
        let expanded = shellexpand(custom);
        PathBuf::from(expanded)
    } else {
        config_dir()?.join("chromium-data")
    };
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn shellexpand(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).to_string_lossy().to_string();
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(!config.browser.headless);
        assert_eq!(config.browser.timeout, 600);
        assert!(config.sources.is_empty());
    }

    #[test]
    fn test_parse_config() {
        let toml_str = r#"
[browser]
headless = false
timeout = 300

[schwab]
enabled = true
transaction_account = "1234"
op_item = "schwab.com"

[chase]
enabled = true
"#;
        let table: toml::Table = toml::from_str(toml_str).unwrap();
        let config = Config::from_toml(table).unwrap();
        assert_eq!(config.browser.timeout, 300);
        assert!(config.sources.contains_key("schwab"));
        assert!(config.sources["schwab"].enabled);
        assert_eq!(
            config.sources["schwab"]
                .extra
                .get("op_item")
                .and_then(|v| v.as_str()),
            Some("schwab.com")
        );
    }

    #[test]
    fn test_shellexpand() {
        let expanded = shellexpand("/absolute/path");
        assert_eq!(expanded, "/absolute/path");
    }
}
