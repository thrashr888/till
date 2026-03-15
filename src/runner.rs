use crate::types::SyncEnvelope;
use anyhow::Result;
use std::path::PathBuf;
use tokio::process::Command;

/// Find the scrapers directory (relative to the binary or well-known locations).
pub fn find_scrapers_dir() -> Result<PathBuf> {
    // 1. Relative to executable
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            let candidate = parent.join("scrapers");
            if candidate.join("pyproject.toml").exists() {
                return Ok(candidate);
            }
            // Up one level (e.g., target/debug/till -> project root)
            if let Some(grandparent) = parent.parent() {
                let candidate = grandparent.join("scrapers");
                if candidate.join("pyproject.toml").exists() {
                    return Ok(candidate);
                }
                // Up two levels
                if let Some(great) = grandparent.parent() {
                    let candidate = great.join("scrapers");
                    if candidate.join("pyproject.toml").exists() {
                        return Ok(candidate);
                    }
                }
            }
        }
    }

    // 2. Relative to cwd
    if let Ok(cwd) = std::env::current_dir() {
        let candidate = cwd.join("scrapers");
        if candidate.join("pyproject.toml").exists() {
            return Ok(candidate);
        }
    }

    // 3. Well-known dev path
    if let Some(home) = dirs::home_dir() {
        let candidate = home.join("Workspace/till/scrapers");
        if candidate.join("pyproject.toml").exists() {
            return Ok(candidate);
        }
    }

    anyhow::bail!("Could not find till scrapers directory. Ensure scrapers/pyproject.toml exists.")
}

/// Find the `uv` binary.
fn find_uv() -> PathBuf {
    if let Ok(p) = which::which("uv") {
        return p;
    }
    if let Some(home) = dirs::home_dir() {
        for rel in [".local/bin/uv", ".cargo/bin/uv"] {
            let candidate = home.join(rel);
            if candidate.exists() {
                return candidate;
            }
        }
    }
    for p in ["/opt/homebrew/bin/uv", "/usr/local/bin/uv"] {
        let candidate = PathBuf::from(p);
        if candidate.exists() {
            return candidate;
        }
    }
    PathBuf::from("uv")
}

/// List available scraper sources by calling `till-scrape --list`.
pub async fn list_sources() -> Result<Vec<String>> {
    let scrapers_dir = find_scrapers_dir()?;
    let uv = find_uv();

    let output = Command::new(&uv)
        .args(["run", "--directory"])
        .arg(&scrapers_dir)
        .args(["till-scrape", "--list"])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Failed to list sources: {stderr}");
    }

    let stdout = String::from_utf8(output.stdout)?;
    let sources: Vec<String> = stdout.lines().map(|l| l.trim().to_string()).collect();
    Ok(sources)
}

/// Run a scraper for the given source.
pub async fn run_scraper(
    source: &str,
    config_path: Option<&str>,
    test_mode: bool,
) -> Result<SyncEnvelope> {
    let scrapers_dir = find_scrapers_dir()?;
    let uv = find_uv();

    let mut cmd = Command::new(&uv);
    cmd.arg("run")
        .arg("--directory")
        .arg(&scrapers_dir)
        .arg("till-scrape")
        .arg("--source")
        .arg(source);

    if let Some(config) = config_path {
        cmd.arg("--config").arg(config);
    }

    if test_mode {
        cmd.env("TILL_TEST_MODE", "1");
    }

    // Pass credentials via env if available
    if let Ok((username, password)) = crate::credentials::resolve_credentials(source) {
        cmd.env("TILL_USERNAME", &username);
        cmd.env("TILL_PASSWORD", &password);
    }

    // Pass browser config
    if let Ok(config) = crate::config::load_config() {
        if !config.browser.headless {
            cmd.env("TILL_HEADFUL", "1");
        }
    }

    // Pass source config extras as TILL_{SOURCE}_{KEY} env vars
    pass_config_env(&mut cmd, source);

    let output = tokio::time::timeout(std::time::Duration::from_secs(600), cmd.output())
        .await
        .map_err(|_| anyhow::anyhow!("{source} scraper timed out after 10 minutes"))?
        .map_err(|e| anyhow::anyhow!("Failed to run uv: {e}"))?;

    // Forward stderr (scraper progress output)
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines() {
        if !line.is_empty() {
            eprintln!("[{source}] {line}");
        }
    }

    if !output.status.success() {
        anyhow::bail!(
            "{source} scraper failed (exit {}): {}",
            output.status,
            stderr.lines().last().unwrap_or("unknown error")
        );
    }

    let stdout = String::from_utf8(output.stdout)?;
    let trimmed = stdout.trim();

    if trimmed.is_empty() {
        anyhow::bail!("{source} scraper produced no output");
    }

    let envelope: SyncEnvelope = serde_json::from_str(trimmed)
        .map_err(|e| anyhow::anyhow!("Failed to parse {source} output: {e}"))?;

    Ok(envelope)
}

/// Pass config extras for a source as env vars: TILL_{SOURCE}_{KEY}=value
fn pass_config_env(cmd: &mut Command, source: &str) {
    if let Ok(config) = crate::config::load_config() {
        if let Some(source_config) = config.sources.get(source) {
            let prefix = format!("TILL_{}", source.to_uppercase());
            for (key, value) in &source_config.extra {
                // Skip credential-related keys (handled separately)
                if key.starts_with("op_") {
                    continue;
                }
                let env_key = format!("{}_{}", prefix, key.to_uppercase());
                let env_val = match value {
                    toml::Value::String(s) => s.clone(),
                    toml::Value::Array(arr) => arr
                        .iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                    other => other.to_string(),
                };
                cmd.env(&env_key, &env_val);
            }
        }
    }
}

/// Run scraper in test/replay mode.
pub async fn run_test(
    source: &str,
    headful: bool,
    pause: bool,
    save_html: bool,
    replay: Option<&str>,
) -> Result<SyncEnvelope> {
    let scrapers_dir = find_scrapers_dir()?;
    let uv = find_uv();

    let mut cmd = Command::new(&uv);
    cmd.arg("run")
        .arg("--directory")
        .arg(&scrapers_dir)
        .arg("till-scrape")
        .arg("--source")
        .arg(source);

    cmd.env("TILL_TEST_MODE", "1");

    if headful {
        cmd.env("TILL_HEADFUL", "1");
    }
    if pause {
        cmd.env("TILL_PAUSE", "1");
    }
    if save_html {
        cmd.env("TILL_SAVE_HTML", "1");
    }
    if let Some(replay_file) = replay {
        cmd.arg("--replay").arg(replay_file);
    }

    // Pass credentials via env if available
    if let Ok((username, password)) = crate::credentials::resolve_credentials(source) {
        cmd.env("TILL_USERNAME", &username);
        cmd.env("TILL_PASSWORD", &password);
    }

    // Pass source config extras as env vars
    pass_config_env(&mut cmd, source);

    let output = tokio::time::timeout(std::time::Duration::from_secs(600), cmd.output())
        .await
        .map_err(|_| anyhow::anyhow!("{source} test timed out after 10 minutes"))?
        .map_err(|e| anyhow::anyhow!("Failed to run uv: {e}"))?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines() {
        if !line.is_empty() {
            eprintln!("[{source}] {line}");
        }
    }

    let stdout = String::from_utf8(output.stdout)?;
    let trimmed = stdout.trim();

    if trimmed.is_empty() {
        return Ok(SyncEnvelope {
            source: source.to_string(),
            status: Some("ok".to_string()),
            error: None,
            accounts: vec![],
            transactions: vec![],
            positions: vec![],
            balance_history: vec![],
        });
    }

    let envelope: SyncEnvelope = serde_json::from_str(trimmed)
        .map_err(|e| anyhow::anyhow!("Failed to parse {source} test output: {e}"))?;

    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_uv_no_panic() {
        let _ = find_uv();
    }
}
