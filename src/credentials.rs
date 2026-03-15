use anyhow::Result;
use std::process::Command;

const SERVICE_PREFIX: &str = "till";

fn service_name(source: &str) -> String {
    format!("{SERVICE_PREFIX}.{source}")
}

// ── 1Password (`op` CLI) ──

/// Get credentials from 1Password using `op` CLI.
/// `op_item` is the item name/ID in 1Password.
/// Reads username and password fields.
pub fn get_credentials_from_op(
    op_item: &str,
    username_field: &str,
    password_field: &str,
) -> Result<(String, String)> {
    let username = op_read_field(op_item, username_field)?;
    let password = op_read_field(op_item, password_field)?;
    Ok((username, password))
}

fn op_read_field(item: &str, field: &str) -> Result<String> {
    let output = Command::new("op")
        .args(["item", "get", item, "--fields", field])
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to run `op` CLI: {e}. Is 1Password CLI installed?"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("op: failed to read {field} from '{item}': {stderr}");
    }

    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

/// Check if `op` CLI is available.
#[allow(dead_code)]
pub fn has_op() -> bool {
    Command::new("op")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ── macOS Keychain ──

/// Store username and password in macOS Keychain.
pub fn set_credentials(source: &str, username: &str, password: &str) -> Result<()> {
    let service = service_name(source);

    let status = Command::new("security")
        .args([
            "add-generic-password",
            "-s",
            &format!("{service}.username"),
            "-a",
            source,
            "-w",
            username,
            "-U",
        ])
        .status()?;

    if !status.success() {
        anyhow::bail!("Failed to store username in Keychain");
    }

    let status = Command::new("security")
        .args([
            "add-generic-password",
            "-s",
            &format!("{service}.password"),
            "-a",
            source,
            "-w",
            password,
            "-U",
        ])
        .status()?;

    if !status.success() {
        anyhow::bail!("Failed to store password in Keychain");
    }

    Ok(())
}

/// Retrieve username and password from macOS Keychain.
pub fn get_credentials(source: &str) -> Result<(String, String)> {
    let service = service_name(source);

    let username = get_keychain_value(&format!("{service}.username"), source)?;
    let password = get_keychain_value(&format!("{service}.password"), source)?;

    if username.is_empty() || password.is_empty() {
        anyhow::bail!("Credentials for '{source}' are empty");
    }

    Ok((username, password))
}

/// Check if credentials exist for a source (any method).
pub fn has_credentials(source: &str) -> bool {
    // Check keychain
    if get_credentials(source).is_ok() {
        return true;
    }
    // Check env vars
    if get_credentials_from_env(source).is_some() {
        return true;
    }
    // Check if op_item is configured
    if let Ok(config) = crate::config::load_config() {
        if let Some(source_config) = config.sources.get(source) {
            if source_config.extra.contains_key("op_item") {
                return true;
            }
        }
    }
    false
}

/// Delete credentials for a source.
pub fn delete_credentials(source: &str) -> Result<()> {
    let service = service_name(source);

    let _ = Command::new("security")
        .args([
            "delete-generic-password",
            "-s",
            &format!("{service}.username"),
            "-a",
            source,
        ])
        .output();

    let _ = Command::new("security")
        .args([
            "delete-generic-password",
            "-s",
            &format!("{service}.password"),
            "-a",
            source,
        ])
        .output();

    Ok(())
}

fn get_keychain_value(service: &str, account: &str) -> Result<String> {
    let output = Command::new("security")
        .args(["find-generic-password", "-s", service, "-a", account, "-w"])
        .output()?;

    if !output.status.success() {
        anyhow::bail!("No credentials found for {account}");
    }

    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

// ── Environment variables ──

/// Get credentials from env vars: TILL_{SOURCE}_USERNAME / TILL_{SOURCE}_PASSWORD.
pub fn get_credentials_from_env(source: &str) -> Option<(String, String)> {
    let prefix = format!("TILL_{}", source.to_uppercase());
    let username = std::env::var(format!("{prefix}_USERNAME")).ok()?;
    let password = std::env::var(format!("{prefix}_PASSWORD")).ok()?;
    Some((username, password))
}

// ── Resolution chain ──

/// Resolve credentials for a source. Priority:
/// 1. 1Password (if `op_item` configured in config.toml)
/// 2. macOS Keychain
/// 3. Environment variables
pub fn resolve_credentials(source: &str) -> Result<(String, String)> {
    // 1. Try 1Password if configured
    if let Ok(config) = crate::config::load_config() {
        if let Some(source_config) = config.sources.get(source) {
            if let Some(op_item) = source_config.extra.get("op_item").and_then(|v| v.as_str()) {
                let username_field = source_config
                    .extra
                    .get("op_username_field")
                    .and_then(|v| v.as_str())
                    .unwrap_or("username");
                let password_field = source_config
                    .extra
                    .get("op_password_field")
                    .and_then(|v| v.as_str())
                    .unwrap_or("password");

                match get_credentials_from_op(op_item, username_field, password_field) {
                    Ok(creds) => return Ok(creds),
                    Err(e) => {
                        eprintln!("Warning: 1Password lookup failed for '{source}': {e}");
                        // Fall through to other methods
                    }
                }
            }
        }
    }

    // 2. Try macOS Keychain
    if let Ok(creds) = get_credentials(source) {
        return Ok(creds);
    }

    // 3. Try env vars
    if let Some(creds) = get_credentials_from_env(source) {
        return Ok(creds);
    }

    anyhow::bail!(
        "No credentials found for '{source}'. Options:\n  \
         - Add op_item to config:  [{}] op_item = \"Item Name\"\n  \
         - Use Keychain:           till creds set --source {source}\n  \
         - Use env vars:           TILL_{}_USERNAME / TILL_{}_PASSWORD",
        source,
        source.to_uppercase(),
        source.to_uppercase(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_name() {
        assert_eq!(service_name("schwab"), "till.schwab");
        assert_eq!(service_name("chase"), "till.chase");
    }

    #[test]
    fn test_env_fallback_missing() {
        assert!(get_credentials_from_env("nonexistent_test_source_xyz").is_none());
    }
}
