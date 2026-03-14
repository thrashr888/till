use anyhow::Result;
use std::process::Command;

const SERVICE_PREFIX: &str = "till";

fn service_name(source: &str) -> String {
    format!("{SERVICE_PREFIX}.{source}")
}

/// Store username and password in macOS Keychain.
pub fn set_credentials(source: &str, username: &str, password: &str) -> Result<()> {
    let service = service_name(source);

    // Store username
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

    // Store password
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

    Ok((username, password))
}

/// Check if credentials exist for a source.
pub fn has_credentials(source: &str) -> bool {
    get_credentials(source).is_ok()
}

/// Delete credentials for a source.
pub fn delete_credentials(source: &str) -> Result<()> {
    let service = service_name(source);

    // Delete username (ignore errors if not found)
    let _ = Command::new("security")
        .args([
            "delete-generic-password",
            "-s",
            &format!("{service}.username"),
            "-a",
            source,
        ])
        .output();

    // Delete password
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

/// Get credentials from env vars as fallback.
/// Checks TILL_{SOURCE}_USERNAME and TILL_{SOURCE}_PASSWORD.
pub fn get_credentials_from_env(source: &str) -> Option<(String, String)> {
    let prefix = format!("TILL_{}", source.to_uppercase());
    let username = std::env::var(format!("{prefix}_USERNAME")).ok()?;
    let password = std::env::var(format!("{prefix}_PASSWORD")).ok()?;
    Some((username, password))
}

/// Get credentials from Keychain, falling back to env vars.
pub fn resolve_credentials(source: &str) -> Result<(String, String)> {
    if let Ok(creds) = get_credentials(source) {
        return Ok(creds);
    }
    if let Some(creds) = get_credentials_from_env(source) {
        return Ok(creds);
    }
    anyhow::bail!(
        "No credentials found for '{source}'. Use `till creds set --source {source}` or set TILL_{}_USERNAME/PASSWORD env vars.",
        source.to_uppercase()
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
