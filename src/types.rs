use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
pub enum AccountType {
    Checking,
    Savings,
    Brokerage,
    Credit,
    Ira,
    #[serde(rename = "401k")]
    FourOhOneK,
    Hsa,
    Other,
}

impl std::fmt::Display for AccountType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checking => write!(f, "checking"),
            Self::Savings => write!(f, "savings"),
            Self::Brokerage => write!(f, "brokerage"),
            Self::Credit => write!(f, "credit"),
            Self::Ira => write!(f, "ira"),
            Self::FourOhOneK => write!(f, "401k"),
            Self::Hsa => write!(f, "hsa"),
            Self::Other => write!(f, "other"),
        }
    }
}

impl std::str::FromStr for AccountType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "checking" => Ok(Self::Checking),
            "savings" => Ok(Self::Savings),
            "brokerage" | "investment" => Ok(Self::Brokerage),
            "credit" => Ok(Self::Credit),
            "ira" | "roth" | "traditional" => Ok(Self::Ira),
            "401k" => Ok(Self::FourOhOneK),
            "hsa" => Ok(Self::Hsa),
            _ => Ok(Self::Other),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub source: String,
    pub name: String,
    pub account_type: String,
    pub balance: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_credit: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day_change: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day_change_percent: Option<f64>,
    pub last_synced: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub source: String,
    pub account_id: String,
    pub date: String,
    pub description: String,
    pub amount: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default)]
    pub is_recurring: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recurring_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub id: String,
    pub source: String,
    pub account_id: String,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_price: Option<f64>,
    pub market_value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day_gain: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_gain: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_gain_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    pub last_synced: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceSnapshot {
    pub id: Option<i64>,
    pub source: String,
    pub account_id: String,
    pub balance: f64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncLogEntry {
    pub id: Option<i64>,
    pub source: String,
    pub status: String,
    #[serde(default)]
    pub accounts_synced: i64,
    #[serde(default)]
    pub transactions_synced: i64,
    #[serde(default)]
    pub positions_synced: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

/// Envelope for scraper JSON output and import/export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEnvelope {
    pub source: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub accounts: Vec<serde_json::Value>,
    #[serde(default)]
    pub transactions: Vec<serde_json::Value>,
    #[serde(default)]
    pub positions: Vec<serde_json::Value>,
    #[serde(default)]
    pub balance_history: Vec<serde_json::Value>,
}

/// Manifest for a scraper plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PluginManifest {
    pub name: String,
    pub display_name: String,
    #[serde(default)]
    pub account_types: Vec<String>,
    #[serde(default)]
    pub provides: Vec<String>,
    #[serde(default)]
    pub requires_headful: bool,
    #[serde(default)]
    pub config_schema: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_type_roundtrip() {
        let at: AccountType = "brokerage".parse().unwrap();
        assert_eq!(at.to_string(), "brokerage");
    }

    #[test]
    fn test_account_type_aliases() {
        let at: AccountType = "investment".parse().unwrap();
        assert_eq!(at.to_string(), "brokerage");
        let at: AccountType = "roth".parse().unwrap();
        assert_eq!(at.to_string(), "ira");
    }

    #[test]
    fn test_account_type_unknown() {
        let at: AccountType = "crypto".parse().unwrap();
        assert_eq!(at.to_string(), "other");
    }

    #[test]
    fn test_sync_envelope_deserialize() {
        let json = r#"{"source":"schwab","accounts":[],"transactions":[]}"#;
        let env: SyncEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(env.source, "schwab");
        assert!(env.accounts.is_empty());
    }

    #[test]
    fn test_plugin_manifest_deserialize() {
        let json = r#"{
            "name": "schwab",
            "display_name": "Charles Schwab",
            "account_types": ["brokerage"],
            "provides": ["accounts"],
            "requires_headful": true
        }"#;
        let m: PluginManifest = serde_json::from_str(json).unwrap();
        assert_eq!(m.name, "schwab");
        assert!(m.requires_headful);
    }
}
