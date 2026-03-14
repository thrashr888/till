use crate::types::{Account, BalanceSnapshot, Position, SyncEnvelope, SyncLogEntry, Transaction};
use anyhow::Result;
use rusqlite::{params, Connection};
use std::path::Path;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    #[cfg(test)]
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                name TEXT NOT NULL,
                account_type TEXT NOT NULL,
                balance REAL NOT NULL,
                available_credit REAL,
                day_change REAL,
                day_change_percent REAL,
                last_synced TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                category TEXT,
                status TEXT,
                is_recurring INTEGER DEFAULT 0,
                recurring_group TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );

            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                account_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                description TEXT,
                quantity REAL,
                last_price REAL,
                market_value REAL NOT NULL,
                day_gain REAL,
                total_gain REAL,
                total_gain_percent REAL,
                metadata TEXT,
                last_synced TEXT NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );

            CREATE TABLE IF NOT EXISTS balance_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                account_id TEXT NOT NULL,
                balance REAL NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );

            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                accounts_synced INTEGER DEFAULT 0,
                transactions_synced INTEGER DEFAULT 0,
                positions_synced INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT
            );
            ",
        )?;
        Ok(())
    }

    // ── Accounts ──

    pub fn upsert_account(&self, account: &Account) -> Result<()> {
        self.conn.execute(
            "INSERT INTO accounts (id, source, name, account_type, balance, available_credit, day_change, day_change_percent, last_synced, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT (id) DO UPDATE SET
                name = excluded.name,
                account_type = excluded.account_type,
                balance = excluded.balance,
                available_credit = excluded.available_credit,
                day_change = excluded.day_change,
                day_change_percent = excluded.day_change_percent,
                last_synced = excluded.last_synced,
                metadata = excluded.metadata,
                updated_at = datetime('now')",
            params![
                account.id,
                account.source,
                account.name,
                account.account_type,
                account.balance,
                account.available_credit,
                account.day_change,
                account.day_change_percent,
                account.last_synced,
                account.metadata.as_ref().map(|m| m.to_string()),
            ],
        )?;
        Ok(())
    }

    pub fn list_accounts(
        &self,
        source: Option<&str>,
        account_type: Option<&str>,
    ) -> Result<Vec<Account>> {
        let mut sql = "SELECT id, source, name, account_type, balance, available_credit, day_change, day_change_percent, last_synced, metadata FROM accounts WHERE 1=1".to_string();
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

        if let Some(s) = source {
            sql.push_str(" AND source = ?");
            param_values.push(Box::new(s.to_string()));
        }
        if let Some(t) = account_type {
            sql.push_str(" AND account_type = ?");
            param_values.push(Box::new(t.to_string()));
        }
        sql.push_str(" ORDER BY source, name");

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let metadata_str: Option<String> = row.get(9)?;
            Ok(Account {
                id: row.get(0)?,
                source: row.get(1)?,
                name: row.get(2)?,
                account_type: row.get(3)?,
                balance: row.get(4)?,
                available_credit: row.get(5)?,
                day_change: row.get(6)?,
                day_change_percent: row.get(7)?,
                last_synced: row.get(8)?,
                metadata: metadata_str.and_then(|s| serde_json::from_str(&s).ok()),
            })
        })?;

        let mut accounts = vec![];
        for row in rows {
            accounts.push(row?);
        }
        Ok(accounts)
    }

    // ── Transactions ──

    pub fn upsert_transaction(&self, txn: &Transaction) -> Result<()> {
        self.conn.execute(
            "INSERT INTO transactions (id, source, account_id, date, description, amount, category, status, is_recurring, recurring_group, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT (id) DO UPDATE SET
                description = excluded.description,
                amount = excluded.amount,
                category = excluded.category,
                status = excluded.status,
                is_recurring = excluded.is_recurring,
                recurring_group = excluded.recurring_group,
                metadata = excluded.metadata",
            params![
                txn.id,
                txn.source,
                txn.account_id,
                txn.date,
                txn.description,
                txn.amount,
                txn.category,
                txn.status,
                txn.is_recurring as i32,
                txn.recurring_group,
                txn.metadata.as_ref().map(|m| m.to_string()),
            ],
        )?;
        Ok(())
    }

    pub fn list_transactions(
        &self,
        source: Option<&str>,
        account_type: Option<&str>,
        days: Option<u32>,
        category: Option<&str>,
    ) -> Result<Vec<Transaction>> {
        let mut sql = "SELECT t.id, t.source, t.account_id, t.date, t.description, t.amount, t.category, t.status, t.is_recurring, t.recurring_group, t.metadata
             FROM transactions t".to_string();

        let mut conditions = vec!["1=1".to_string()];
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

        if account_type.is_some() {
            sql.push_str(" JOIN accounts a ON t.account_id = a.id");
        }

        if let Some(s) = source {
            conditions.push("t.source = ?".to_string());
            param_values.push(Box::new(s.to_string()));
        }
        if let Some(t) = account_type {
            conditions.push("a.account_type = ?".to_string());
            param_values.push(Box::new(t.to_string()));
        }
        if let Some(d) = days {
            conditions.push(format!("t.date >= date('now', '-{d} days')"));
        }
        if let Some(c) = category {
            conditions.push("t.category = ?".to_string());
            param_values.push(Box::new(c.to_string()));
        }

        sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        sql.push_str(" ORDER BY t.date DESC, t.id");

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let metadata_str: Option<String> = row.get(10)?;
            Ok(Transaction {
                id: row.get(0)?,
                source: row.get(1)?,
                account_id: row.get(2)?,
                date: row.get(3)?,
                description: row.get(4)?,
                amount: row.get(5)?,
                category: row.get(6)?,
                status: row.get(7)?,
                is_recurring: row.get::<_, i32>(8)? != 0,
                recurring_group: row.get(9)?,
                metadata: metadata_str.and_then(|s| serde_json::from_str(&s).ok()),
            })
        })?;

        let mut txns = vec![];
        for row in rows {
            txns.push(row?);
        }
        Ok(txns)
    }

    // ── Positions ──

    pub fn upsert_position(&self, pos: &Position) -> Result<()> {
        self.conn.execute(
            "INSERT INTO positions (id, source, account_id, symbol, description, quantity, last_price, market_value, day_gain, total_gain, total_gain_percent, metadata, last_synced)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
             ON CONFLICT (id) DO UPDATE SET
                symbol = excluded.symbol,
                description = excluded.description,
                quantity = excluded.quantity,
                last_price = excluded.last_price,
                market_value = excluded.market_value,
                day_gain = excluded.day_gain,
                total_gain = excluded.total_gain,
                total_gain_percent = excluded.total_gain_percent,
                metadata = excluded.metadata,
                last_synced = excluded.last_synced",
            params![
                pos.id,
                pos.source,
                pos.account_id,
                pos.symbol,
                pos.description,
                pos.quantity,
                pos.last_price,
                pos.market_value,
                pos.day_gain,
                pos.total_gain,
                pos.total_gain_percent,
                pos.metadata.as_ref().map(|m| m.to_string()),
                pos.last_synced,
            ],
        )?;
        Ok(())
    }

    pub fn list_positions(&self, source: Option<&str>) -> Result<Vec<Position>> {
        let mut sql = "SELECT id, source, account_id, symbol, description, quantity, last_price, market_value, day_gain, total_gain, total_gain_percent, metadata, last_synced FROM positions WHERE 1=1".to_string();
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

        if let Some(s) = source {
            sql.push_str(" AND source = ?");
            param_values.push(Box::new(s.to_string()));
        }
        sql.push_str(" ORDER BY market_value DESC");

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let metadata_str: Option<String> = row.get(11)?;
            Ok(Position {
                id: row.get(0)?,
                source: row.get(1)?,
                account_id: row.get(2)?,
                symbol: row.get(3)?,
                description: row.get(4)?,
                quantity: row.get(5)?,
                last_price: row.get(6)?,
                market_value: row.get(7)?,
                day_gain: row.get(8)?,
                total_gain: row.get(9)?,
                total_gain_percent: row.get(10)?,
                metadata: metadata_str.and_then(|s| serde_json::from_str(&s).ok()),
                last_synced: row.get(12)?,
            })
        })?;

        let mut positions = vec![];
        for row in rows {
            positions.push(row?);
        }
        Ok(positions)
    }

    // ── Balance History ──

    pub fn insert_balance_snapshot(&self, snap: &BalanceSnapshot) -> Result<()> {
        self.conn.execute(
            "INSERT INTO balance_history (source, account_id, balance, timestamp)
             VALUES (?1, ?2, ?3, ?4)",
            params![snap.source, snap.account_id, snap.balance, snap.timestamp],
        )?;
        Ok(())
    }

    pub fn list_balance_history(&self, account_id: &str) -> Result<Vec<BalanceSnapshot>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source, account_id, balance, timestamp FROM balance_history WHERE account_id = ? ORDER BY timestamp DESC",
        )?;
        let rows = stmt.query_map(params![account_id], |row| {
            Ok(BalanceSnapshot {
                id: row.get(0)?,
                source: row.get(1)?,
                account_id: row.get(2)?,
                balance: row.get(3)?,
                timestamp: row.get(4)?,
            })
        })?;

        let mut history = vec![];
        for row in rows {
            history.push(row?);
        }
        Ok(history)
    }

    // ── Sync Log ──

    pub fn log_sync_start(&self, source: &str) -> Result<i64> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO sync_log (source, status, started_at) VALUES (?1, 'running', ?2)",
            params![source, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn log_sync_finish(
        &self,
        log_id: i64,
        status: &str,
        accounts: i64,
        transactions: i64,
        positions: i64,
        error: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "UPDATE sync_log SET status = ?1, accounts_synced = ?2, transactions_synced = ?3, positions_synced = ?4, error_message = ?5, finished_at = ?6 WHERE id = ?7",
            params![status, accounts, transactions, positions, error, now, log_id],
        )?;
        Ok(())
    }

    pub fn list_sync_log(&self, source: Option<&str>, limit: usize) -> Result<Vec<SyncLogEntry>> {
        let mut sql =
            "SELECT id, source, status, accounts_synced, transactions_synced, positions_synced, error_message, started_at, finished_at FROM sync_log WHERE 1=1"
                .to_string();
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

        if let Some(s) = source {
            sql.push_str(" AND source = ?");
            param_values.push(Box::new(s.to_string()));
        }
        sql.push_str(&format!(" ORDER BY started_at DESC LIMIT {limit}"));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(SyncLogEntry {
                id: row.get(0)?,
                source: row.get(1)?,
                status: row.get(2)?,
                accounts_synced: row.get(3)?,
                transactions_synced: row.get(4)?,
                positions_synced: row.get(5)?,
                error_message: row.get(6)?,
                started_at: row.get(7)?,
                finished_at: row.get(8)?,
            })
        })?;

        let mut entries = vec![];
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    // ── Import from SyncEnvelope ──

    pub fn import_envelope(&self, envelope: &SyncEnvelope) -> Result<(i64, i64, i64)> {
        let now = chrono::Utc::now().to_rfc3339();
        let mut accounts_count = 0i64;
        let mut transactions_count = 0i64;
        let mut positions_count = 0i64;

        // Build mapping from original account_id -> hashed id
        let mut id_map = std::collections::HashMap::new();

        for acct_val in &envelope.accounts {
            let account = parse_account_from_json(acct_val, &envelope.source, &now);
            if let Some(a) = account {
                // Map original account_id to the hashed one
                if let Some(orig_id) = acct_val.get("account_id").and_then(|v| v.as_str()) {
                    id_map.insert(orig_id.to_string(), a.id.clone());
                }
                self.upsert_account(&a)?;
                self.insert_balance_snapshot(&BalanceSnapshot {
                    id: None,
                    source: envelope.source.clone(),
                    account_id: a.id.clone(),
                    balance: a.balance,
                    timestamp: now.clone(),
                })?;
                accounts_count += 1;
            }
        }

        for txn_val in &envelope.transactions {
            let txn = parse_transaction_from_json(txn_val, &envelope.source);
            if let Some(mut t) = txn {
                // Remap account_id if needed
                if let Some(mapped) = id_map.get(&t.account_id) {
                    t.account_id = mapped.clone();
                }
                self.upsert_transaction(&t)?;
                transactions_count += 1;
            }
        }

        for pos_val in &envelope.positions {
            let pos = parse_position_from_json(pos_val, &envelope.source, &now);
            if let Some(mut p) = pos {
                if let Some(mapped) = id_map.get(&p.account_id) {
                    p.account_id = mapped.clone();
                }
                self.upsert_position(&p)?;
                positions_count += 1;
            }
        }

        Ok((accounts_count, transactions_count, positions_count))
    }

    // ── Export ──

    pub fn export_envelope(&self, source: Option<&str>) -> Result<SyncEnvelope> {
        let accounts = self.list_accounts(source, None)?;
        let transactions = self.list_transactions(source, None, None, None)?;
        let positions = self.list_positions(source)?;

        Ok(SyncEnvelope {
            source: source.unwrap_or("all").to_string(),
            status: Some("ok".to_string()),
            error: None,
            accounts: accounts
                .iter()
                .map(|a| serde_json::to_value(a).unwrap_or_default())
                .collect(),
            transactions: transactions
                .iter()
                .map(|t| serde_json::to_value(t).unwrap_or_default())
                .collect(),
            positions: positions
                .iter()
                .map(|p| serde_json::to_value(p).unwrap_or_default())
                .collect(),
            balance_history: vec![],
        })
    }

    // ── Balances summary ──

    pub fn balances_summary(&self) -> Result<Vec<serde_json::Value>> {
        let mut stmt = self.conn.prepare(
            "SELECT source, account_type, SUM(balance) as total, COUNT(*) as count
             FROM accounts GROUP BY source, account_type ORDER BY source, account_type",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "source": row.get::<_, String>(0)?,
                "account_type": row.get::<_, String>(1)?,
                "total": row.get::<_, f64>(2)?,
                "count": row.get::<_, i64>(3)?,
            }))
        })?;

        let mut results = vec![];
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}

// ── JSON parsing helpers ──

fn make_id(source: &str, key: &str) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(format!("{source}:{key}"));
    format!("{:x}", hasher.finalize())[..16].to_string()
}

fn parse_account_from_json(val: &serde_json::Value, source: &str, now: &str) -> Option<Account> {
    let name = val
        .get("account_name")
        .or_else(|| val.get("name"))
        .and_then(|v| v.as_str())?
        .to_string();
    let balance = val.get("balance").and_then(|v| v.as_f64())?;
    let account_id = val
        .get("account_id")
        .and_then(|v| v.as_str())
        .unwrap_or(&name);
    let id = make_id(source, account_id);

    let account_type = val
        .get("account_type")
        .and_then(|v| v.as_str())
        .unwrap_or("other")
        .to_string();

    Some(Account {
        id,
        source: source.to_string(),
        name,
        account_type,
        balance,
        available_credit: val.get("available_credit").and_then(|v| v.as_f64()),
        day_change: val.get("day_change").and_then(|v| v.as_f64()),
        day_change_percent: val.get("day_change_percent").and_then(|v| v.as_f64()),
        last_synced: now.to_string(),
        metadata: Some(val.clone()),
    })
}

fn parse_transaction_from_json(val: &serde_json::Value, source: &str) -> Option<Transaction> {
    let description = val.get("description").and_then(|v| v.as_str())?.to_string();
    let amount = val.get("amount").and_then(|v| v.as_f64())?;
    let date = val
        .get("date")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let txn_id = val
        .get("txn_id")
        .or_else(|| val.get("id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| make_id(source, &format!("{date}:{description}:{amount}")));

    let account_id = val
        .get("account_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    Some(Transaction {
        id: txn_id,
        source: source.to_string(),
        account_id,
        date,
        description,
        amount,
        category: val
            .get("category")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        status: val
            .get("status")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        is_recurring: val
            .get("is_recurring")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        recurring_group: val
            .get("recurring_group")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        metadata: Some(val.clone()),
    })
}

fn parse_position_from_json(val: &serde_json::Value, source: &str, now: &str) -> Option<Position> {
    let symbol = val.get("symbol").and_then(|v| v.as_str())?.to_string();
    let market_value = val.get("market_value").and_then(|v| v.as_f64())?;
    let account_id = val
        .get("account_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let position_id = val
        .get("position_id")
        .or_else(|| val.get("id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| make_id(source, &format!("{account_id}:{symbol}")));

    Some(Position {
        id: position_id,
        source: source.to_string(),
        account_id,
        symbol,
        description: val
            .get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        quantity: val.get("quantity").and_then(|v| v.as_f64()),
        last_price: val.get("last_price").and_then(|v| v.as_f64()),
        market_value,
        day_gain: val
            .get("day_gain")
            .and_then(|v| v.as_f64())
            .or_else(|| val.get("days_gain").and_then(|v| v.as_f64())),
        total_gain: val.get("total_gain").and_then(|v| v.as_f64()),
        total_gain_percent: val.get("total_gain_percent").and_then(|v| v.as_f64()),
        metadata: Some(val.clone()),
        last_synced: now.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Database {
        Database::open_memory().unwrap()
    }

    #[test]
    fn test_upsert_account() {
        let db = test_db();
        let account = Account {
            id: "test1".to_string(),
            source: "schwab".to_string(),
            name: "Checking".to_string(),
            account_type: "checking".to_string(),
            balance: 5000.0,
            available_credit: None,
            day_change: Some(10.0),
            day_change_percent: Some(0.2),
            last_synced: "2026-01-01T00:00:00Z".to_string(),
            metadata: None,
        };
        db.upsert_account(&account).unwrap();

        let accounts = db.list_accounts(None, None).unwrap();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].name, "Checking");
        assert_eq!(accounts[0].balance, 5000.0);
    }

    #[test]
    fn test_upsert_replaces() {
        let db = test_db();
        let mut account = Account {
            id: "test1".to_string(),
            source: "schwab".to_string(),
            name: "Checking".to_string(),
            account_type: "checking".to_string(),
            balance: 5000.0,
            available_credit: None,
            day_change: None,
            day_change_percent: None,
            last_synced: "2026-01-01T00:00:00Z".to_string(),
            metadata: None,
        };
        db.upsert_account(&account).unwrap();

        account.balance = 6000.0;
        db.upsert_account(&account).unwrap();

        let accounts = db.list_accounts(None, None).unwrap();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].balance, 6000.0);
    }

    #[test]
    fn test_list_accounts_filter_source() {
        let db = test_db();
        for (id, source) in [("a1", "schwab"), ("a2", "chase")] {
            db.upsert_account(&Account {
                id: id.to_string(),
                source: source.to_string(),
                name: format!("{source} account"),
                account_type: "checking".to_string(),
                balance: 1000.0,
                available_credit: None,
                day_change: None,
                day_change_percent: None,
                last_synced: "now".to_string(),
                metadata: None,
            })
            .unwrap();
        }

        let schwab = db.list_accounts(Some("schwab"), None).unwrap();
        assert_eq!(schwab.len(), 1);
        assert_eq!(schwab[0].source, "schwab");
    }

    #[test]
    fn test_upsert_transaction() {
        let db = test_db();
        db.upsert_account(&Account {
            id: "acct1".to_string(),
            source: "chase".to_string(),
            name: "Freedom".to_string(),
            account_type: "credit".to_string(),
            balance: -500.0,
            available_credit: Some(4500.0),
            day_change: None,
            day_change_percent: None,
            last_synced: "now".to_string(),
            metadata: None,
        })
        .unwrap();

        let txn = Transaction {
            id: "tx1".to_string(),
            source: "chase".to_string(),
            account_id: "acct1".to_string(),
            date: "2026-03-14".to_string(),
            description: "COSTCO".to_string(),
            amount: -125.50,
            category: Some("Groceries".to_string()),
            status: Some("posted".to_string()),
            is_recurring: false,
            recurring_group: None,
            metadata: None,
        };
        db.upsert_transaction(&txn).unwrap();

        let txns = db
            .list_transactions(Some("chase"), None, None, None)
            .unwrap();
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].description, "COSTCO");
    }

    #[test]
    fn test_sync_log() {
        let db = test_db();
        let log_id = db.log_sync_start("schwab").unwrap();
        db.log_sync_finish(log_id, "ok", 3, 10, 5, None).unwrap();

        let entries = db.list_sync_log(None, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, "ok");
        assert_eq!(entries[0].accounts_synced, 3);
    }

    #[test]
    fn test_import_export_roundtrip() {
        let db = test_db();
        let envelope = SyncEnvelope {
            source: "schwab".to_string(),
            status: Some("ok".to_string()),
            error: None,
            accounts: vec![serde_json::json!({
                "account_id": "abc",
                "account_name": "Checking",
                "account_type": "checking",
                "balance": 5000.0,
            })],
            transactions: vec![],
            positions: vec![],
            balance_history: vec![],
        };

        let (a, t, p) = db.import_envelope(&envelope).unwrap();
        assert_eq!(a, 1);
        assert_eq!(t, 0);
        assert_eq!(p, 0);

        let exported = db.export_envelope(Some("schwab")).unwrap();
        assert_eq!(exported.accounts.len(), 1);
    }

    #[test]
    fn test_make_id_deterministic() {
        let id1 = make_id("schwab", "checking_1234");
        let id2 = make_id("schwab", "checking_1234");
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 16);
    }

    #[test]
    fn test_balances_summary() {
        let db = test_db();
        for (id, source, at, bal) in [
            ("a1", "schwab", "brokerage", 100_000.0),
            ("a2", "schwab", "checking", 5_000.0),
            ("a3", "chase", "credit", -1_000.0),
        ] {
            db.upsert_account(&Account {
                id: id.to_string(),
                source: source.to_string(),
                name: id.to_string(),
                account_type: at.to_string(),
                balance: bal,
                available_credit: None,
                day_change: None,
                day_change_percent: None,
                last_synced: "now".to_string(),
                metadata: None,
            })
            .unwrap();
        }

        let summary = db.balances_summary().unwrap();
        assert_eq!(summary.len(), 3);
    }
}
