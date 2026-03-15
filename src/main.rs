mod config;
mod credentials;
mod db;
mod pretty;
mod runner;
mod types;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::io::{self, Read, Write};

#[derive(Parser)]
#[command(name = "till", version, about = "Personal finance CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output as JSON instead of tables
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Sync data from finance sources
    Sync {
        /// Source to sync (syncs all enabled if omitted)
        #[arg(long)]
        source: Option<String>,
    },
    /// List accounts
    Accounts {
        #[arg(long)]
        source: Option<String>,
        #[arg(long, alias = "type")]
        account_type: Option<String>,
        /// Pretty-print as table
        #[arg(long)]
        pretty: bool,
    },
    /// List transactions
    Transactions {
        #[arg(long)]
        source: Option<String>,
        #[arg(long, alias = "type")]
        account_type: Option<String>,
        /// Show transactions from last N days
        #[arg(long)]
        days: Option<u32>,
        #[arg(long)]
        category: Option<String>,
        #[arg(long)]
        pretty: bool,
    },
    /// List positions
    Positions {
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        pretty: bool,
    },
    /// Show balance summary
    Balances {
        #[arg(long)]
        pretty: bool,
    },
    /// Show balance history for an account
    History {
        /// Account ID
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        pretty: bool,
    },
    /// List discovered scraper sources
    Sources,
    /// Manage credentials
    Creds {
        #[command(subcommand)]
        action: CredsAction,
    },
    /// Export data as JSON
    Export {
        #[arg(long)]
        source: Option<String>,
        #[arg(long, default_value = "json")]
        format: String,
    },
    /// Import data from JSON (stdin)
    Import {
        /// Override source name
        #[arg(long)]
        source: Option<String>,
    },
    /// Generate a new scraper plugin skeleton
    Scaffold {
        /// Name of the new source
        name: String,
    },
    /// Test a scraper interactively
    Test {
        #[arg(long)]
        source: String,
        /// Force visible browser window
        #[arg(long)]
        headful: bool,
        /// Pause after login for manual inspection
        #[arg(long)]
        pause: bool,
        /// Save page HTML for offline development
        #[arg(long)]
        save_html: bool,
        /// Replay saved HTML instead of live scraping
        #[arg(long)]
        replay: Option<String>,
    },
    /// Explore a bank website via existing Chrome session (no login needed)
    Explore {
        /// URL to explore
        #[arg(long)]
        url: String,
        /// Label for output files
        #[arg(long)]
        label: Option<String>,
        /// Interactive multi-page capture mode
        #[arg(long)]
        interactive: bool,
    },
    /// Show sync log
    Log {
        #[arg(long)]
        source: Option<String>,
        /// Number of entries to show
        #[arg(long, default_value = "20")]
        limit: usize,
        #[arg(long)]
        pretty: bool,
    },
}

#[derive(Subcommand)]
enum CredsAction {
    /// Set credentials for a source
    Set {
        #[arg(long)]
        source: String,
    },
    /// Show if credentials exist for a source
    Get {
        #[arg(long)]
        source: String,
    },
    /// Delete credentials for a source
    Delete {
        #[arg(long)]
        source: String,
    },
}

fn output<W: Write>(
    w: &mut W,
    value: &serde_json::Value,
    pretty_flag: bool,
    json_flag: bool,
) -> Result<()> {
    if json_flag {
        writeln!(w, "{}", serde_json::to_string_pretty(value)?)?;
    } else if pretty_flag {
        pretty::render(w, value)?;
    } else {
        writeln!(w, "{}", serde_json::to_string(value)?)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let db_path = config::db_path()?;
    let db = db::Database::open(&db_path)?;
    let mut stdout = io::stdout();

    match cli.command {
        Commands::Sync { source } => {
            let sources = if let Some(ref s) = source {
                vec![s.clone()]
            } else {
                let config = config::load_config()?;
                config
                    .sources
                    .iter()
                    .filter(|(_, v)| v.enabled)
                    .map(|(k, _)| k.clone())
                    .collect()
            };

            if sources.is_empty() {
                eprintln!("No sources configured. Add sources to ~/.config/till/config.toml");
                return Ok(());
            }

            for source_name in &sources {
                eprintln!("Syncing {source_name}...");
                let log_id = db.log_sync_start(source_name)?;

                match runner::run_scraper(source_name, None, false).await {
                    Ok(envelope) => {
                        let (a, t, p) = db.import_envelope(&envelope)?;
                        db.log_sync_finish(log_id, "ok", a, t, p, None)?;
                        eprintln!("  {source_name}: {a} accounts, {t} transactions, {p} positions");
                    }
                    Err(e) => {
                        db.log_sync_finish(log_id, "error", 0, 0, 0, Some(&e.to_string()))?;
                        eprintln!("  {source_name}: error - {e}");
                    }
                }
            }
        }

        Commands::Accounts {
            source,
            account_type,
            pretty,
        } => {
            let accounts = db.list_accounts(source.as_deref(), account_type.as_deref())?;
            let value = serde_json::to_value(&accounts)?;
            output(&mut stdout, &value, pretty, cli.json)?;
        }

        Commands::Transactions {
            source,
            account_type,
            days,
            category,
            pretty,
        } => {
            let txns = db.list_transactions(
                source.as_deref(),
                account_type.as_deref(),
                days,
                category.as_deref(),
            )?;
            let value = serde_json::to_value(&txns)?;
            output(&mut stdout, &value, pretty, cli.json)?;
        }

        Commands::Positions { source, pretty } => {
            let positions = db.list_positions(source.as_deref())?;
            let value = serde_json::to_value(&positions)?;
            output(&mut stdout, &value, pretty, cli.json)?;
        }

        Commands::Balances { pretty } => {
            let summary = db.balances_summary()?;
            let value = serde_json::Value::Array(summary);
            output(&mut stdout, &value, pretty, cli.json)?;
        }

        Commands::History { account_id, pretty } => {
            let history = db.list_balance_history(&account_id)?;
            let value = serde_json::to_value(&history)?;
            output(&mut stdout, &value, pretty, cli.json)?;
        }

        Commands::Sources => match runner::list_sources().await {
            Ok(sources) => {
                let config = config::load_config()?;
                let items: Vec<serde_json::Value> = sources
                    .iter()
                    .map(|s| {
                        let enabled = config.sources.get(s).map(|c| c.enabled).unwrap_or(false);
                        let has_creds = credentials::has_credentials(s);
                        serde_json::json!({
                            "source": s,
                            "enabled": enabled,
                            "credentials": has_creds,
                        })
                    })
                    .collect();
                let value = serde_json::Value::Array(items);
                output(&mut stdout, &value, true, cli.json)?;
            }
            Err(e) => {
                eprintln!("Failed to list sources: {e}");
            }
        },

        Commands::Creds { action } => match action {
            CredsAction::Set { source } => {
                eprint!("Username: ");
                io::stderr().flush()?;
                let mut username = String::new();
                io::stdin().read_line(&mut username)?;
                let username = username.trim().to_string();

                eprint!("Password: ");
                io::stderr().flush()?;
                let mut password = String::new();
                io::stdin().read_line(&mut password)?;
                let password = password.trim().to_string();

                credentials::set_credentials(&source, &username, &password)?;
                eprintln!("Credentials saved for {source}");
            }
            CredsAction::Get { source } => {
                let has = credentials::has_credentials(&source);
                let value = serde_json::json!({
                    "source": source,
                    "has_credentials": has,
                });
                output(&mut stdout, &value, true, cli.json)?;
            }
            CredsAction::Delete { source } => {
                credentials::delete_credentials(&source)?;
                eprintln!("Credentials deleted for {source}");
            }
        },

        Commands::Export { source, format: _ } => {
            let envelope = db.export_envelope(source.as_deref())?;
            writeln!(stdout, "{}", serde_json::to_string_pretty(&envelope)?)?;
        }

        Commands::Import { source } => {
            let mut input = String::new();
            io::stdin().read_to_string(&mut input)?;
            let mut envelope: types::SyncEnvelope = serde_json::from_str(&input)?;
            if let Some(s) = source {
                envelope.source = s;
            }
            let (a, t, p) = db.import_envelope(&envelope)?;
            eprintln!("Imported: {a} accounts, {t} transactions, {p} positions");
        }

        Commands::Scaffold { name } => {
            scaffold_plugin(&name)?;
        }

        Commands::Test {
            source,
            headful,
            pause,
            save_html,
            replay,
        } => {
            eprintln!("Testing {source}...");
            let envelope =
                runner::run_test(&source, headful, pause, save_html, replay.as_deref()).await?;
            let value = serde_json::to_value(&envelope)?;
            output(&mut stdout, &value, true, cli.json)?;
        }

        Commands::Explore {
            url,
            label,
            interactive,
        } => {
            let scrapers_dir = runner::find_scrapers_dir()?;
            let uv = which::which("uv").unwrap_or_else(|_| std::path::PathBuf::from("uv"));
            let mut cmd = tokio::process::Command::new(&uv);
            cmd.arg("run")
                .arg("--directory")
                .arg(&scrapers_dir)
                .arg("python")
                .arg("-m")
                .arg("till_scrapers.explore")
                .arg("--url")
                .arg(&url);
            if let Some(ref l) = label {
                cmd.arg("--label").arg(l);
            }
            if interactive {
                cmd.arg("--interactive");
            }
            let status = cmd.status().await?;
            if !status.success() {
                eprintln!(
                    "Explore failed. Make sure Chrome is running with --remote-debugging-port=9222"
                );
            }
        }

        Commands::Log {
            source,
            limit,
            pretty,
        } => {
            let entries = db.list_sync_log(source.as_deref(), limit)?;
            let value = serde_json::to_value(&entries)?;
            output(&mut stdout, &value, pretty, cli.json)?;
        }
    }

    Ok(())
}

fn scaffold_plugin(name: &str) -> Result<()> {
    let scrapers_dir = runner::find_scrapers_dir()?;
    let plugin_dir = scrapers_dir.join("till_scrapers").join(name);

    if plugin_dir.exists() {
        anyhow::bail!("Plugin directory already exists: {}", plugin_dir.display());
    }

    std::fs::create_dir_all(&plugin_dir)?;

    // manifest.json
    let manifest = serde_json::json!({
        "name": name,
        "display_name": name.chars().next().map(|c| c.to_uppercase().to_string()).unwrap_or_default() + &name[1..],
        "account_types": ["brokerage"],
        "provides": ["accounts"],
        "requires_headful": true,
        "config_schema": {}
    });
    std::fs::write(
        plugin_dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;

    // __init__.py
    std::fs::write(plugin_dir.join("__init__.py"), "")?;

    // scraper.py
    let class_name = format!(
        "{}Scraper",
        name.chars()
            .next()
            .map(|c| c.to_uppercase().to_string())
            .unwrap_or_default()
            + &name[1..]
    );
    let scraper_py = format!(
        r#"from till_scrapers.base import BaseScraper


class {class_name}(BaseScraper):
    """{display_name} scraper — generated by `till scaffold`."""

    LOGIN_URL = "https://www.{name}.com/"  # TODO: update

    async def extract(self, page) -> dict:
        """Extract account data from the page.

        Override this method with your extraction logic.
        """
        # TODO: implement extraction
        await page.screenshot(path="/tmp/till_{name}_debug.png")

        return {{
            "status": "ok",
            "source": "{name}",
            "accounts": [],
            "transactions": [],
            "positions": [],
            "balance_history": [],
        }}
"#,
        class_name = class_name,
        display_name = manifest["display_name"].as_str().unwrap_or(name),
        name = name,
    );
    std::fs::write(plugin_dir.join("scraper.py"), scraper_py)?;

    eprintln!("Scaffolded plugin: {}", plugin_dir.display());
    eprintln!("  - manifest.json");
    eprintln!("  - scraper.py");
    eprintln!("\nEdit scraper.py to implement your extraction logic.");
    eprintln!("Test with: till test --source {name}");

    Ok(())
}
