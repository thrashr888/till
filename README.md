# till

Personal finance CLI — a self-hosted Plaid.

Scrapes your bank and brokerage accounts using browser automation, stores everything locally in SQLite, and gives you a single command-line interface to query across all of them.

## Install

```sh
cargo install till-cli
```

Or build from source:

```sh
git clone https://github.com/thrashr888/till
cd till
cargo install --path .
```

Requires [uv](https://docs.astral.sh/uv/) for Python scraper execution and Chromium for browser automation.

## Quick Start

```sh
# Set credentials (stored in macOS Keychain)
till creds set --source schwab

# Sync your accounts
till sync --source schwab

# View accounts
till accounts --pretty

# View recent transactions
till transactions --days 30 --pretty

# View positions
till positions --pretty
```

## Sources

| Source | Status | Provides |
|--------|--------|----------|
| Charles Schwab | Working | Accounts, transactions, positions |
| E*Trade | Working | Accounts, positions |
| Chase | Working | Credit cards, transactions |
| American Express | Skeleton | Credit cards, transactions |
| Bank of America | Skeleton | Accounts, transactions |
| Fidelity | Skeleton | Accounts, positions |
| Morgan Stanley | Skeleton | Accounts, positions |
| Wells Fargo | Skeleton | Accounts, transactions |

List discovered sources:

```sh
till sources
```

## Usage

### Sync

```sh
till sync                          # All enabled sources
till sync --source schwab          # Single source
```

### Query

```sh
till accounts [--source X] [--type brokerage] [--pretty]
till transactions [--source X] [--days 30] [--category groceries] [--pretty]
till positions [--source X] [--pretty]
till balances [--pretty]
till history --account-id X [--pretty]
```

### Credentials

```sh
till creds set --source schwab     # Store username/password in Keychain
till creds get --source schwab     # Check if credentials exist
till creds delete --source schwab  # Remove credentials
```

Environment variable fallback: `TILL_SCHWAB_USERNAME` / `TILL_SCHWAB_PASSWORD`.

### Import / Export

```sh
till export > backup.json
till export --source schwab > schwab.json
till import < backup.json
```

JSON format matches scraper output — you can manually create JSON files for banks without scrapers.

### Sync Log

```sh
till log                           # Recent sync history
till log --source schwab           # Filter by source
```

## Adding a New Source

```sh
till scaffold fidelity
```

Creates `scrapers/till_scrapers/fidelity/` with:
- `manifest.json` — plugin metadata
- `scraper.py` — scraper class extending `BaseScraper`

### Development Workflow

```sh
# Run scraper without saving to DB
till test --source fidelity

# Visible browser for debugging
till test --source fidelity --headful

# Pause after login for DOM inspection
till test --source fidelity --pause

# Save page HTML for offline selector iteration
till test --source fidelity --save-html

# Replay saved HTML (no re-authentication needed)
till test --source fidelity --replay /tmp/till_fidelity_page.html
```

## Config

`~/.config/till/config.toml`:

```toml
[schwab]
enabled = true
transaction_account = "...1234"

[chase]
enabled = true

[browser]
headless = false
timeout = 600
```

## Architecture

Rust handles CLI, config, credentials, SQLite storage, and output formatting. Python handles browser automation via Playwright with stealth plugins. They communicate via JSON on stdout.

```
till (Rust CLI)
  ├── config.toml loader
  ├── macOS Keychain credentials
  ├── SQLite with upsert semantics
  ├── JSON import/export
  └── pretty table renderer

scrapers (Python/Playwright)
  ├── base.py — shared browser setup + replay mode
  ├── registry.py — manifest.json plugin discovery
  └── <source>/scraper.py — per-bank extraction logic
```

Rust has zero per-bank knowledge. Sources self-register via `manifest.json`. The `till-scrape --list` command discovers available plugins at runtime.

## Storage

SQLite at `~/.config/till/till.db`. All syncs use `INSERT ... ON CONFLICT DO UPDATE` so re-running is always safe.

## License

MIT
