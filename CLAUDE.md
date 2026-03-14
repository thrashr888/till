# CLAUDE.md

## Project Overview

**till** is a personal finance CLI — a self-hosted Plaid — that scrapes data from financial institutions (Schwab, E*Trade, Chase) and stores it locally in SQLite.

## Architecture

Rust CLI (`till`) handles config/credentials/storage/output. Python scrapers handle browser automation (Playwright + stealth). They communicate via JSON on stdout.

```
till/
  src/           # Rust CLI
    main.rs      # clap CLI dispatch
    pretty.rs    # Table/KV renderer
    config.rs    # ~/.config/till/config.toml
    credentials.rs  # macOS Keychain via `security` CLI
    runner.rs    # uv subprocess runner (source-agnostic)
    db.rs        # SQLite with upsert semantics
    types.rs     # Account, Transaction, Position, etc.
  scrapers/      # Python scraper plugins
    till_scrapers/
      base.py    # Shared browser setup, replay mode
      registry.py # Plugin discovery via manifest.json
      runner.py  # CLI entry: --source <name> | --list
      schwab/    # Each source has manifest.json + scraper.py
      etrade/
      chase/
```

## Key Design Principles

- Rust has zero per-bank knowledge — only account *types* and data *types*
- Python plugins self-register via manifest.json
- SQLite for persistent storage with upsert semantics
- JSON export/import for portability
- Scrapers output JSON to stdout, progress to stderr

## Development Commands

```bash
cargo fmt -- --check && cargo clippy -- -D warnings && cargo test
till scaffold <name>     # Generate new scraper plugin
till test --source <name> --headful --pause
```

## Config

- Config: `~/.config/till/config.toml`
- Database: `~/.config/till/till.db`
- Credentials: macOS Keychain (`till.<source>.username`, `till.<source>.password`)
- Env fallback: `TILL_<SOURCE>_USERNAME`, `TILL_<SOURCE>_PASSWORD`
