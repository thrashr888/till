"""E*Trade scraper using pyetrade API (no browser needed).

Uses OAuth 1.0a via pyetrade for API access. Credentials loaded from
1Password CLI or environment variables. OAuth tokens persisted in
macOS Keychain (service: "till-etrade").

Usage:
    scraper = EtradeScraper()
    result = await scraper.scrape()
"""

import hashlib
import json
import logging
import os
import subprocess
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta

import keyring
import pyetrade
from requests_oauthlib import OAuth1Session

logger = logging.getLogger(__name__)

# Keychain config
KEYCHAIN_SERVICE = "till-etrade"
KEYCHAIN_TOKEN_KEY = "oauth_tokens"

# E*Trade tokens expire after 2 hours
TOKEN_MAX_AGE = 7000  # ~2h minus buffer

# API timeout for all calls
API_TIMEOUT = 30


def _op_read(ref: str) -> str | None:
    """Read a secret from 1Password CLI. Returns None if op is unavailable."""
    try:
        result = subprocess.run(
            ["op", "read", ref],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _load_credentials(sandbox: bool = True) -> tuple[str, str]:
    """Load E*Trade API key and secret from 1Password or env vars.

    Returns (consumer_key, consumer_secret).
    """
    if sandbox:
        key_env, secret_env = "ETRADE_SANDBOX_API_KEY", "ETRADE_SANDBOX_API_SECRET"
    else:
        key_env, secret_env = "ETRADE_PROD_API_KEY", "ETRADE_PROD_API_SECRET"

    key = None
    secret = None

    # Try 1Password first
    key_op_ref = os.environ.get(f"{key_env}_OP")
    secret_op_ref = os.environ.get(f"{secret_env}_OP")

    if key_op_ref:
        key = _op_read(key_op_ref)
    if secret_op_ref:
        secret = _op_read(secret_op_ref)

    # Fall back to plain env vars
    if not key:
        key = os.environ.get(key_env)
    if not secret:
        secret = os.environ.get(secret_env)

    if not key or not secret:
        raise RuntimeError(
            f"E*Trade API credentials not found. Set {key_env} and {secret_env} "
            f"env vars, or {key_env}_OP / {secret_env}_OP for 1Password refs."
        )

    return key, secret


@dataclass
class OAuthTokens:
    access_token: str
    access_token_secret: str
    timestamp: float

    @property
    def expired(self) -> bool:
        return (time.time() - self.timestamp) > TOKEN_MAX_AGE

    def to_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "access_token_secret": self.access_token_secret,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def from_dict(d: dict) -> "OAuthTokens":
        return OAuthTokens(
            access_token=d["access_token"],
            access_token_secret=d["access_token_secret"],
            timestamp=d["timestamp"],
        )


def _save_tokens(tokens: OAuthTokens) -> None:
    """Save OAuth tokens to macOS Keychain."""
    try:
        keyring.set_password(
            KEYCHAIN_SERVICE, KEYCHAIN_TOKEN_KEY, json.dumps(tokens.to_dict())
        )
    except Exception as e:
        logger.warning("Keychain save failed: %s", e)


def _load_tokens() -> OAuthTokens | None:
    """Load OAuth tokens from macOS Keychain."""
    try:
        token_json = keyring.get_password(KEYCHAIN_SERVICE, KEYCHAIN_TOKEN_KEY)
        if token_json:
            return OAuthTokens.from_dict(json.loads(token_json))
    except Exception as e:
        logger.debug("Keychain load failed: %s", e)
    return None


def _authenticate_interactive(consumer_key: str, consumer_secret: str) -> OAuthTokens:
    """Run the interactive OAuth browser flow. Returns tokens."""
    session = OAuth1Session(
        consumer_key, consumer_secret,
        callback_uri="oob", signature_type="AUTH_HEADER",
    )

    # Step 1: Get request token
    try:
        request_tokens = session.fetch_request_token(
            "https://api.etrade.com/oauth/request_token"
        )
    except Exception as exc:
        raise RuntimeError(
            f"E*Trade OAuth request token failed. Check your API keys. Error: {exc}"
        ) from exc

    req_token = request_tokens["oauth_token"]
    req_secret = request_tokens["oauth_token_secret"]

    authorize_url = (
        f"https://us.etrade.com/e/t/etws/authorize"
        f"?key={consumer_key}&token={req_token}"
    )

    print(f"\nOpening browser for E*Trade authorization...", file=sys.stderr)
    print(f"URL: {authorize_url}", file=sys.stderr)
    webbrowser.open(authorize_url)

    verifier = input("Enter verification code from E*Trade: ").strip()

    # Step 2: Exchange verifier for access tokens
    session = OAuth1Session(
        consumer_key, consumer_secret,
        resource_owner_key=req_token,
        resource_owner_secret=req_secret,
        signature_type="AUTH_HEADER",
    )
    session._client.client.verifier = verifier

    try:
        token_data = session.fetch_access_token(
            "https://api.etrade.com/oauth/access_token"
        )
    except Exception as exc:
        raise RuntimeError(
            f"E*Trade access token exchange failed. "
            f"The verification code may be expired. Error: {exc}"
        ) from exc

    tokens = OAuthTokens(
        access_token=token_data["oauth_token"],
        access_token_secret=token_data["oauth_token_secret"],
        timestamp=time.time(),
    )
    _save_tokens(tokens)
    return tokens


def _get_tokens(consumer_key: str, consumer_secret: str) -> OAuthTokens:
    """Load saved tokens or run interactive auth. Raises if non-interactive and no valid tokens."""
    tokens = _load_tokens()

    if tokens and not tokens.expired:
        logger.info("Loaded saved tokens (age: %.0fs)", time.time() - tokens.timestamp)
        return tokens

    # Tokens exist but expired -- try renewal before full re-auth
    if tokens:
        try:
            manager = pyetrade.ETradeAccessManager(
                client_key=consumer_key,
                client_secret=consumer_secret,
                resource_owner_key=tokens.access_token,
                resource_owner_secret=tokens.access_token_secret,
            )
            if manager.renew_access_token():
                renewed = OAuthTokens(
                    access_token=tokens.access_token,
                    access_token_secret=tokens.access_token_secret,
                    timestamp=time.time(),
                )
                _save_tokens(renewed)
                logger.info("Token renewed successfully")
                return renewed
        except Exception:
            logger.debug("Token renewal failed, will re-authenticate")

    # Non-interactive: cannot prompt for verifier
    if not sys.stdin.isatty():
        reason = "expired" if tokens else "missing"
        raise RuntimeError(
            f"E*Trade authentication required (tokens {reason}). "
            f"Run `till-scrape --source etrade` interactively to authenticate."
        )

    return _authenticate_interactive(consumer_key, consumer_secret)


def _ensure_list(value) -> list:
    """Normalize E*Trade API responses -- single dicts become [dict]."""
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return value
    return []


class EtradeScraper:
    """E*Trade API scraper using pyetrade. No browser needed."""

    def __init__(self, headless: bool = True):
        # headless param accepted for interface compat but ignored (no browser)
        sandbox_env = os.environ.get("ETRADE_SANDBOX", "").lower()
        self.sandbox = sandbox_env in ("1", "true", "yes")
        self.replay_file = None  # Not used, but runner may set it

    async def scrape(self, username=None, password=None) -> dict:
        """Fetch accounts, positions, and transactions from E*Trade API.

        Args are accepted for interface compat but ignored (uses OAuth, not username/password).
        """
        try:
            consumer_key, consumer_secret = _load_credentials(sandbox=self.sandbox)
        except RuntimeError as e:
            return {
                "status": "error",
                "source": "etrade",
                "error": str(e),
                "accounts": [],
                "transactions": [],
                "positions": [],
                "balance_history": [],
            }

        try:
            tokens = _get_tokens(consumer_key, consumer_secret)
        except RuntimeError as e:
            return {
                "status": "error",
                "source": "etrade",
                "error": str(e),
                "accounts": [],
                "transactions": [],
                "positions": [],
                "balance_history": [],
            }

        dev = self.sandbox
        accounts_api = pyetrade.ETradeAccounts(
            client_key=consumer_key,
            client_secret=consumer_secret,
            resource_owner_key=tokens.access_token,
            resource_owner_secret=tokens.access_token_secret,
            dev=dev,
        )

        # --- List accounts ---
        print("   Fetching E*Trade accounts via API...", file=sys.stderr)
        try:
            acct_resp = accounts_api.list_accounts(resp_format="json")
        except Exception as e:
            return {
                "status": "error",
                "source": "etrade",
                "error": f"Failed to list accounts: {e}",
                "accounts": [],
                "transactions": [],
                "positions": [],
                "balance_history": [],
            }

        raw_accounts = _ensure_list(
            acct_resp.get("AccountListResponse", {})
            .get("Accounts", {})
            .get("Account", [])
        )

        account_results = []
        all_positions = []
        all_transactions = []

        for acct in raw_accounts:
            if acct.get("accountStatus") == "CLOSED":
                continue

            account_id_key = acct.get("accountIdKey", "")
            account_name = acct.get("accountDesc", "").strip()
            account_type = self._map_account_type(
                acct.get("accountType", ""),
                acct.get("institutionType", ""),
                account_name,
            )

            # Use account_id_key as the stable identifier
            account_id = hashlib.md5(
                f"etrade_{account_id_key}".encode()
            ).hexdigest()[:16]

            # --- Get balance ---
            balance = 0.0
            day_change = None
            day_change_pct = None
            try:
                bal_resp = accounts_api.get_account_balance(
                    account_id_key=account_id_key,
                    real_time=True,
                    resp_format="json",
                )
                bal = bal_resp.get("BalanceResponse", {})
                computed = bal.get("Computed", {})
                rt = computed.get("RealTimeValues", {})
                balance = float(rt.get("totalAccountValue", 0))
                # Day change from real-time values
                day_change = float(rt.get("netMv", 0)) if rt.get("netMv") else None
                day_change_pct = float(rt.get("netMvPct", 0)) if rt.get("netMvPct") else None
            except Exception as e:
                print(f"   Balance error for {account_name}: {e}", file=sys.stderr)

            print(
                f"   {account_name} ({account_type}): ${balance:,.2f}",
                file=sys.stderr,
            )

            account_results.append({
                "account_id": account_id,
                "account_name": account_name,
                "account_type": account_type,
                "balance": balance,
                "day_change": day_change,
                "day_change_percent": day_change_pct,
            })

            # --- Get positions ---
            try:
                port_resp = accounts_api.get_account_portfolio(
                    account_id_key=account_id_key,
                    resp_format="json",
                )
                portfolios = _ensure_list(
                    port_resp.get("PortfolioResponse", {}).get("AccountPortfolio", [])
                )
                for portfolio in portfolios:
                    for pos in _ensure_list(portfolio.get("Position", [])):
                        symbol = (
                            pos.get("Product", {}).get("symbol", "")
                            or pos.get("symbolDescription", "")
                        )
                        if not symbol:
                            continue

                        quantity = float(pos.get("quantity", 0))
                        market_value = float(pos.get("marketValue", 0))
                        total_gain = float(pos.get("totalGain", 0))
                        day_gain = float(pos.get("daysGain", 0))
                        price_paid = float(pos.get("pricePaid", 0))

                        quick = pos.get("Quick", {})
                        last_price = float(quick.get("lastTrade", 0))

                        cost_basis = market_value - total_gain
                        total_gain_pct = (
                            (total_gain / cost_basis * 100) if cost_basis != 0 else 0.0
                        )

                        position_id = hashlib.md5(
                            f"{account_id}_{symbol}".encode()
                        ).hexdigest()[:16]

                        all_positions.append({
                            "position_id": position_id,
                            "account_id": account_id,
                            "symbol": symbol,
                            "description": pos.get("symbolDescription"),
                            "quantity": quantity,
                            "last_price": last_price,
                            "price_paid": price_paid,
                            "market_value": market_value,
                            "day_gain": day_gain,
                            "total_gain": total_gain,
                            "total_gain_percent": total_gain_pct,
                        })
            except Exception as e:
                print(f"   Positions error for {account_name}: {e}", file=sys.stderr)

            # --- Get transactions (last 30 days) ---
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                txn_resp = accounts_api.list_transactions(
                    account_id_key=account_id_key,
                    start_date=start_date,
                    end_date=end_date,
                    count=50,
                    resp_format="json",
                )
                raw_txns = _ensure_list(
                    txn_resp.get("TransactionListResponse", {})
                    .get("Transaction", [])
                )
                for txn in raw_txns:
                    txn_id = str(txn.get("transactionId", ""))
                    brokerage = txn.get("brokerage", {})

                    # Product info may be nested
                    product = brokerage.get("product", [])
                    symbol = ""
                    qty = 0.0
                    price = 0.0
                    if product:
                        first = product[0] if isinstance(product, list) else product
                        symbol = first.get("symbol", "")
                        qty = float(first.get("quantity", 0))
                        price = float(first.get("price", 0))

                    all_transactions.append({
                        "transaction_id": txn_id,
                        "account_id": account_id,
                        "date": txn.get("transactionDate"),
                        "type": txn.get("transactionType", ""),
                        "description": brokerage.get("displaySymbol", txn.get("description", "")),
                        "symbol": symbol,
                        "quantity": qty,
                        "price": price,
                        "amount": float(txn.get("amount", 0)),
                    })
            except Exception as e:
                # Transactions may not be available for all account types
                logger.debug("Transactions error for %s: %s", account_name, e)

        print(
            f"   Found {len(account_results)} accounts, "
            f"{len(all_positions)} positions, "
            f"{len(all_transactions)} transactions",
            file=sys.stderr,
        )

        return {
            "status": "ok",
            "source": "etrade",
            "accounts": account_results,
            "transactions": all_transactions,
            "positions": all_positions,
            "balance_history": [],
        }

    @staticmethod
    def _map_account_type(acct_type: str, inst_type: str, name: str) -> str:
        """Map E*Trade account type strings to standard types."""
        t = (acct_type + " " + name).lower()
        if "401" in t:
            return "401k"
        if any(w in t for w in ["ira", "roth", "retirement"]):
            return "ira"
        if "checking" in t:
            return "checking"
        if "saving" in t:
            return "savings"
        return "brokerage"
