"""E*Trade scraper.

Strategy: Login via Playwright, then intercept E*Trade's internal API calls
with page.on("response") to capture account and position data. Falls back
to DOM extraction if API interception doesn't produce results.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


class EtradeScraper(BaseScraper):
    LOGIN_URL = "https://us.etrade.com/etx/hw/v2/accountshome"

    def __init__(self, headless: bool = True):
        super().__init__(headless=headless)

        include_str = os.environ.get("TILL_ETRADE_INCLUDE_ACCOUNTS", "")
        self.include_accounts = (
            [s.strip() for s in include_str.split(",") if s.strip()]
            if include_str
            else []
        )

    async def navigate_and_login(self, page, username: str, password: str):
        # Check for active session first by navigating to the accounts page
        print("   Checking for active session...", file=sys.stderr)
        try:
            await page.goto(
                self.LOGIN_URL,
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.wait_for_timeout(3000)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
        except Exception as e:
            print(f"   Session check navigation error: {e}", file=sys.stderr)

        current_url = page.url
        print(f"   Session check URL: {current_url}", file=sys.stderr)

        # If we're not on a login page, session is active
        if "login" not in current_url.lower() and "logon" not in current_url.lower():
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — need to login
        print("   Session expired, logging in...", file=sys.stderr)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source etrade` "
                "or set TILL_USERNAME/TILL_PASSWORD env vars."
            )

        print("   Auto-filling login credentials...", file=sys.stderr)
        try:
            await page.wait_for_timeout(2000)

            # Fill username
            print("   Entering username...", file=sys.stderr)
            await page.wait_for_selector("#USER", timeout=5000)
            await page.fill("#USER", username)
            await page.wait_for_timeout(500)

            # Fill password
            print("   Entering password...", file=sys.stderr)
            await page.wait_for_selector("#password", timeout=5000)
            await page.fill("#password", password)
            await page.wait_for_timeout(500)

            # Click login button
            print("   Clicking login...", file=sys.stderr)
            await page.wait_for_selector("#mfaLogonButton", timeout=5000)
            await page.click("#mfaLogonButton")

            # Wait for navigation after login
            print("   Waiting for login to complete...", file=sys.stderr)
            await page.wait_for_timeout(5000)

            # Check if we landed on accounts page or need 2FA
            current_url = page.url
            if "login" in current_url.lower() or "logon" in current_url.lower():
                print("   May need 2FA, waiting...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_etrade_2fa.png")
                try:
                    await page.wait_for_url(
                        "**/etx/hw/**", timeout=120000
                    )
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await page.screenshot(path="/tmp/till_etrade_login.png")
                    raise Exception(
                        f"Login failed. URL: {page.url}. "
                        "Check /tmp/till_etrade_login.png"
                    )
            else:
                print("   Login successful!", file=sys.stderr)

        except Exception as e:
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_etrade_login.png")
            raise

        await page.wait_for_timeout(2000)

    async def extract(self, page) -> dict:
        """Extract accounts and positions using E*Trade's internal APIs."""

        # Step 1: Set up API interception
        api_responses = {}

        async def capture_api(response):
            url = response.url
            if response.status == 200 and (
                "/api/" in url
                or "/rest/" in url
                or "/services/" in url
                or "portfolio" in url.lower()
                or "account" in url.lower()
                or "position" in url.lower()
            ):
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body = await response.text()
                    if body and body.strip() and body.strip()[0] in "{[":
                        api_responses[url] = json.loads(body)
                        print(
                            f"   API [{response.status}]: {url[:120]}",
                            file=sys.stderr,
                        )
                except Exception:
                    pass

        page.on("response", capture_api)

        # Step 2: Navigate to accounts page to trigger API calls
        print("   Loading account summary...", file=sys.stderr)
        await page.goto(
            self.LOGIN_URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)

        await page.screenshot(path="/tmp/till_etrade_accounts.png")

        # Step 3: Parse accounts from intercepted API responses
        accounts = []
        for url, data in api_responses.items():
            if "account" in url.lower():
                accounts_from_api = self._parse_accounts_from_api(data)
                if accounts_from_api:
                    accounts.extend(accounts_from_api)

        # Fallback: DOM extraction if API didn't produce accounts
        if not accounts or all(a["balance"] == 0 for a in accounts):
            print(
                "   API didn't return account data, falling back to DOM",
                file=sys.stderr,
            )
            dom_accounts = await self._extract_accounts_dom(page)
            if dom_accounts:
                accounts = dom_accounts

        # Filter by include_accounts
        if self.include_accounts:
            before = len(accounts)
            accounts = [
                a
                for a in accounts
                if a.get("account_suffix") in self.include_accounts
                or any(inc in a.get("name", "") for inc in self.include_accounts)
            ]
            skipped = before - len(accounts)
            if skipped:
                print(
                    f"   Filtered to {len(accounts)} accounts (skipped {skipped})",
                    file=sys.stderr,
                )

        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Step 4: Navigate to positions page to capture position API calls
        positions = []
        api_responses.clear()

        print("   Loading positions page...", file=sys.stderr)
        positions_url = (
            "https://us.etrade.com/etx/pxy/portfolios/positions"
            "?_formtarget=portfoliolist"
        )
        await page.goto(
            positions_url,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)

        await page.screenshot(path="/tmp/till_etrade_positions.png")

        # Parse positions from intercepted API responses
        for url, data in api_responses.items():
            if "position" in url.lower() or "portfolio" in url.lower():
                positions_from_api = self._parse_positions_from_api(data, accounts)
                if positions_from_api:
                    positions.extend(positions_from_api)

        # Fallback: DOM/text extraction if API didn't produce positions
        if not positions:
            print(
                "   API didn't return position data, falling back to DOM",
                file=sys.stderr,
            )
            target_account = accounts[0] if accounts else None
            if target_account:
                account_id = hashlib.md5(
                    target_account["name"].encode()
                ).hexdigest()[:16]
                content = await page.inner_text("body")
                positions = self._parse_positions_from_text(
                    content, account_id, target_account["name"]
                )

        print(f"   Found {len(positions)} positions", file=sys.stderr)

        # Save API dump for debugging
        if api_responses:
            debug_path = "/tmp/till_etrade_api_dump.json"
            try:
                with open(debug_path, "w") as f:
                    json.dump(api_responses, f, indent=2, default=str)
                print(f"   Saved API dump to {debug_path}", file=sys.stderr)
            except Exception:
                pass

        # Build results
        account_results = []
        for acct in accounts:
            suffix = acct.get("account_suffix", "")
            id_key = f"etrade_{suffix}" if suffix else acct["name"]
            account_id = hashlib.md5(id_key.encode()).hexdigest()[:16]
            account_results.append(
                {
                    "account_id": account_id,
                    "account_name": (
                        f"{acct['name']} ...{suffix}" if suffix else acct["name"]
                    ),
                    "account_type": acct.get("type", "brokerage"),
                    "balance": acct["balance"],
                    "day_change": acct.get("day_change"),
                    "day_change_percent": acct.get("day_change_percent"),
                }
            )

        position_results = []
        for pos in positions:
            position_results.append(
                {
                    "position_id": pos["position_id"],
                    "account_id": pos["account_id"],
                    "symbol": pos["symbol"],
                    "description": pos.get("description"),
                    "quantity": pos.get("quantity"),
                    "last_price": pos.get("last_price"),
                    "market_value": pos.get("market_value"),
                    "day_gain": pos.get("day_gain"),
                    "total_gain": pos.get("total_gain"),
                    "total_gain_percent": pos.get("total_gain_percent"),
                }
            )

        return {
            "status": "ok",
            "source": "etrade",
            "accounts": account_results,
            "transactions": [],
            "positions": position_results,
            "balance_history": [],
        }

    # ── API response parsers ────────────────────────────────────────────

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from E*Trade's internal API response."""
        accounts = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            # Try common keys for account arrays
            for key in [
                "Accounts",
                "accounts",
                "AccountList",
                "accountList",
                "Items",
                "items",
                "AccountListResponse",
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            # Check nested structures
            if not items:
                for val in data.values():
                    if isinstance(val, list) and len(val) > 0:
                        if isinstance(val[0], dict) and any(
                            k in val[0]
                            for k in [
                                "AccountNumber",
                                "accountNumber",
                                "accountId",
                                "AccountId",
                                "accountIdKey",
                                "Balance",
                                "balance",
                                "netAccountValue",
                            ]
                        ):
                            items = val
                            break
                    # E*Trade often nests: data -> key -> AccountListResponse -> Accounts
                    if isinstance(val, dict):
                        for inner in val.values():
                            if isinstance(inner, list):
                                items = inner
                                break
                            if isinstance(inner, dict):
                                for deeper in inner.values():
                                    if isinstance(deeper, list):
                                        items = deeper
                                        break

        for item in items:
            if not isinstance(item, dict):
                continue

            name = (
                item.get("accountName")
                or item.get("AccountName")
                or item.get("accountDesc")
                or item.get("AccountDesc")
                or item.get("displayName")
                or item.get("DisplayName")
                or item.get("institutionType")
                or ""
            )
            balance = (
                item.get("netAccountValue")
                or item.get("NetAccountValue")
                or item.get("accountValue")
                or item.get("AccountValue")
                or item.get("totalMarketValue")
                or item.get("balance")
                or item.get("Balance")
                or 0
            )
            acct_num = (
                item.get("accountId")
                or item.get("AccountId")
                or item.get("accountNumber")
                or item.get("AccountNumber")
                or item.get("accountIdKey")
                or ""
            )
            day_change = item.get("dayChange") or item.get("DayChange") or item.get("todaysGainLoss")
            day_change_pct = item.get("dayChangePercent") or item.get("DayChangePercent") or item.get("todaysGainLossPct")

            if isinstance(balance, str):
                try:
                    balance = float(balance.replace(",", "").replace("$", ""))
                except ValueError:
                    balance = 0

            suffix = str(acct_num)[-4:] if acct_num else ""
            acct_type = self._infer_type(
                item.get("accountType", "") or item.get("AccountType", "") or name
            )

            if name:
                print(
                    f"   API: {name} ...{suffix}: ${balance:,.2f} ({acct_type})",
                    file=sys.stderr,
                )
                accounts.append(
                    {
                        "name": name,
                        "balance": float(balance) if balance else 0.0,
                        "type": acct_type,
                        "account_suffix": suffix,
                        "day_change": day_change,
                        "day_change_percent": day_change_pct,
                    }
                )

        return accounts

    def _parse_positions_from_api(self, data, accounts: list[dict]) -> list[dict]:
        """Parse positions from E*Trade's internal API response."""
        positions = []

        # Determine default account_id from first account
        default_account_id = ""
        default_account_name = ""
        if accounts:
            suffix = accounts[0].get("account_suffix", "")
            id_key = f"etrade_{suffix}" if suffix else accounts[0]["name"]
            default_account_id = hashlib.md5(id_key.encode()).hexdigest()[:16]
            default_account_name = accounts[0]["name"]

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                "positions",
                "Positions",
                "PositionList",
                "positionList",
                "Items",
                "items",
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            # Nested: PortfolioResponse -> AccountPortfolio -> Position
            if not items:
                for val in data.values():
                    if isinstance(val, dict):
                        for inner in val.values():
                            if isinstance(inner, list):
                                # Could be list of account portfolios
                                for entry in inner:
                                    if isinstance(entry, dict):
                                        for pk in ["Position", "position", "positions"]:
                                            if pk in entry and isinstance(entry[pk], list):
                                                items.extend(entry[pk])
                                if not items:
                                    items = inner
                                break
                    elif isinstance(val, list):
                        items = val
                        break

        for item in items:
            if not isinstance(item, dict):
                continue

            symbol = (
                item.get("symbol")
                or item.get("Symbol")
                or item.get("symbolDescription")
                or ""
            )
            # E*Trade sometimes nests: Product -> symbol
            if not symbol and "Product" in item:
                product = item["Product"]
                if isinstance(product, dict):
                    symbol = product.get("symbol") or product.get("Symbol") or ""
            if not symbol and "product" in item:
                product = item["product"]
                if isinstance(product, dict):
                    symbol = product.get("symbol") or product.get("Symbol") or ""

            if not symbol:
                continue

            quantity = (
                item.get("quantity")
                or item.get("Quantity")
                or item.get("qty")
                or item.get("positionQty")
                or 0
            )
            market_value = (
                item.get("marketValue")
                or item.get("MarketValue")
                or item.get("totalMarketValue")
                or item.get("currentValue")
                or 0
            )
            last_price = (
                item.get("lastTrade")
                or item.get("LastTrade")
                or item.get("lastPrice")
                or item.get("LastPrice")
                or item.get("currentPrice")
                or 0
            )
            # Quick -> lastTrade for E*Trade
            if not last_price and "Quick" in item:
                quick = item["Quick"]
                if isinstance(quick, dict):
                    last_price = quick.get("lastTrade") or quick.get("lastPrice") or 0

            day_gain = (
                item.get("daysGain")
                or item.get("DaysGain")
                or item.get("dayGain")
                or item.get("todaysGainLoss")
                or 0
            )
            total_gain = (
                item.get("totalGain")
                or item.get("TotalGain")
                or item.get("totalGainLoss")
                or 0
            )
            total_gain_pct = (
                item.get("totalGainPct")
                or item.get("TotalGainPct")
                or item.get("totalGainLossPct")
                or 0
            )
            description = (
                item.get("symbolDescription")
                or item.get("description")
                or item.get("Description")
            )

            if isinstance(market_value, str):
                try:
                    market_value = float(market_value.replace(",", "").replace("$", ""))
                except ValueError:
                    market_value = 0

            account_id = default_account_id
            account_name = default_account_name

            position_id = hashlib.md5(
                f"{account_id}_{symbol}".encode()
            ).hexdigest()[:16]

            positions.append(
                {
                    "position_id": position_id,
                    "account_id": account_id,
                    "account_name": account_name,
                    "symbol": symbol,
                    "description": description,
                    "quantity": float(quantity) if quantity else None,
                    "last_price": float(last_price) if last_price else None,
                    "market_value": float(market_value) if market_value else None,
                    "day_gain": float(day_gain) if day_gain else None,
                    "total_gain": float(total_gain) if total_gain else None,
                    "total_gain_percent": (
                        float(total_gain_pct) if total_gain_pct else None
                    ),
                }
            )

        return positions

    # ── DOM fallback extractors ─────────────────────────────────────────

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Fallback: extract accounts from DOM elements."""
        accounts = []

        selectors = [
            'div[class*="Account---account"]',
            'div[class*="AccountCardView"]',
            'div[class*="account-card"]',
            '[data-testid*="account"]',
        ]

        account_cards = []
        for selector in selectors:
            account_cards = await page.query_selector_all(selector)
            if account_cards:
                print(
                    f"   Found {len(account_cards)} cards with: {selector}",
                    file=sys.stderr,
                )
                break

        for card in account_cards:
            try:
                text = await card.inner_text()
                lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
                if not lines:
                    continue

                name = lines[0]

                # Look for account suffix
                for line in lines:
                    match = re.search(
                        r"(?:ending with|ending in|\.{3}|-)(\d{4})\b",
                        line,
                        re.IGNORECASE,
                    )
                    if match and match.group(1) not in name:
                        name = f"{name} - {match.group(1)}"
                        break

                net_value = None
                day_gain = None
                day_gain_percent = None

                for i, line in enumerate(lines):
                    line_lower = line.lower()
                    next_line = lines[i + 1] if i + 1 < len(lines) else ""

                    if "net account value" in line_lower:
                        net_value = self._parse_dollar(line) or self._parse_dollar(
                            next_line
                        )
                    elif (
                        "day's gain" in line_lower or "days gain" in line_lower
                    ):
                        day_gain = self._parse_dollar(line) or self._parse_dollar(
                            next_line
                        )
                        day_gain_percent = self._parse_percent(
                            line
                        ) or self._parse_percent(next_line)

                # Fallback: first dollar amount on the card
                if net_value is None:
                    for line in lines:
                        if "$" in line:
                            net_value = self._parse_dollar(line)
                            if net_value:
                                break

                if net_value and net_value > 0:
                    # Extract suffix for filtering
                    suffix = ""
                    suffix_match = re.search(r"(\d{4})\s*$", name)
                    if suffix_match:
                        suffix = suffix_match.group(1)

                    accounts.append(
                        {
                            "name": name,
                            "balance": net_value,
                            "type": "brokerage",
                            "account_suffix": suffix,
                            "day_change": day_gain,
                            "day_change_percent": day_gain_percent,
                        }
                    )

            except Exception as e:
                print(f"   Error parsing account card: {e}", file=sys.stderr)

        return accounts

    def _parse_positions_from_text(
        self, content: str, account_id: str, account_name: str
    ) -> list[dict]:
        """Fallback: parse positions from page text using regex heuristics."""
        positions = []
        lines = content.split("\n")

        skip_symbols = {
            "CASH", "TOTAL", "DAY", "GAIN", "LOSS", "BUY", "SELL", "TYPE",
            "LAST", "PRICE", "QTY", "VALUE", "TRADE", "ACTIONS", "SYMBOL",
            "NET", "IRA", "ROTH", "SEP", "HSA", "PLAN", "ESPP", "RSU", "TRAD",
        }

        for i, line in enumerate(lines):
            line = line.strip()
            symbol_match = re.match(r"^([A-Z]{1,5})(?:\s|$|\()", line)
            if not symbol_match:
                continue

            symbol = symbol_match.group(1)
            if symbol in skip_symbols:
                continue

            context = "\n".join(lines[i : min(len(lines), i + 15)])
            symbol_pos = context.find(symbol)
            after = context[symbol_pos + len(symbol) :] if symbol_pos >= 0 else context

            all_numbers = re.findall(r"([\d,]+\.?\d*)", after)
            all_numbers = [
                float(n.replace(",", ""))
                for n in all_numbers
                if n and float(n.replace(",", "")) > 0
            ]
            all_percents = [
                float(p) for p in re.findall(r"([\d.]+)%", after) if p
            ]

            if len(all_numbers) < 3:
                continue

            market_value = None
            last_price = None
            quantity = None

            percent_set = set(all_percents)
            main_numbers = [n for n in all_numbers if n not in percent_set]

            if len(main_numbers) >= 7:
                last_price = main_numbers[0] if main_numbers[0] < 5000 else None
                quantity = main_numbers[2] if len(main_numbers) > 2 else None
                market_value = main_numbers[6] if len(main_numbers) > 6 else None
            else:
                large = [n for n in all_numbers if n > 10000]
                if large:
                    market_value = max(large)
                prices = [n for n in all_numbers if 10 < n < 2000]
                if prices:
                    last_price = prices[0]
                if market_value and last_price and last_price > 0:
                    quantity = market_value / last_price

            if market_value and market_value > 100:
                position_id = hashlib.md5(
                    f"{account_id}_{symbol}".encode()
                ).hexdigest()[:16]
                positions.append(
                    {
                        "position_id": position_id,
                        "account_id": account_id,
                        "account_name": account_name,
                        "symbol": symbol,
                        "description": None,
                        "quantity": quantity,
                        "last_price": last_price,
                        "market_value": market_value,
                        "day_gain": None,
                        "total_gain": None,
                        "total_gain_percent": (
                            all_percents[-1] if all_percents else None
                        ),
                    }
                )

        # Cash position
        for j, line in enumerate(lines):
            if line.strip().lower() in ("cash", "cash "):
                for k in range(j, min(j + 5, len(lines))):
                    m = re.search(r"\$?([\d,]+\.?\d*)", lines[k])
                    if m:
                        val = float(m.group(1).replace(",", ""))
                        if val > 0:
                            positions.append(
                                {
                                    "position_id": hashlib.md5(
                                        f"{account_id}_CASH".encode()
                                    ).hexdigest()[:16],
                                    "account_id": account_id,
                                    "account_name": account_name,
                                    "symbol": "CASH",
                                    "description": "Cash & Sweep Vehicle",
                                    "quantity": None,
                                    "last_price": None,
                                    "market_value": val,
                                    "day_gain": 0,
                                    "total_gain": 0,
                                    "total_gain_percent": 0,
                                }
                            )
                            break
                break

        return positions

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_dollar(text: str):
        match = re.search(r"([+-])?\$?([\d,]+\.?\d*)", text)
        if match:
            sign = match.group(1) or ""
            amount = float(match.group(2).replace(",", ""))
            return -amount if sign == "-" else amount
        return None

    @staticmethod
    def _parse_percent(text: str):
        match = re.search(r"([+-]?\d+\.?\d*)%", text)
        return float(match.group(1)) if match else None

    @staticmethod
    def _infer_type(text: str) -> str:
        t = text.lower()
        if "checking" in t:
            return "checking"
        if "saving" in t:
            return "savings"
        if "401" in t:
            return "401k"
        if any(w in t for w in ["ira", "roth", "retirement"]):
            return "ira"
        if any(w in t for w in ["brokerage", "individual"]):
            return "brokerage"
        return "brokerage"
