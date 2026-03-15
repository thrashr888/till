"""Fidelity scraper.

Strategy: Login via Playwright, then use Fidelity's internal APIs directly
with the session cookies. Falls back to DOM scraping if API interception
doesn't capture account data.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


class FidelityScraper(BaseScraper):
    LOGIN_URL = "https://digital.fidelity.com/prgw/digital/login/full-page"
    DASHBOARD_URL = "https://digital.fidelity.com/ftgw/digital/portfolio/summary"

    def __init__(self, headless: bool = True):
        # Fidelity works headless with stealth
        super().__init__(headless=headless)

        include_str = os.environ.get("TILL_FIDELITY_INCLUDE_ACCOUNTS", "")
        self.include_accounts = [s.strip() for s in include_str.split(",") if s.strip()] if include_str else []

    async def navigate_and_login(self, page, username: str, password: str):
        # Try dashboard first — session cookies may still be valid
        print("   Checking for active session...", file=sys.stderr)
        try:
            await page.goto(
                self.DASHBOARD_URL,
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

        # Session is valid if we stayed on the portfolio page (not redirected to login)
        if "portfolio" in current_url and "login" not in current_url.lower():
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — go to login page
        print("   Session expired, logging in...", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=300000)
        await page.wait_for_timeout(2000)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source fidelity` "
                "or set TILL_USERNAME/TILL_PASSWORD env vars."
            )

        print("   Auto-filling login credentials...", file=sys.stderr)
        try:
            # Wait for login form
            await page.wait_for_selector('#userId-input', timeout=15000)

            # Username
            print("   Entering username...", file=sys.stderr)
            await page.locator('#userId-input').click()
            await page.locator('#userId-input').fill("")
            await page.locator('#userId-input').type(username, delay=50)
            await page.wait_for_timeout(500)

            # Password
            print("   Entering password...", file=sys.stderr)
            await page.locator('#password').click()
            await page.locator('#password').type(password, delay=30)
            await page.wait_for_timeout(500)

            # Submit
            print("   Clicking login...", file=sys.stderr)
            await page.locator('button[type="submit"]').click()

            print("   Waiting for login...", file=sys.stderr)
            try:
                await page.wait_for_url("**/portfolio/**", timeout=120000)
            except Exception:
                pass

            if "portfolio" in page.url and "login" not in page.url.lower():
                print("   Login successful!", file=sys.stderr)
            else:
                print(f"   Post-login: {page.url}", file=sys.stderr)
                print("   Waiting for 2FA...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_fidelity_2fa.png")
                try:
                    await page.wait_for_url("**/portfolio/**", timeout=120000)
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await page.screenshot(path="/tmp/till_fidelity_login.png")
                    raise Exception(f"Login failed. URL: {page.url}")

        except Exception as e:
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_fidelity_login.png")
            raise

        await page.wait_for_timeout(2000)

    async def extract(self, page) -> dict:
        """Extract accounts and positions using Fidelity's internal APIs."""

        # Step 1: Intercept API calls
        api_responses = {}

        async def capture_api(response):
            url = response.url
            if '/api/' in url or '/ftgw/' in url or '/digital/' in url:
                if response.status == 200:
                    try:
                        ct = response.headers.get('content-type', '')
                        if 'json' not in ct:
                            return
                        body = await response.text()
                        if body and body.strip() and body.strip()[0] in '{[':
                            api_responses[url] = json.loads(body)
                            print(f"   API [{response.status}]: {url[:100]}", file=sys.stderr)
                    except Exception:
                        pass

        page.on("response", capture_api)

        # Step 2: Navigate to portfolio summary to trigger API calls
        print("   Loading portfolio summary...", file=sys.stderr)
        await page.goto(
            self.DASHBOARD_URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)

        await page.screenshot(path="/tmp/till_fidelity_accounts.png")

        # Step 3: Parse accounts from intercepted API responses
        accounts = []
        positions = []
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['account', 'portfolio', 'position', 'summary']):
                accts = self._parse_accounts_from_api(data)
                if accts:
                    accounts.extend(accts)
                pos = self._parse_positions_from_api(data)
                if pos:
                    positions.extend(pos)

        # Fallback: DOM extraction if API interception didn't find accounts with balances
        if not accounts or all(a['balance'] == 0 for a in accounts):
            print("   API didn't return account balances, falling back to DOM", file=sys.stderr)
            dom_accounts = await self._extract_accounts_dom(page)
            if dom_accounts:
                accounts = dom_accounts

        # Filter by include_accounts
        if self.include_accounts:
            before = len(accounts)
            accounts = [
                a for a in accounts
                if a.get("account_suffix") in self.include_accounts
                or any(inc in a.get("name", "") for inc in self.include_accounts)
            ]
            skipped = before - len(accounts)
            if skipped:
                print(f"   Filtered to {len(accounts)} accounts (skipped {skipped})", file=sys.stderr)

        print(f"   Found {len(accounts)} accounts", file=sys.stderr)
        print(f"   Found {len(positions)} positions", file=sys.stderr)

        # Save API dump for debugging
        if api_responses:
            debug_path = "/tmp/till_fidelity_api_dump.json"
            try:
                with open(debug_path, 'w') as f:
                    json.dump(api_responses, f, indent=2, default=str)
                print(f"   Saved API dump to {debug_path}", file=sys.stderr)
            except Exception:
                pass

        # Build results
        account_results = []
        for acct in accounts:
            suffix = acct.get("account_suffix", "")
            id_key = f"fidelity_{suffix}" if suffix else acct["name"]
            account_id = hashlib.md5(id_key.encode()).hexdigest()[:16]
            account_results.append({
                "account_id": account_id,
                "account_name": f"{acct['name']} ...{suffix}" if suffix else acct["name"],
                "account_type": acct.get("type", "other"),
                "balance": acct["balance"],
                "day_change": acct.get("day_change"),
                "day_change_percent": acct.get("day_change_percent"),
            })

        position_results = []
        for pos in positions:
            position_results.append({
                "symbol": pos.get("symbol", ""),
                "name": pos.get("name", ""),
                "quantity": pos.get("quantity", 0),
                "price": pos.get("price", 0),
                "value": pos.get("value", 0),
                "day_change": pos.get("day_change"),
                "day_change_percent": pos.get("day_change_percent"),
                "account_id": pos.get("account_id", ""),
            })

        return {
            "status": "ok",
            "source": "fidelity",
            "accounts": account_results,
            "transactions": [],
            "positions": position_results,
            "balance_history": [],
        }

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Fidelity's internal API response."""
        accounts = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'Accounts', 'accounts', 'accountList', 'AccountList',
                'portfolioSummary', 'summary', 'Items', 'items',
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            # Check nested structures
            if not items:
                for val in data.values():
                    if isinstance(val, list) and len(val) > 0:
                        if isinstance(val[0], dict) and any(
                            k in val[0] for k in [
                                'accountNumber', 'AccountNumber', 'accountId',
                                'balance', 'Balance', 'totalValue', 'marketValue',
                            ]
                        ):
                            items = val
                            break

        for item in items:
            if not isinstance(item, dict):
                continue

            name = (
                item.get('accountName') or item.get('AccountName') or
                item.get('displayName') or item.get('DisplayName') or
                item.get('accountDescription') or item.get('description') or
                item.get('nickName') or ''
            )
            balance = (
                item.get('totalValue') or item.get('TotalValue') or
                item.get('marketValue') or item.get('MarketValue') or
                item.get('balance') or item.get('Balance') or
                item.get('accountValue') or item.get('AccountValue') or 0
            )
            acct_num = (
                item.get('accountNumber') or item.get('AccountNumber') or
                item.get('accountId') or item.get('AccountId') or
                item.get('acctNum') or ''
            )

            if isinstance(balance, str):
                try:
                    balance = float(balance.replace(',', '').replace('$', ''))
                except ValueError:
                    balance = 0

            suffix = str(acct_num)[-4:] if acct_num else ""

            acct_type = self._infer_type(
                item.get('accountType', '') or item.get('AccountType') or
                item.get('registrationType', '') or name
            )

            if name:
                print(f"   API: {name} ...{suffix}: ${balance:,.2f} ({acct_type})", file=sys.stderr)
                accounts.append({
                    "name": name,
                    "balance": float(balance) if balance else 0.0,
                    "type": acct_type,
                    "account_suffix": suffix,
                    "day_change": item.get('dayChange') or item.get('DayChange'),
                    "day_change_percent": item.get('dayChangePercent') or item.get('DayChangePercent'),
                })

        return accounts

    def _parse_positions_from_api(self, data) -> list[dict]:
        """Parse positions from Fidelity's internal API response."""
        positions = []

        items = []
        if isinstance(data, dict):
            for key in [
                'positions', 'Positions', 'holdings', 'Holdings',
                'positionList', 'PositionList',
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            # Check nested account structures with positions inside
            if not items:
                for val in data.values():
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict):
                                for pk in ['positions', 'Positions', 'holdings']:
                                    if pk in item and isinstance(item[pk], list):
                                        items.extend(item[pk])

        for item in items:
            if not isinstance(item, dict):
                continue

            symbol = (
                item.get('symbol') or item.get('Symbol') or
                item.get('ticker') or item.get('Ticker') or ''
            )
            name = (
                item.get('description') or item.get('Description') or
                item.get('securityName') or item.get('name') or ''
            )
            quantity = (
                item.get('quantity') or item.get('Quantity') or
                item.get('shares') or item.get('Shares') or 0
            )
            price = (
                item.get('lastPrice') or item.get('LastPrice') or
                item.get('price') or item.get('Price') or
                item.get('currentPrice') or 0
            )
            value = (
                item.get('marketValue') or item.get('MarketValue') or
                item.get('value') or item.get('Value') or
                item.get('currentValue') or 0
            )

            for field in [quantity, price, value]:
                if isinstance(field, str):
                    try:
                        field = float(field.replace(',', '').replace('$', ''))
                    except ValueError:
                        field = 0

            if symbol or name:
                positions.append({
                    "symbol": symbol,
                    "name": name,
                    "quantity": float(quantity) if quantity else 0,
                    "price": float(price) if price else 0,
                    "value": float(value) if value else 0,
                    "day_change": item.get('dayChange') or item.get('DayChange'),
                    "day_change_percent": item.get('dayChangePercent') or item.get('DayChangePercent'),
                    "account_id": item.get('accountId') or item.get('accountNumber') or "",
                })

        return positions

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Fallback: extract accounts from DOM on Fidelity portfolio summary."""
        accounts = []

        # Fidelity groups accounts in sections — look for account rows
        # Try various selectors that Fidelity's portfolio page uses
        for selector in [
            '[data-testid*="account"]',
            '.account-row',
            '.portfolio-account',
            '.js-account',
            'div[class*="AccountRow"]',
            'tr[class*="account"]',
        ]:
            rows = await page.query_selector_all(selector)
            if rows:
                print(f"   DOM: found {len(rows)} rows with {selector}", file=sys.stderr)
                for row in rows:
                    try:
                        acct = await self._parse_dom_account_row(row)
                        if acct:
                            accounts.append(acct)
                    except Exception as e:
                        print(f"   DOM parse error: {e}", file=sys.stderr)
                if accounts:
                    break

        # Broader fallback: look for any table rows with dollar amounts
        if not accounts:
            rows = await page.query_selector_all('table tbody tr')
            for row in rows:
                try:
                    acct = await self._parse_dom_table_row(row)
                    if acct:
                        accounts.append(acct)
                except Exception:
                    continue

        return accounts

    async def _parse_dom_account_row(self, row) -> dict | None:
        """Parse a single account row element."""
        text = await row.inner_text()
        if not text or not text.strip():
            return None

        lines = [l.strip() for l in text.split('\n') if l.strip()]
        if len(lines) < 2:
            return None

        name = lines[0]
        if 'Total' in name or not name:
            return None

        # Look for account number pattern (e.g., ...1234 or ending in 1234)
        suffix = ""
        for line in lines:
            m = re.search(r'(?:\.{2,3}|ending\s+in\s+)(\d{3,4})', line, re.IGNORECASE)
            if m:
                suffix = m.group(1)
                break

        # Look for dollar amount as balance
        balance = 0.0
        for line in lines:
            m = re.search(r'\$?([\d,]+\.?\d*)', line)
            if m:
                try:
                    val = float(m.group(1).replace(',', ''))
                    if val > balance:
                        balance = val
                except ValueError:
                    pass

        acct_type = self._infer_type(name)
        print(f"   DOM: {name} ...{suffix}: ${balance:,.2f} ({acct_type})", file=sys.stderr)

        return {
            "name": name,
            "balance": balance,
            "type": acct_type,
            "account_suffix": suffix,
            "day_change": None,
            "day_change_percent": None,
        }

    async def _parse_dom_table_row(self, row) -> dict | None:
        """Parse a table row for account info."""
        cells = await row.query_selector_all('td')
        if len(cells) < 2:
            return None

        texts = [await c.inner_text() for c in cells]
        texts = [t.strip() for t in texts]

        name = texts[0].split('\n')[0].strip()
        if not name or 'Total' in name:
            return None

        suffix = ""
        for t in texts:
            m = re.search(r'(?:\.{2,3}|ending\s+in\s+)(\d{3,4})', t, re.IGNORECASE)
            if m:
                suffix = m.group(1)
                break

        balance = 0.0
        for t in texts[1:]:
            m = re.search(r'\$?([\d,]+\.?\d*)', t)
            if m:
                try:
                    val = float(m.group(1).replace(',', ''))
                    if val > balance:
                        balance = val
                except ValueError:
                    pass

        if balance == 0:
            return None

        acct_type = self._infer_type(name)
        print(f"   DOM: {name} ...{suffix}: ${balance:,.2f} ({acct_type})", file=sys.stderr)

        return {
            "name": name,
            "balance": balance,
            "type": acct_type,
            "account_suffix": suffix,
            "day_change": None,
            "day_change_percent": None,
        }

    @staticmethod
    def _infer_type(text: str) -> str:
        t = text.lower()
        if 'checking' in t:
            return "checking"
        if 'saving' in t:
            return "savings"
        if '401' in t:
            return "401k"
        if 'hsa' in t or 'health' in t:
            return "hsa"
        if any(w in t for w in ['ira', 'roth', 'retirement', 'rollover']):
            return "ira"
        if any(w in t for w in ['brokerage', 'individual', 'joint']):
            return "brokerage"
        return "other"
