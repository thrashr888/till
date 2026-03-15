"""Morgan Stanley scraper.

Strategy: Login via Playwright, then use Morgan Stanley's internal APIs directly
with the session cookies. Falls back to DOM scraping if API interception
doesn't capture account data.

Supports two variants:
  - personal: Standard wealth management accounts
  - workplace: StockPlan Connect / employer stock plans
Set TILL_MORGANSTANLEY_VARIANT=workplace for employer accounts.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


class MorganstanleyScraper(BaseScraper):
    LOGIN_URL = "https://login.morganstanley.com/SignIn/"
    DASHBOARD_URL = "https://www.morganstanley.com/what-we-do/wealth-management"

    # Workplace (StockPlan Connect) URLs
    WORKPLACE_LOGIN_URL = "https://stockplanconnect.morganstanley.com"
    WORKPLACE_DASHBOARD_URL = "https://stockplanconnect.morganstanley.com/app/myholdings"

    def __init__(self, headless: bool = True):
        # Morgan Stanley detects headless — force headful
        super().__init__(headless=False)
        self.variant = os.environ.get("TILL_MORGANSTANLEY_VARIANT", "personal")

        include_str = os.environ.get("TILL_MORGANSTANLEY_INCLUDE_ACCOUNTS", "")
        self.include_accounts = [s.strip() for s in include_str.split(",") if s.strip()] if include_str else []

    def _get_login_url(self) -> str:
        if self.variant == "workplace":
            return self.WORKPLACE_LOGIN_URL
        return self.LOGIN_URL

    def _get_dashboard_url(self) -> str:
        if self.variant == "workplace":
            return self.WORKPLACE_DASHBOARD_URL
        return self.DASHBOARD_URL

    async def navigate_and_login(self, page, username: str, password: str):
        dashboard_url = self._get_dashboard_url()
        login_url = self._get_login_url()

        # Try dashboard first — session cookies may still be valid
        print(f"   Checking for active session ({self.variant})...", file=sys.stderr)
        try:
            await page.goto(
                dashboard_url,
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

        # Session is valid if we didn't get redirected to login
        if "signin" not in current_url.lower() and "login" not in current_url.lower():
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — go to login page
        print("   Session expired, logging in...", file=sys.stderr)
        await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source morganstanley` "
                "or set TILL_USERNAME/TILL_PASSWORD env vars."
            )

        print("   Auto-filling login credentials...", file=sys.stderr)
        try:
            # Morgan Stanley login form — try multiple selector strategies
            # The login page may use different selectors depending on variant

            # Wait for any login form to appear
            username_field = None
            for selector in [
                '#username', '#userName', 'input[name="username"]',
                'input[name="userName"]', 'input[type="text"]',
                '#txtUserName', 'input[id*="user" i]',
            ]:
                try:
                    loc = page.locator(selector).first
                    await loc.wait_for(timeout=3000)
                    if await loc.is_visible():
                        username_field = loc
                        print(f"   Found username field: {selector}", file=sys.stderr)
                        break
                except Exception:
                    continue

            if not username_field:
                await page.screenshot(path="/tmp/till_morganstanley_login.png")
                raise Exception("Could not find username field on login page")

            # Enter username
            print("   Entering username...", file=sys.stderr)
            await username_field.click()
            await username_field.fill("")
            await username_field.type(username, delay=50)
            await page.wait_for_timeout(500)

            # Find password field
            password_field = None
            for selector in [
                '#password', '#txtPassword', 'input[name="password"]',
                'input[type="password"]', 'input[id*="pass" i]',
            ]:
                try:
                    loc = page.locator(selector).first
                    if await loc.is_visible():
                        password_field = loc
                        print(f"   Found password field: {selector}", file=sys.stderr)
                        break
                except Exception:
                    continue

            if not password_field:
                # Some MS login flows show password on a second step
                print("   No password field visible, checking for next button...", file=sys.stderr)
                for btn_sel in [
                    'button[type="submit"]', '#btnNext', 'button:has-text("Next")',
                    'input[type="submit"]',
                ]:
                    try:
                        btn = page.locator(btn_sel).first
                        if await btn.is_visible():
                            await btn.click()
                            await page.wait_for_timeout(2000)
                            break
                    except Exception:
                        continue

                # Now look for password field again
                for selector in [
                    '#password', '#txtPassword', 'input[name="password"]',
                    'input[type="password"]', 'input[id*="pass" i]',
                ]:
                    try:
                        loc = page.locator(selector).first
                        await loc.wait_for(timeout=5000)
                        if await loc.is_visible():
                            password_field = loc
                            break
                    except Exception:
                        continue

            if not password_field:
                await page.screenshot(path="/tmp/till_morganstanley_login.png")
                raise Exception("Could not find password field on login page")

            # Enter password
            print("   Entering password...", file=sys.stderr)
            await password_field.click()
            await password_field.type(password, delay=30)
            await page.wait_for_timeout(500)

            # Click submit
            print("   Clicking login...", file=sys.stderr)
            submitted = False
            for btn_sel in [
                'button[type="submit"]', '#btnSubmit', '#btnLogin',
                'button:has-text("Sign In")', 'button:has-text("Log In")',
                'input[type="submit"]',
            ]:
                try:
                    btn = page.locator(btn_sel).first
                    if await btn.is_visible():
                        await btn.click()
                        submitted = True
                        break
                except Exception:
                    continue

            if not submitted:
                await page.screenshot(path="/tmp/till_morganstanley_login.png")
                raise Exception("Could not find submit button")

            print("   Waiting for login...", file=sys.stderr)
            try:
                await page.wait_for_url(
                    lambda url: "signin" not in url.lower() and "login" not in url.lower(),
                    timeout=30000,
                )
            except Exception:
                pass

            if "signin" not in page.url.lower() and "login" not in page.url.lower():
                print("   Login successful!", file=sys.stderr)
            else:
                print(f"   Post-login: {page.url}", file=sys.stderr)
                print("   Waiting for 2FA...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_morganstanley_2fa.png")
                try:
                    await page.wait_for_url(
                        lambda url: "signin" not in url.lower() and "login" not in url.lower(),
                        timeout=30000,
                    )
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await page.screenshot(path="/tmp/till_morganstanley_login.png")
                    raise Exception(f"Login failed. URL: {page.url}")

        except Exception as e:
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_morganstanley_login.png")
            raise

        await page.wait_for_timeout(2000)

    async def extract(self, page) -> dict:
        """Extract accounts and positions using Morgan Stanley's internal APIs."""

        # Step 1: Intercept API calls
        api_responses = {}

        async def capture_api(response):
            url = response.url
            if any(k in url for k in ['/api/', '/svc/', '/service/', '/gw/', '/rest/']):
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

        # Step 2: Navigate to appropriate dashboard
        dashboard_url = self._get_dashboard_url()
        print(f"   Loading dashboard ({self.variant})...", file=sys.stderr)
        await page.goto(
            dashboard_url,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)

        # For personal accounts, also try the accounts overview page
        if self.variant == "personal":
            try:
                await page.goto(
                    "https://www.morganstanley.com/what-we-do/wealth-management/accounts",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await page.wait_for_timeout(5000)
            except Exception:
                pass

        await page.screenshot(path="/tmp/till_morganstanley_accounts.png")

        # Step 3: Parse accounts from intercepted API responses
        accounts = []
        positions = []
        for url, data in api_responses.items():
            if any(k in url.lower() for k in [
                'account', 'portfolio', 'position', 'summary', 'holding', 'balance',
            ]):
                accts = self._parse_accounts_from_api(data)
                if accts:
                    accounts.extend(accts)
                pos = self._parse_positions_from_api(data)
                if pos:
                    positions.extend(pos)

        # Fallback: DOM extraction
        if not accounts or all(a['balance'] == 0 for a in accounts):
            print("   API didn't return account balances, falling back to DOM", file=sys.stderr)
            if self.variant == "workplace":
                dom_accounts = await self._extract_workplace_dom(page)
            else:
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
            debug_path = "/tmp/till_morganstanley_api_dump.json"
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
            id_key = f"morganstanley_{suffix}" if suffix else acct["name"]
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
            "source": "morganstanley",
            "accounts": account_results,
            "transactions": [],
            "positions": position_results,
            "balance_history": [],
        }

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Morgan Stanley's internal API response."""
        accounts = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'Accounts', 'accounts', 'accountList', 'AccountList',
                'portfolioSummary', 'summary', 'accountSummary',
                'Items', 'items', 'data',
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
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
                item.get('accountValue') or item.get('AccountValue') or
                item.get('netValue') or item.get('NetValue') or 0
            )
            acct_num = (
                item.get('accountNumber') or item.get('AccountNumber') or
                item.get('accountId') or item.get('AccountId') or ''
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
        """Parse positions from Morgan Stanley's internal API response."""
        positions = []

        items = []
        if isinstance(data, dict):
            for key in [
                'positions', 'Positions', 'holdings', 'Holdings',
                'positionList', 'PositionList', 'securities', 'Securities',
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            if not items:
                for val in data.values():
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict):
                                for pk in ['positions', 'Positions', 'holdings', 'securities']:
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
                item.get('securityName') or item.get('name') or
                item.get('securityDescription') or ''
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
        """Fallback: extract personal accounts from DOM."""
        accounts = []

        for selector in [
            '[data-testid*="account"]',
            '.account-row',
            '.account-card',
            'div[class*="Account"]',
            'tr[class*="account"]',
            '.wm-account',
        ]:
            rows = await page.query_selector_all(selector)
            if rows:
                print(f"   DOM: found {len(rows)} rows with {selector}", file=sys.stderr)
                for row in rows:
                    try:
                        acct = await self._parse_dom_row(row)
                        if acct:
                            accounts.append(acct)
                    except Exception as e:
                        print(f"   DOM parse error: {e}", file=sys.stderr)
                if accounts:
                    break

        # Broader fallback: table rows
        if not accounts:
            rows = await page.query_selector_all('table tbody tr')
            for row in rows:
                try:
                    acct = await self._parse_dom_row(row)
                    if acct:
                        accounts.append(acct)
                except Exception:
                    continue

        return accounts

    async def _extract_workplace_dom(self, page) -> list[dict]:
        """Fallback: extract workplace/StockPlan Connect accounts from DOM."""
        accounts = []

        # StockPlan Connect typically shows holdings in a summary view
        for selector in [
            '[data-testid*="holding"]',
            '.holding-row',
            '.stock-plan-summary',
            'div[class*="Holding"]',
            '.account-summary',
            'tr[class*="holding"]',
        ]:
            rows = await page.query_selector_all(selector)
            if rows:
                print(f"   DOM: found {len(rows)} rows with {selector}", file=sys.stderr)
                for row in rows:
                    try:
                        acct = await self._parse_dom_row(row)
                        if acct:
                            accounts.append(acct)
                    except Exception as e:
                        print(f"   DOM parse error: {e}", file=sys.stderr)
                if accounts:
                    break

        # Broader fallback
        if not accounts:
            rows = await page.query_selector_all('table tbody tr')
            for row in rows:
                try:
                    acct = await self._parse_dom_row(row)
                    if acct:
                        accounts.append(acct)
                except Exception:
                    continue

        return accounts

    async def _parse_dom_row(self, row) -> dict | None:
        """Parse a single DOM row for account info."""
        text = await row.inner_text()
        if not text or not text.strip():
            return None

        lines = [l.strip() for l in text.split('\n') if l.strip()]
        if len(lines) < 2:
            return None

        name = lines[0]
        if 'Total' in name or not name:
            return None

        # Look for account number suffix
        suffix = ""
        for line in lines:
            m = re.search(r'(?:\.{2,3}|ending\s+in\s+|x{2,})(\d{3,4})', line, re.IGNORECASE)
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

    @staticmethod
    def _infer_type(text: str) -> str:
        t = text.lower()
        if 'checking' in t:
            return "checking"
        if 'saving' in t:
            return "savings"
        if '401' in t:
            return "401k"
        if any(w in t for w in ['ira', 'roth', 'retirement', 'rollover']):
            return "ira"
        if any(w in t for w in ['stock plan', 'espp', 'rsu', 'equity', 'employer']):
            return "brokerage"
        if any(w in t for w in ['brokerage', 'individual', 'joint', 'investment']):
            return "brokerage"
        return "other"
