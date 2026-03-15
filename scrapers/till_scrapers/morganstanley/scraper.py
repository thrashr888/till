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
from pathlib import Path
from till_scrapers.base import BaseScraper


def _mask_acct(num: str) -> str:
    """Mask all but last 4 characters of an account number."""
    if len(num) <= 4:
        return f"...{num}"
    return f"...{num[-4:]}"


class MorganstanleyScraper(BaseScraper):
    LOGIN_URL = "https://login.morganstanley.com/SignIn/"
    DASHBOARD_URL = "https://www.morganstanley.com/what-we-do/wealth-management"

    # Workplace (Morgan Stanley at Work / Solium) URLs
    WORKPLACE_LOGIN_URL = "https://atwork.morganstanley.com/solium/servlet/userLogin"
    WORKPLACE_DASHBOARD_URL = "https://atwork.morganstanley.com/solium/servlet/ui/dashboard"

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

    async def _save_debug_snapshot(self, page, label: str):
        """Save screenshot + HTML for debugging."""
        try:
            await page.screenshot(path=f"/tmp/till_morganstanley_{label}.png")
            html = await page.content()
            Path(f"/tmp/till_morganstanley_{label}.html").write_text(html)
        except Exception:
            pass

    async def navigate_and_login(self, page, username: str, password: str):
        dashboard_url = self._get_dashboard_url()
        login_url = self._get_login_url()

        # Try dashboard first — session cookies may still be valid
        print(f"   Checking for active session ({self.variant})...", file=sys.stderr)
        try:
            await page.goto(
                dashboard_url,
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(2000)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
        except Exception as e:
            print(f"   Session check navigation error: {e}", file=sys.stderr)

        current_url = page.url
        print(f"   Session check URL: {current_url}", file=sys.stderr)

        # Session is valid if we're on a dashboard/portfolio page (not login)
        if self.variant == "workplace":
            # Workplace: logged in if we're on /ui/dashboard or /ui/portfolio
            if "/ui/dashboard" in current_url or "/ui/portfolio" in current_url:
                print("   Session active, skipping login", file=sys.stderr)
                return
        elif "signin" not in current_url.lower() and "login" not in current_url.lower():
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
            # Username field — workplace uses input[type="text"], personal uses #username
            username_selectors = (
                ['input[type="text"]', '#username', '#userName', 'input[name="username"]']
                if self.variant == "workplace"
                else ['#username', '#userName', 'input[name="username"]', 'input[type="text"]']
            )
            username_field = None
            for selector in username_selectors:
                try:
                    loc = page.locator(selector).first
                    await loc.wait_for(timeout=3000)
                    if await loc.is_visible():
                        username_field = loc
                        break
                except Exception:
                    continue

            if not username_field:
                await self._save_debug_snapshot(page, "login_fail")
                raise Exception(
                    f"Could not find username field on login page. "
                    f"URL: {page.url}  -- Try running headful with --pause"
                )

            # Enter username
            await username_field.click()
            await username_field.fill("")
            await username_field.type(username, delay=50)
            await page.wait_for_timeout(500)

            # Find password field — workplace uses #password
            password_field = None
            for selector in [
                '#password', 'input[name="password"]', 'input[type="password"]',
            ]:
                try:
                    loc = page.locator(selector).first
                    if await loc.is_visible():
                        password_field = loc
                        break
                except Exception:
                    continue

            if not password_field:
                # Some MS login flows show password on a second step
                for btn_sel in [
                    'input[type="submit"]', 'button[type="submit"]',
                    'button:has-text("Next")',
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
                    '#password', 'input[name="password"]', 'input[type="password"]',
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
                await self._save_debug_snapshot(page, "login_fail")
                raise Exception(
                    f"Could not find password field. "
                    f"URL: {page.url}  -- Try running headful with --pause"
                )

            # Enter password
            await password_field.click()
            await password_field.type(password, delay=30)
            await page.wait_for_timeout(500)

            # Click submit — workplace uses input[type="submit"]
            submit_selectors = (
                ['input[type="submit"]', 'button[type="submit"]',
                 'button:has-text("Sign In")', 'button:has-text("Log In")']
                if self.variant == "workplace"
                else ['button[type="submit"]', 'button:has-text("Sign In")',
                      'button:has-text("Log In")', 'input[type="submit"]']
            )
            submitted = False
            for btn_sel in submit_selectors:
                try:
                    btn = page.locator(btn_sel).first
                    if await btn.is_visible():
                        await btn.click()
                        submitted = True
                        break
                except Exception:
                    continue

            if not submitted:
                await self._save_debug_snapshot(page, "login_fail")
                raise Exception(
                    f"Could not find submit button. "
                    f"URL: {page.url}  -- Try running headful with --pause"
                )

            print("   Waiting for login...", file=sys.stderr)

            # Wait for redirect away from login page
            def _is_logged_in(url: str) -> bool:
                u = url.lower()
                if self.variant == "workplace":
                    return "/ui/dashboard" in u or "/ui/portfolio" in u
                return "signin" not in u and "login" not in u

            try:
                await page.wait_for_url(_is_logged_in, timeout=30000)
            except Exception:
                pass

            if _is_logged_in(page.url):
                print("   Login successful!", file=sys.stderr)
            else:
                print("   Waiting for 2FA...", file=sys.stderr)
                await self._save_debug_snapshot(page, "2fa")
                try:
                    await page.wait_for_url(_is_logged_in, timeout=30000)
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await self._save_debug_snapshot(page, "login_fail")
                    raise Exception(
                        f"Login failed (possibly 2FA timeout). "
                        f"URL: {page.url}  -- Try running headful with --pause"
                    )

        except Exception as e:
            if "login fail" not in str(e).lower():
                await self._save_debug_snapshot(page, "login_fail")
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            raise

        await page.wait_for_timeout(2000)
        await self._save_debug_snapshot(page, "post_login")

    async def extract(self, page) -> dict:
        """Extract accounts, positions, and transactions using Morgan Stanley's internal APIs."""

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
            timeout=30000,
        )
        await page.wait_for_timeout(5000)

        # Step 2b: Try direct API calls for account data
        print("   Trying direct API calls...", file=sys.stderr)
        api_endpoints = [
            "https://www.morganstanley.com/api/accounts/summary",
            "https://www.morganstanley.com/api/portfolio/summary",
            "https://www.morganstanley.com/gw/accounts",
            "https://www.morganstanley.com/gw/portfolio/positions",
        ]
        if self.variant == "workplace":
            api_endpoints = [
                "https://atwork.morganstanley.com/solium/servlet/api/myholdings",
                "https://atwork.morganstanley.com/solium/servlet/api/accounts/summary",
                "https://atwork.morganstanley.com/solium/servlet/api/portfolio",
            ]

        for endpoint in api_endpoints:
            try:
                resp = await page.request.get(endpoint, timeout=10000)
                if resp.status == 200:
                    ct = resp.headers.get('content-type', '')
                    if 'json' in ct:
                        body = await resp.text()
                        if body and body.strip() and body.strip()[0] in '{[':
                            api_responses[endpoint] = json.loads(body)
                            print(f"   Direct API [{resp.status}]: {endpoint[:80]}", file=sys.stderr)
            except Exception:
                pass

        # For personal accounts, also try the accounts overview page
        if self.variant == "personal":
            try:
                await page.goto(
                    "https://www.morganstanley.com/what-we-do/wealth-management/accounts",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await page.wait_for_timeout(3000)
            except Exception:
                pass

        await self._save_debug_snapshot(page, "accounts")

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

        # Step 4: Extract transactions
        transactions = await self._extract_transactions(page, api_responses, accounts)
        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

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
                "account_name": f"{acct['name']} {_mask_acct(suffix)}" if suffix else acct["name"],
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

        txn_results = []
        for txn in transactions:
            txn_results.append({
                "txn_id": txn["id"],
                "account_id": txn["account_id"],
                "date": txn["date"],
                "description": txn["description"],
                "amount": txn["amount"],
                "category": txn.get("category"),
                "status": txn.get("status", "posted"),
            })

        return {
            "status": "ok",
            "source": "morganstanley",
            "accounts": account_results,
            "transactions": txn_results,
            "positions": position_results,
            "balance_history": [],
        }

    async def _extract_transactions(self, page, api_responses: dict, accounts: list[dict]) -> list[dict]:
        """Navigate to activity page and extract transactions."""
        transactions = []

        # Determine default account_id
        default_acct_id = ""
        if accounts:
            suffix = accounts[0].get("account_suffix", "")
            id_key = f"morganstanley_{suffix}" if suffix else accounts[0]["name"]
            default_acct_id = hashlib.md5(id_key.encode()).hexdigest()[:16]

        # Clear API responses to capture fresh ones from activity pages
        api_responses.clear()

        # Navigate to activity/transaction pages
        activity_urls = []
        if self.variant == "workplace":
            activity_urls = [
                "https://atwork.morganstanley.com/solium/servlet/ui/activity",
                "https://atwork.morganstanley.com/solium/servlet/ui/activity/past-events",
            ]
        else:
            activity_urls = [
                "https://www.morganstanley.com/what-we-do/wealth-management/activity",
                "https://www.morganstanley.com/what-we-do/wealth-management/transactions",
            ]

        print("   Loading activity page...", file=sys.stderr)
        for url in activity_urls:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)
                # If we didn't get redirected back to login, this page exists
                if "signin" not in page.url.lower() and "login" not in page.url.lower():
                    break
            except Exception:
                continue

        await self._save_debug_snapshot(page, "activity")

        # Try direct API calls for transactions
        txn_api_endpoints = [
            "https://www.morganstanley.com/api/transactions",
            "https://www.morganstanley.com/api/activity",
            "https://www.morganstanley.com/gw/transactions",
        ]
        if self.variant == "workplace":
            txn_api_endpoints = [
                "https://atwork.morganstanley.com/solium/servlet/api/transactions",
                "https://atwork.morganstanley.com/solium/servlet/api/activity",
            ]

        for endpoint in txn_api_endpoints:
            try:
                resp = await page.request.get(endpoint, timeout=10000)
                if resp.status == 200:
                    ct = resp.headers.get('content-type', '')
                    if 'json' in ct:
                        body = await resp.text()
                        if body and body.strip() and body.strip()[0] in '{[':
                            api_responses[endpoint] = json.loads(body)
                            print(f"   Direct API [{resp.status}]: {endpoint[:80]}", file=sys.stderr)
            except Exception:
                pass

        # Parse transactions from API responses
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['transaction', 'activity', 'history']):
                txns = self._parse_transactions_from_api(data, default_acct_id)
                if txns:
                    transactions.extend(txns)

        # Fallback: DOM extraction
        if not transactions:
            print("   No transactions from API, trying DOM...", file=sys.stderr)
            transactions = await self._extract_transactions_dom(page, default_acct_id)

        return transactions

    def _parse_transactions_from_api(self, data, default_acct_id: str) -> list[dict]:
        """Parse transactions from Morgan Stanley's internal API response."""
        transactions = []
        items = []

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'transactions', 'Transactions', 'transactionList',
                'activity', 'Activity', 'activityList',
                'Items', 'items', 'data',
            ]:
                if key in data and isinstance(data[key], list) and data[key]:
                    items = data[key]
                    break

        for item in items:
            if not isinstance(item, dict):
                continue

            date = (
                item.get('transactionDate') or item.get('date') or
                item.get('tradeDate') or item.get('settleDate') or ''
            )
            desc = (
                item.get('description') or item.get('transactionDescription') or
                item.get('activityDescription') or item.get('type') or ''
            )
            amount = (
                item.get('amount') or item.get('transactionAmount') or
                item.get('netAmount') or item.get('totalAmount') or 0
            )

            if isinstance(amount, str):
                try:
                    amount = float(amount.replace(',', '').replace('$', ''))
                except ValueError:
                    amount = 0

            if date and desc:
                iso_date = self._normalize_date(date)
                txn_id = hashlib.md5(f"{iso_date}_{desc}_{amount}".encode()).hexdigest()[:16]

                status = "posted"
                if item.get('isPending') or item.get('pending'):
                    status = "pending"

                transactions.append({
                    "id": txn_id,
                    "account_id": default_acct_id,
                    "date": iso_date,
                    "description": desc.strip(),
                    "amount": float(amount),
                    "category": self._infer_category(desc),
                    "status": status,
                })

        return transactions

    async def _extract_transactions_dom(self, page, default_acct_id: str) -> list[dict]:
        """Fallback: extract transactions from the activity page DOM."""
        transactions = []

        # Try common transaction row selectors
        rows = []
        for selector in [
            '[data-testid*="transaction"]', 'table tbody tr',
        ]:
            rows = await page.query_selector_all(selector)
            if rows:
                print(f"   DOM: found {len(rows)} transaction rows with {selector}", file=sys.stderr)
                break

        for row in rows:
            try:
                text = await row.inner_text()
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                if len(lines) < 2:
                    continue

                # Find date
                date_match = re.search(
                    r'(\d{1,2}/\d{1,2}/\d{2,4})|(\d{4}-\d{2}-\d{2})',
                    text,
                )
                if not date_match:
                    continue

                iso_date = self._normalize_date(date_match.group(0))

                # Find amount
                amount = None
                for line in reversed(lines):
                    m = re.search(r'([+-])?\$?([\d,]+\.?\d*)', line)
                    if m:
                        val = float(m.group(2).replace(',', ''))
                        if m.group(1) == '-':
                            val = -val
                        amount = val
                        break

                if amount is None:
                    continue

                # Description: first non-date, non-amount line
                desc = ""
                for line in lines:
                    if line == date_match.group(0) or "$" in line:
                        continue
                    if len(line) > 3:
                        desc = line
                        break

                if not desc:
                    desc = lines[1] if len(lines) > 1 else "Unknown"

                txn_id = hashlib.md5(f"{iso_date}_{desc}_{amount}".encode()).hexdigest()[:16]
                transactions.append({
                    "id": txn_id,
                    "account_id": default_acct_id,
                    "date": iso_date,
                    "description": desc.strip(),
                    "amount": float(amount),
                    "category": self._infer_category(desc),
                    "status": "posted",
                })
            except Exception:
                continue

        return transactions

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Morgan Stanley's internal API response."""
        accounts = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'Accounts', 'accounts', 'accountList',
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
                print(f"   API: {name} {_mask_acct(suffix)}: ${balance:,.2f} ({acct_type})", file=sys.stderr)
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
                'positionList', 'securities',
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
            '[data-testid*="account"]', '.account-row', 'div[class*="Account"]',
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
        """Extract workplace (Morgan Stanley at Work) accounts from dashboard page text.

        The dashboard shows:
          - Total portfolio value: $X
          - Available value section with account names, balances, and share counts
          - Unavailable value section with account names, balances, and share counts

        Example page text layout:
          Available value
          $14,132.78
          IBM Long Share Account
          $14,132.7857.38 shares
          Unavailable value
          $388,383.56
          Share Units (RSU)
          $388,383.561,577 Share Units (RSU)
        """
        accounts = []

        # Get the full page text
        page_text = await page.inner_text('body')
        lines = [l.strip() for l in page_text.split('\n') if l.strip()]

        print(f"   DOM: parsing {len(lines)} text lines from workplace dashboard", file=sys.stderr)

        # Parse Available and Unavailable sections
        for section_label, availability in [("Available value", "available"), ("Unavailable value", "unavailable")]:
            try:
                # Find the section header line index
                section_idx = None
                for i, line in enumerate(lines):
                    if line == section_label:
                        section_idx = i
                        break

                if section_idx is None:
                    print(f"   DOM: could not find '{section_label}' section", file=sys.stderr)
                    continue

                # The section total is the next line (e.g. "$14,132.78")
                section_total_idx = section_idx + 1
                if section_total_idx >= len(lines):
                    continue

                section_total_str = lines[section_total_idx]
                section_total_match = re.search(r'\$([\d,]+\.?\d*)', section_total_str)
                if not section_total_match:
                    continue

                section_total = float(section_total_match.group(1).replace(',', ''))
                print(f"   DOM: {section_label} total: ${section_total:,.2f}", file=sys.stderr)

                # Scan subsequent lines for account entries.
                # Pattern: account name line, then "$balance<shares> shares" or "$balance<N> Share Units"
                # e.g. "IBM Long Share Account" followed by "$14,132.7857.38 shares"
                # e.g. "Share Units (RSU)" followed by "$388,383.561,577 Share Units (RSU)"
                i = section_total_idx + 1
                # Look ahead for account entries until we hit a section boundary
                boundary_markers = [
                    "Unavailable value", "Available value", "Upcoming events",
                    "Past events", "Quick Links", "Your Equity Education",
                    "Timeline", "Tasks",
                ]
                while i < len(lines):
                    line = lines[i]

                    # Stop if we hit a section boundary
                    if any(line.startswith(m) for m in boundary_markers):
                        break

                    # Skip non-account lines (navigation, details links, disclaimers, etc.)
                    if line in ("Transact", "Timeline", "Disclaimer", "details", "Slide 1 of 2") or line.endswith("details"):
                        i += 1
                        continue

                    # Try to find account name + value pattern.
                    # The value line has format: $<balance><shares> shares
                    # or: $<balance><N> Share Units (RSU)
                    # Look at next line for the combined balance+shares string
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        # Match: $402,516.3457.38 shares  or  $388,383.561,577 Share Units (RSU)
                        combined_match = re.match(
                            r'\$([\d,]+\.?\d*)([\d,.]+)\s+(shares?|Share\s+Units.*)',
                            next_line,
                        )
                        if combined_match:
                            acct_name = line
                            balance = float(combined_match.group(1).replace(',', ''))
                            shares_str = combined_match.group(2).replace(',', '')
                            try:
                                shares = float(shares_str)
                            except ValueError:
                                shares = 0.0
                            share_desc = combined_match.group(3).strip()

                            acct_type = "brokerage"
                            if "rsu" in acct_name.lower() or "rsu" in share_desc.lower():
                                acct_type = "brokerage"

                            print(
                                f"   DOM: {acct_name}: ${balance:,.2f} "
                                f"({shares} shares, {availability})",
                                file=sys.stderr,
                            )

                            # Use availability + account name to create a stable suffix
                            suffix = hashlib.md5(
                                f"{availability}_{acct_name}".encode()
                            ).hexdigest()[:4]

                            accounts.append({
                                "name": f"{acct_name} ({availability.title()})",
                                "balance": balance,
                                "type": acct_type,
                                "account_suffix": suffix,
                                "day_change": None,
                                "day_change_percent": None,
                                "shares": shares,
                                "availability": availability,
                            })
                            i += 2  # skip the value line
                            continue

                    i += 1

            except Exception as e:
                print(f"   DOM: error parsing {section_label}: {e}", file=sys.stderr)

        # If we found nothing from sections, try to at least get the total portfolio value
        if not accounts:
            print("   DOM: no accounts from sections, trying total portfolio value", file=sys.stderr)
            for i, line in enumerate(lines):
                if line == "Total portfolio value" and i + 1 < len(lines):
                    total_match = re.search(r'\$([\d,]+\.?\d*)', lines[i + 1])
                    if total_match:
                        total = float(total_match.group(1).replace(',', ''))
                        print(f"   DOM: total portfolio value: ${total:,.2f}", file=sys.stderr)
                        accounts.append({
                            "name": "Morgan Stanley at Work Portfolio",
                            "balance": total,
                            "type": "brokerage",
                            "account_suffix": "msaw",
                            "day_change": None,
                            "day_change_percent": None,
                        })
                    break

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
        print(f"   DOM: {name} {_mask_acct(suffix)}: ${balance:,.2f} ({acct_type})", file=sys.stderr)

        return {
            "name": name,
            "balance": balance,
            "type": acct_type,
            "account_suffix": suffix,
            "day_change": None,
            "day_change_percent": None,
        }

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Normalize various date formats to ISO (YYYY-MM-DD)."""
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]

        m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', date_str)
        if m:
            month, day, year = m.group(1), m.group(2), m.group(3)
            if len(year) == 2:
                year = '20' + year
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        import calendar
        month_abbrs = {v.lower(): f"{i:02d}" for i, v in enumerate(calendar.month_abbr) if v}
        m = re.search(r'(\w{3})\s+(\d{1,2}),?\s*(\d{4})?', date_str)
        if m:
            month_str = m.group(1).lower()
            day = m.group(2)
            year = m.group(3) or "2026"
            month_num = month_abbrs.get(month_str, "01")
            return f"{year}-{month_num}-{day.zfill(2)}"

        return date_str

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

    @staticmethod
    def _infer_category(description: str) -> str:
        """Infer transaction category from description."""
        d = description.upper()
        if any(w in d for w in ['DIVIDEND', 'DIV']):
            return "Dividend"
        if any(w in d for w in ['INTEREST', 'INT']):
            return "Interest"
        if any(w in d for w in ['BUY', 'PURCHASE', 'BOUGHT']):
            return "Buy"
        if any(w in d for w in ['SELL', 'SOLD', 'SALE']):
            return "Sell"
        if any(w in d for w in ['TRANSFER', 'XFER', 'WIRE']):
            return "Transfer"
        if any(w in d for w in ['FEE', 'COMMISSION']):
            return "Fee"
        if any(w in d for w in ['VEST', 'RSU', 'ESPP']):
            return "Vesting"
        return "Other"
