"""Charles Schwab scraper.

Strategy: Login via Playwright, then extract accounts from DOM (API returns $0
balances with base64 suffixes) and transactions via API interception on the
history page. Direct API calls are attempted first for transactions.
"""

import re
import sys
import hashlib
import json
import os
from pathlib import Path
from till_scrapers.base import BaseScraper


def _save_html(page_content: str, stage: str):
    """Save HTML snapshot for offline debugging."""
    try:
        path = f"/tmp/till_schwab_{stage}.html"
        Path(path).write_text(page_content)
        print(f"   Saved HTML snapshot: {path}", file=sys.stderr)
    except Exception as e:
        print(f"   Failed to save HTML snapshot ({stage}): {e}", file=sys.stderr)


class SchwabScraper(BaseScraper):
    LOGIN_URL = "https://client.schwab.com/Login/SignOn/CustomerCenterLogin.aspx"

    # Known API base for transaction history
    TXN_API_BASE = (
        "https://ausgateway.schwab.com/api/is/transactionhistory"
    )

    def __init__(self, headless: bool = True):
        super().__init__(headless=headless)

        self.transaction_account = os.environ.get("TILL_SCHWAB_TRANSACTION_ACCOUNT", "")

        include_str = os.environ.get("TILL_SCHWAB_INCLUDE_ACCOUNTS", "")
        self.include_accounts = [s.strip() for s in include_str.split(",") if s.strip()] if include_str else []

    async def navigate_and_login(self, page, username: str, password: str):
        # Try accounts page first -- session cookies may still be valid
        print("   Checking for active session...", file=sys.stderr)
        try:
            await page.goto(
                "https://client.schwab.com/app/accounts/summary/",
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
        _save_html(await page.content(), "session_check")

        # Session is valid if we stayed on the accounts page (not redirected to login)
        if "client.schwab.com/app/accounts" in current_url and "login" not in current_url.lower():
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired -- go to login page
        print("   Session expired, logging in...", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        _save_html(await page.content(), "login_page")

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source schwab` "
                "or set TILL_SCHWAB_USERNAME/TILL_SCHWAB_PASSWORD env vars."
            )

        print("   Auto-filling login credentials...", file=sys.stderr)
        try:
            await page.wait_for_timeout(1000)

            # Login form is inside an iframe
            frame = None
            for selector in [
                'iframe[title="log in form"]',
                'iframe#lmsIframe',
                'iframe[aria-label="Login widget"]',
            ]:
                try:
                    frame = page.frame_locator(selector)
                    await frame.locator('input').first.wait_for(timeout=3000)
                    print(f"   Found login iframe: {selector}", file=sys.stderr)
                    break
                except Exception:
                    frame = None

            if frame is None:
                print("   No iframe found, trying direct page login", file=sys.stderr)
                await page.locator('#loginIdInput').click()
                await page.locator('#loginIdInput').fill("")
                await page.locator('#loginIdInput').type(username, delay=50)
                await page.wait_for_timeout(300)
                await page.locator('#passwordInput').click()
                await page.locator('#passwordInput').type(password, delay=30)
                await page.wait_for_timeout(300)
                await page.click('#btnLogin')
            else:
                print("   Entering username...", file=sys.stderr)
                try:
                    f = frame.get_by_role("textbox", name="Login ID")
                    await f.click()
                    await f.fill("")
                    await f.type(username, delay=50)
                except Exception:
                    f = frame.locator('#loginIdInput')
                    await f.click()
                    await f.fill("")
                    await f.type(username, delay=50)
                await page.wait_for_timeout(500)

                print("   Entering password...", file=sys.stderr)
                try:
                    f = frame.get_by_role("textbox", name="Password")
                    await f.click()
                    await f.type(password, delay=30)
                except Exception:
                    f = frame.locator('#passwordInput')
                    await f.click()
                    await f.type(password, delay=30)
                await page.wait_for_timeout(500)

                print("   Clicking login...", file=sys.stderr)
                try:
                    await frame.get_by_role("button", name="Log in").click()
                except Exception:
                    await frame.locator('#btnLogin').click()

            print("   Waiting for login...", file=sys.stderr)
            try:
                await page.wait_for_url("**/client.schwab.com/**", timeout=30000)
            except Exception:
                pass

            if "client.schwab.com/app/accounts" in page.url:
                print("   Login successful!", file=sys.stderr)
            else:
                print(f"   Post-login URL: {page.url}", file=sys.stderr)
                print("   Waiting for 2FA...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_schwab_2fa.png")
                try:
                    await page.wait_for_url("**/app/accounts/**", timeout=30000)
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await page.screenshot(path="/tmp/till_schwab_login.png")
                    _save_html(await page.content(), "login_failed")
                    raise Exception(
                        f"Login failed at URL: {page.url}. "
                        "Run `till test --source schwab --headful --pause` to re-auth."
                    )

        except Exception as e:
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_schwab_login.png")
            raise

        _save_html(await page.content(), "post_login")
        await page.wait_for_timeout(2000)

    async def extract(self, page) -> dict:
        """Extract accounts via DOM and transactions via API."""

        # Step 1: Set up API interception for transaction data only
        api_responses = {}

        async def capture_api(response):
            url = response.url
            if '/api/' in url and response.status == 200:
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

        # Step 2: Navigate to summary page to load accounts
        print("   Loading account summary...", file=sys.stderr)
        await page.goto(
            "https://client.schwab.com/app/accounts/summary/",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await page.wait_for_timeout(5000)

        await page.screenshot(path="/tmp/till_schwab_accounts.png")
        _save_html(await page.content(), "accounts_summary")

        # Step 3: Extract accounts from DOM (API returns $0 balances with base64 suffixes)
        accounts = await self._extract_accounts_dom(page)

        if not accounts:
            print(
                "   No accounts found in DOM. URL: " + page.url + ". "
                "Run `till test --source schwab --headful --pause` to re-auth.",
                file=sys.stderr,
            )

        # Filter by include_accounts (match suffix OR name substring)
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

        # Step 4: Get transactions via direct API call, then interception fallback
        transactions = []
        if self.transaction_account:
            api_responses.clear()
            transactions = await self._extract_transactions_api(page, api_responses)

        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

        # Build results
        account_results = []
        for acct in accounts:
            suffix = acct.get("account_suffix", "")
            id_key = f"schwab_{suffix}" if suffix else acct["name"]
            account_id = hashlib.md5(id_key.encode()).hexdigest()[:16]
            account_results.append({
                "account_id": account_id,
                "account_name": f"{acct['name']} ...{suffix}" if suffix else acct["name"],
                "account_type": acct.get("type", "other"),
                "balance": acct["balance"],
                "day_change": acct.get("day_change"),
                "day_change_percent": acct.get("day_change_percent"),
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
            "source": "schwab",
            "accounts": account_results,
            "transactions": txn_results,
            "positions": [],
            "balance_history": [],
        }

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Extract accounts from DOM -- the reliable method for Schwab balances."""
        accounts = []
        wrapper = await page.query_selector('div.allAccountsWrapper') or page

        rows = await wrapper.query_selector_all('sdps-table-row, table tbody tr')
        for row in rows:
            try:
                is_header = await row.get_attribute('account-list-table-group-header')
                if is_header is not None:
                    continue

                sr_el = await row.query_selector('span.sr-only')
                suffix = None
                if sr_el:
                    sr_text = await sr_el.inner_text()
                    m = re.search(r'Account number ending in\s+(\S+)', sr_text)
                    if m:
                        suffix = m.group(1)
                if not suffix:
                    continue

                cells = await row.query_selector_all('sdps-table-cell, td')
                if len(cells) < 4:
                    continue

                texts = [await c.inner_text() for c in cells]
                texts = [t.strip() for t in texts]

                name = texts[0].split('\n')[0].strip().rstrip('\u2020').strip()
                if not name or 'Total' in name:
                    continue

                acct_type = self._infer_type_from_columns(texts[1], name)

                value = None
                day_chg = None
                day_pct = None
                for i, t in enumerate(texts[1:], 1):
                    dm = re.search(r'([+-])?\$?([\d,]+\.?\d*)', t)
                    if dm:
                        amt = float(dm.group(2).replace(',', ''))
                        if dm.group(1) == '-':
                            amt = -amt
                        if i == 3:
                            value = amt
                        elif i == 4:
                            day_chg = amt
                    pm = re.search(r'([+-]?\d+\.?\d*)%', t)
                    if pm:
                        day_pct = float(pm.group(1))

                print(f"   DOM: {name} ...{suffix}: ${value or 0:,.2f} ({acct_type})", file=sys.stderr)
                accounts.append({
                    "name": name,
                    "balance": value or 0.0,
                    "type": acct_type,
                    "account_suffix": suffix,
                    "day_change": day_chg,
                    "day_change_percent": day_pct,
                })
            except Exception as e:
                print(f"   DOM parse error: {e}", file=sys.stderr)
        return accounts

    async def _extract_transactions_api(self, page, api_responses: dict) -> list[dict]:
        """Get transactions via direct API call first, then interception fallback.

        Strategy:
        1. Try direct API call to ausgateway.schwab.com for the configured account
        2. Fall back to navigating to history page and intercepting API calls
        """
        transactions = []
        acct_id = hashlib.md5(f"schwab_{self.transaction_account}".encode()).hexdigest()[:16]

        # Strategy 1: Direct API call (faster, no page navigation needed)
        print("   Trying direct API call for transactions...", file=sys.stderr)
        try:
            # Schwab transaction history API -- try fetching directly with session cookies
            api_url = (
                f"{self.TXN_API_BASE}/v1/transactions"
                f"?accountNumber={self.transaction_account}"
                f"&timeFrame=Last30Days"
            )
            response = await page.request.get(api_url, timeout=15000)
            if response.ok:
                data = await response.json()
                txns = self._parse_transactions_from_api(data)
                if txns:
                    print(f"   Direct API: {len(txns)} transactions", file=sys.stderr)
                    transactions.extend(txns)
                    # Save for debugging
                    try:
                        with open("/tmp/till_schwab_api_direct.json", 'w') as f:
                            json.dump(data, f, indent=2, default=str)
                    except Exception:
                        pass
                    return transactions
            else:
                print(f"   Direct API returned {response.status}, falling back to interception", file=sys.stderr)
        except Exception as e:
            print(f"   Direct API failed: {e}, falling back to interception", file=sys.stderr)

        # Strategy 2: Navigate to history page and intercept API calls
        print("   Loading transaction history page...", file=sys.stderr)
        await page.goto(
            "https://client.schwab.com/app/accounts/history/#/",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await page.wait_for_timeout(5000)
        await page.screenshot(path="/tmp/till_schwab_history_default.png")
        _save_html(await page.content(), "history_default")

        # Capture transactions from default account load
        transactions.extend(self._collect_transactions_from_responses(api_responses))

        # Now switch to the checking account if configured
        if self.transaction_account:
            print(f"   Selecting account ...{self.transaction_account}...", file=sys.stderr)

            dropdown_btn = None
            for btn in await page.query_selector_all('button'):
                try:
                    text = await btn.inner_text()
                    if re.search(r'(?:\u2026|\.\.\.)[\d-]{2,4}', text) and await btn.is_visible():
                        dropdown_btn = btn
                        break
                except Exception:
                    continue

            if dropdown_btn:
                btn_text = (await dropdown_btn.inner_text()).strip()
                print(f"   Dropdown shows: {btn_text[:50]}", file=sys.stderr)

                if self.transaction_account not in btn_text:
                    await dropdown_btn.click()
                    await page.wait_for_timeout(1000)

                    api_responses.clear()

                    found = False
                    for elem in await page.query_selector_all('li, a, div[role="option"]'):
                        try:
                            text = await elem.inner_text()
                            if self.transaction_account in text and await elem.is_visible():
                                if 'TRANSFER' not in text.upper():
                                    print(f"   Clicking: {text.strip()[:60]}", file=sys.stderr)
                                    await elem.click()
                                    found = True
                                    break
                        except Exception:
                            continue

                    if found:
                        await page.wait_for_timeout(5000)
                        await page.screenshot(path="/tmp/till_schwab_history_checking.png")
                        _save_html(await page.content(), "history_checking")

                        new_txns = self._collect_transactions_from_responses(api_responses)
                        transactions.extend(new_txns)
                    else:
                        print(
                            f"   Could not find ...{self.transaction_account} in dropdown. "
                            f"URL: {page.url}. "
                            "Run `till test --source schwab --headful --pause` to debug.",
                            file=sys.stderr,
                        )
                else:
                    print(f"   Already showing ...{self.transaction_account}", file=sys.stderr)
            else:
                print(
                    f"   No account dropdown found on history page. URL: {page.url}. "
                    "Run `till test --source schwab --headful --pause` to debug.",
                    file=sys.stderr,
                )

        # Save captured API data for offline debugging
        if api_responses:
            debug_path = "/tmp/till_schwab_api_dump.json"
            try:
                txn_data = {
                    url: data for url, data in api_responses.items()
                    if 'transactionhistory' in url.lower() and 'ausgateway' in url.lower()
                }
                with open(debug_path, 'w') as f:
                    json.dump(txn_data, f, indent=2, default=str)
                print(f"   Saved API dump to {debug_path}", file=sys.stderr)
            except Exception:
                pass

        return transactions

    def _collect_transactions_from_responses(self, api_responses: dict) -> list[dict]:
        """Parse all transaction API responses collected so far."""
        transactions = []
        for url, data in api_responses.items():
            url_lower = url.lower()
            if 'transactionhistory' not in url_lower or 'ausgateway' not in url_lower:
                continue
            if not isinstance(data, dict):
                continue
            keys = list(data.keys())
            # Skip config/metadata responses (only have flags/profile)
            if set(keys) <= {'flags', 'profile', 'accountSelectorData'}:
                continue
            print(f"   API response keys: {keys[:10]}", file=sys.stderr)
            for k in keys:
                v = data[k]
                if isinstance(v, list):
                    sample = list(v[0].keys())[:6] if v and isinstance(v[0], dict) else str(type(v[0]) if v else 'empty')
                    print(f"     {k}: {len(v)} items, sample: {sample}", file=sys.stderr)
            txns = self._parse_transactions_from_api(data)
            if txns:
                print(f"   Parsed {len(txns)} transactions from API", file=sys.stderr)
                transactions.extend(txns)
        return transactions

    def _parse_transactions_from_api(self, data) -> list[dict]:
        """Parse transactions from Schwab's API response.

        Schwab uses different keys for different account types:
        - brokerageTransactions: transactionDate, action, symbol, description, amount
        - bankTransactions: similar structure for checking/savings
        """
        transactions = []
        items = []

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'postedTransactions', 'pendingTransactions',
                'brokerageTransactions', 'bankTransactions',
                'Transactions', 'transactions',
                'TransactionList', 'Items', 'items',
            ]:
                if key in data and isinstance(data[key], list) and data[key]:
                    items = data[key]
                    print(f"   Found {len(items)} transactions in '{key}'", file=sys.stderr)
                    break

        acct_id = hashlib.md5(f"schwab_{self.transaction_account}".encode()).hexdigest()[:16]

        for item in items:
            if not isinstance(item, dict):
                continue

            date = (
                item.get('transactionDate') or item.get('postingDate') or
                item.get('TransactionDate') or item.get('Date') or
                item.get('date') or ''
            )
            desc = (
                item.get('description') or item.get('Description') or
                item.get('TransactionDescription') or ''
            )

            # Bank transactions have separate withdrawalAmount/depositAmount fields
            withdrawal = (
                item.get('withdrawalAmount') or item.get('withdrawal') or
                item.get('Withdrawal') or ''
            )
            deposit = (
                item.get('depositAmount') or item.get('deposit') or
                item.get('Deposit') or ''
            )

            amount = None
            if withdrawal and str(withdrawal).strip():
                w = str(withdrawal).replace(',', '').replace('$', '').strip()
                if w:
                    amount = -abs(float(w))
            elif deposit and str(deposit).strip():
                d = str(deposit).replace(',', '').replace('$', '').strip()
                if d:
                    amount = abs(float(d))

            # Fall back to generic amount fields (brokerage transactions)
            if amount is None:
                raw_amount = (
                    item.get('amount') or item.get('Amount') or
                    item.get('NetAmount') or item.get('netAmount') or 0
                )
                if raw_amount:
                    amount = raw_amount

            action = item.get('action') or item.get('Action') or ''
            symbol = item.get('symbol') or item.get('Symbol') or ''

            if isinstance(amount, str):
                try:
                    amount = float(amount.replace(',', '').replace('$', ''))
                except ValueError:
                    amount = 0

            # Build description from action + symbol + description
            full_desc = desc.strip()
            if action and action not in full_desc:
                full_desc = f"{action}: {full_desc}"
            if symbol and symbol not in full_desc:
                full_desc = f"{full_desc} ({symbol})"

            if date and full_desc:
                iso_date = self._normalize_date(date)

                txn_id = hashlib.md5(f"{iso_date}_{full_desc}_{amount}".encode()).hexdigest()[:16]
                transactions.append({
                    "id": txn_id,
                    "account_id": acct_id,
                    "date": iso_date,
                    "description": full_desc,
                    "amount": float(amount),
                    "category": self._infer_category(full_desc),
                    "status": "posted",
                })

        return transactions

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Normalize a date string to ISO YYYY-MM-DD format."""
        if not date_str:
            return ""
        # Already ISO
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]
        # MM/DD/YYYY
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', date_str)
        if date_match:
            parts = date_match.group(1).split('/')
            if len(parts) == 3:
                m, d, y = parts
                if len(y) == 2:
                    y = '20' + y
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        return date_str[:10]

    @staticmethod
    def _infer_type(text: str) -> str:
        t = text.lower()
        if 'checking' in t:
            return "checking"
        if 'saving' in t:
            return "savings"
        if '401' in t:
            return "401k"
        if any(w in t for w in ['ira', 'roth', 'retirement']):
            return "ira"
        if any(w in t for w in ['brokerage', '529', 'individual']):
            return "brokerage"
        return "other"

    @staticmethod
    def _infer_type_from_columns(type_col: str, name: str) -> str:
        for line in type_col.split('\n'):
            ln = line.strip().lower()
            if ln in ['checking']:
                return "checking"
            if ln in ['savings']:
                return "savings"
            if ln in ['brokerage']:
                return "brokerage"
            if ln in ['ira', 'roth']:
                return "ira"
        return SchwabScraper._infer_type(name)

    @staticmethod
    def _infer_category(description: str) -> str:
        d = description.upper()
        if any(w in d for w in ['PAYROLL', 'DIRECT DEP', 'SALARY']):
            return "Income"
        if any(w in d for w in ['TRANSFER', 'XFER']):
            return "Transfer"
        if any(w in d for w in ['ATM', 'WITHDRAWAL']):
            return "ATM"
        if any(w in d for w in ['INTEREST', 'DIVIDEND']):
            return "Interest"
        if 'CHECK' in d:
            return "Check"
        return "Other"
