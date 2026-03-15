"""Charles Schwab scraper.

Strategy: Login via Playwright, then use Schwab's internal APIs directly
with the session cookies. No DOM scraping of tables — just API calls.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


class SchwabScraper(BaseScraper):
    LOGIN_URL = "https://client.schwab.com/Login/SignOn/CustomerCenterLogin.aspx"

    def __init__(self, headless: bool = True):
        super().__init__(headless=headless)

        self.transaction_account = os.environ.get("TILL_SCHWAB_TRANSACTION_ACCOUNT", "")

        include_str = os.environ.get("TILL_SCHWAB_INCLUDE_ACCOUNTS", "")
        self.include_accounts = [s.strip() for s in include_str.split(",") if s.strip()] if include_str else []

    async def navigate_and_login(self, page, username: str, password: str):
        # Try accounts page first — session cookies may still be valid
        print("   Checking for active session...", file=sys.stderr)
        await page.goto(
            "https://client.schwab.com/app/accounts/summary/",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(2000)

        if "client.schwab.com/app/accounts" in page.url:
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — go to login page
        print(f"   Session expired, logging in...", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=300000)

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
                await page.wait_for_url("**/client.schwab.com/**", timeout=120000)
            except Exception:
                pass

            if "client.schwab.com/app/accounts" in page.url:
                print("   Login successful!", file=sys.stderr)
            else:
                print(f"   Post-login: {page.url}", file=sys.stderr)
                print("   Waiting for 2FA...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_schwab_2fa.png")
                try:
                    await page.wait_for_url("**/app/accounts/**", timeout=120000)
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await page.screenshot(path="/tmp/till_schwab_login.png")
                    raise Exception(f"Login failed. URL: {page.url}")

        except Exception as e:
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_schwab_login.png")
            raise

        await page.wait_for_timeout(2000)

    async def extract(self, page) -> dict:
        """Extract accounts and transactions using Schwab's internal APIs."""

        # Step 1: Intercept API calls to discover endpoints
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
                        # Print truncated URL for debugging
                        print(f"   API [{response.status}]: {url[:100]}", file=sys.stderr)
                except Exception:
                    pass

        page.on("response", capture_api)

        # Step 2: Navigate to summary page to trigger account API calls
        print("   Loading account summary...", file=sys.stderr)
        await page.goto(
            "https://client.schwab.com/app/accounts/summary/",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)

        await page.screenshot(path="/tmp/till_schwab_accounts.png")

        # Step 3: Try to get accounts from intercepted API responses
        accounts = []
        for url, data in api_responses.items():
            if 'Account' in url or 'account' in url:
                accounts_from_api = self._parse_accounts_from_api(data)
                if accounts_from_api:
                    accounts.extend(accounts_from_api)

        # Fallback: DOM extraction if API interception didn't find accounts with balances
        if not accounts or all(a['balance'] == 0 for a in accounts):
            print("   API didn't return account balances, falling back to DOM", file=sys.stderr)
            dom_accounts = await self._extract_accounts_dom(page)
            if dom_accounts:
                accounts = dom_accounts

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

        # Step 4: Get transactions via API or CSV download
        transactions = []
        if self.transaction_account:
            # Navigate to transaction history to trigger API calls
            api_responses.clear()
            transactions = await self._extract_transactions_api(page, api_responses)

            if not transactions:
                # Try CSV download as fallback
                transactions = await self._download_transactions_csv(page)

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

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Schwab's internal API response."""
        accounts = []

        # Handle different API response shapes
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            # Look for account arrays in common keys
            for key in ['Accounts', 'accounts', 'AccountList', 'Items', 'items']:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            # Check nested structures
            if not items:
                for val in data.values():
                    if isinstance(val, list) and len(val) > 0:
                        if isinstance(val[0], dict) and any(
                            k in val[0] for k in ['AccountNumber', 'accountNumber', 'AccountId', 'Balance', 'balance']
                        ):
                            items = val
                            break

        for item in items:
            if not isinstance(item, dict):
                continue

            name = (
                item.get('AccountName') or item.get('accountName') or
                item.get('DisplayName') or item.get('displayName') or
                item.get('Description') or item.get('NickName') or ''
            )
            balance = (
                item.get('AccountValue') or item.get('accountValue') or
                item.get('Balance') or item.get('balance') or
                item.get('TotalValue') or item.get('NetValue') or 0
            )
            acct_num = (
                item.get('AccountNumber') or item.get('accountNumber') or
                item.get('AccountId') or item.get('accountId') or ''
            )

            if isinstance(balance, str):
                try:
                    balance = float(balance.replace(',', '').replace('$', ''))
                except ValueError:
                    balance = 0

            suffix = str(acct_num)[-3:] if acct_num else ""

            acct_type = self._infer_type(
                item.get('AccountType', '') or item.get('accountType', '') or name
            )

            if name:
                print(f"   API: {name} ...{suffix}: ${balance:,.2f} ({acct_type})", file=sys.stderr)
                accounts.append({
                    "name": name,
                    "balance": float(balance) if balance else 0.0,
                    "type": acct_type,
                    "account_suffix": suffix,
                    "day_change": item.get('DayChange') or item.get('dayChange'),
                    "day_change_percent": item.get('DayChangePercent') or item.get('dayChangePercent'),
                })

        return accounts

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Fallback: extract accounts from DOM."""
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
        """Navigate to transaction history, select checking account, capture API data.

        Strategy:
        1. Go to /app/accounts/history/ (loads default account's transactions)
        2. Capture any API responses from the default load
        3. Switch to the checking account via dropdown
        4. Wait for new API responses with bank transactions
        """
        transactions = []

        print("   Loading transaction history...", file=sys.stderr)
        await page.goto(
            "https://client.schwab.com/app/accounts/history/#/",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)
        await page.screenshot(path="/tmp/till_schwab_history_default.png")

        # Capture transactions from default account load
        transactions.extend(self._collect_transactions_from_responses(api_responses))

        # Now switch to the checking account if configured
        if self.transaction_account:
            print(f"   Selecting account ...{self.transaction_account}...", file=sys.stderr)

            # Find the account dropdown button (contains …XXX pattern)
            dropdown_btn = None
            for btn in await page.query_selector_all('button'):
                try:
                    text = await btn.inner_text()
                    if re.search(r'(?:…|\.\.\.)[\d-]{2,4}', text) and await btn.is_visible():
                        dropdown_btn = btn
                        break
                except Exception:
                    continue

            if dropdown_btn:
                btn_text = (await dropdown_btn.inner_text()).strip()
                print(f"   Dropdown shows: {btn_text[:50]}", file=sys.stderr)

                if self.transaction_account not in btn_text:
                    # Need to switch accounts
                    await dropdown_btn.click()
                    await page.wait_for_timeout(2000)

                    # Clear captured responses before switching
                    api_responses.clear()

                    # Click the target account
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
                        # Wait for new API responses
                        await page.wait_for_timeout(8000)
                        await page.screenshot(path="/tmp/till_schwab_history_checking.png")

                        # Capture bank transactions
                        new_txns = self._collect_transactions_from_responses(api_responses)
                        transactions.extend(new_txns)
                    else:
                        print(f"   Could not find ...{self.transaction_account} in dropdown", file=sys.stderr)
                else:
                    print(f"   Already showing ...{self.transaction_account}", file=sys.stderr)
            else:
                print("   No account dropdown found on history page", file=sys.stderr)

        # Save captured API data for offline debugging
        if api_responses:
            debug_path = "/tmp/till_schwab_api_dump.json"
            try:
                with open(debug_path, 'w') as f:
                    # Filter to transaction-related responses
                    txn_data = {
                        url: data for url, data in api_responses.items()
                        if 'transactionhistory' in url.lower() and 'ausgateway' in url.lower()
                    }
                    json.dump(txn_data, f, indent=2, default=str)
                print(f"   Saved API dump to {debug_path}", file=sys.stderr)
            except Exception:
                pass

        if transactions:
            return transactions

        # Fallback: try DOM table
        print("   No transactions from API, trying DOM...", file=sys.stderr)
        try:
            await page.wait_for_function(
                "() => document.querySelectorAll('table tbody tr td').length > 0",
                timeout=10000,
            )
            return await self._extract_transactions_dom(page)
        except Exception:
            print("   No table rows found", file=sys.stderr)
            return []

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
            # Log what we got
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
        """Parse transactions from Schwab's intercepted API response.

        Schwab uses different keys for different account types:
        - brokerageTransactions: transactionDate, action, symbol, description, amount
        - bankTransactions: similar structure for checking/savings
        """
        transactions = []
        items = []

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            # Try all known transaction array keys
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

            # Handle various field naming conventions across brokerage/bank responses
            date = (
                item.get('transactionDate') or item.get('postingDate') or
                item.get('TransactionDate') or item.get('Date') or
                item.get('date') or ''
            )
            desc = (
                item.get('description') or item.get('Description') or
                item.get('TransactionDescription') or ''
            )
            amount = (
                item.get('amount') or item.get('Amount') or
                item.get('runningBalance') or item.get('NetAmount') or
                item.get('netAmount') or 0
            )
            # Bank transactions may have separate withdrawal/deposit fields
            withdrawal = item.get('withdrawal') or item.get('Withdrawal')
            deposit = item.get('deposit') or item.get('Deposit')
            if withdrawal and not amount:
                amount = withdrawal
                if isinstance(amount, str):
                    amount = amount.replace(',', '').replace('$', '')
                amount = -abs(float(amount)) if amount else 0
            elif deposit and not amount:
                amount = deposit
                if isinstance(amount, str):
                    amount = amount.replace(',', '').replace('$', '')
                amount = abs(float(amount)) if amount else 0

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
                # Normalize date to ISO format
                iso_date = date[:10]  # Already ISO or close to it
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', date)
                if date_match:
                    parts = date_match.group(1).split('/')
                    if len(parts) == 3:
                        m, d, y = parts
                        if len(y) == 2:
                            y = '20' + y
                        iso_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

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

    async def _extract_transactions_dom(self, page) -> list[dict]:
        """Scrape transactions from the visible DOM table."""
        transactions = []
        acct_id = hashlib.md5(f"schwab_{self.transaction_account}".encode()).hexdigest()[:16]

        rows = await page.query_selector_all('table tbody tr')
        for row in rows:
            try:
                cells = await row.query_selector_all('td')
                if len(cells) < 4:
                    continue

                texts = [await c.inner_text() for c in cells]
                texts = [t.strip() for t in texts]

                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', texts[0])
                if not date_match:
                    continue

                parts = date_match.group(1).split('/')
                if len(parts) != 3:
                    continue
                m, d, y = parts
                if len(y) == 2:
                    y = '20' + y
                date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

                # Try last column as amount (works for both brokerage and bank layouts)
                amount = self._parse_signed_amount(texts[-1])
                if amount is None and len(texts) >= 5:
                    # Bank layout: withdrawal (col 4) or deposit (col 5)
                    w = self._parse_amount(texts[-3])
                    dep = self._parse_amount(texts[-2])
                    if w:
                        amount = -w
                    elif dep:
                        amount = dep

                if amount is None or amount == 0:
                    continue

                # Description is typically col 3 (both layouts)
                desc = texts[3] if len(texts) > 3 else texts[1]

                txn_id = hashlib.md5(f"{date}_{desc}_{amount}".encode()).hexdigest()[:16]
                transactions.append({
                    "id": txn_id,
                    "account_id": acct_id,
                    "date": date,
                    "description": desc.strip(),
                    "amount": amount,
                    "category": self._infer_category(desc),
                    "status": "posted",
                })
            except Exception:
                continue

        return transactions

    async def _download_transactions_csv(self, page) -> list[dict]:
        """Download transactions via the export/CSV button."""
        transactions = []
        acct_id = hashlib.md5(f"schwab_{self.transaction_account}".encode()).hexdigest()[:16]

        try:
            # Look for download/export button on the history page
            for selector in [
                'button[aria-label*="export" i]',
                'button[aria-label*="download" i]',
                'a[aria-label*="export" i]',
                'a[aria-label*="download" i]',
                'button:has-text("Export")',
                'button:has-text("Download")',
                '[class*="download"]',
                '[class*="export"]',
            ]:
                btn = await page.query_selector(selector)
                if btn and await btn.is_visible():
                    print(f"   Found export button: {selector}", file=sys.stderr)
                    async with page.expect_download(timeout=30000) as dl_info:
                        await btn.click()
                    download = await dl_info.value
                    csv_path = await download.path()
                    if csv_path:
                        from pathlib import Path
                        content = Path(csv_path).read_text()
                        transactions = self._parse_csv_transactions(content, acct_id)
                        print(f"   Parsed {len(transactions)} transactions from CSV", file=sys.stderr)
                    break
        except Exception as e:
            print(f"   CSV download failed: {e}", file=sys.stderr)

        return transactions

    def _parse_csv_transactions(self, csv_content: str, account_id: str) -> list[dict]:
        """Parse Schwab CSV export."""
        import csv
        from io import StringIO

        transactions = []
        reader = csv.DictReader(StringIO(csv_content))

        for row in reader:
            date = row.get('Date', row.get('date', ''))
            desc = row.get('Description', row.get('description', ''))
            amount = row.get('Amount', row.get('amount', ''))
            withdrawal = row.get('Withdrawal', row.get('withdrawal', ''))
            deposit = row.get('Deposit', row.get('deposit', ''))

            # Parse date
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', date)
            if not date_match:
                continue
            parts = date_match.group(1).split('/')
            if len(parts) != 3:
                continue
            m, d, y = parts
            if len(y) == 2:
                y = '20' + y
            iso_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

            # Parse amount
            if amount:
                amt = self._parse_signed_amount(amount)
            elif withdrawal:
                w = self._parse_amount(withdrawal)
                amt = -w if w else None
            elif deposit:
                amt = self._parse_amount(deposit)
            else:
                amt = None

            if amt is None:
                continue

            txn_id = hashlib.md5(f"{iso_date}_{desc}_{amt}".encode()).hexdigest()[:16]
            transactions.append({
                "id": txn_id,
                "account_id": account_id,
                "date": iso_date,
                "description": desc.strip(),
                "amount": amt,
                "category": self._infer_category(desc),
                "status": "posted",
            })

        return transactions

    @staticmethod
    def _parse_signed_amount(text: str) -> float | None:
        if not text or not text.strip():
            return None
        match = re.search(r'(-?)\$?([\d,]+\.?\d*)', text)
        if not match:
            return None
        try:
            amount = float(match.group(2).replace(',', ''))
            if match.group(1) == '-':
                amount = -amount
            return amount
        except ValueError:
            return None

    @staticmethod
    def _parse_amount(text: str) -> float | None:
        if not text or not text.strip():
            return None
        clean = re.sub(r'[^\d,.\-]', '', text)
        if not clean or clean in ['-', '', '.']:
            return None
        try:
            return abs(float(clean.replace(',', '')))
        except ValueError:
            return None

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
            l = line.strip().lower()
            if l in ['checking']:
                return "checking"
            if l in ['savings']:
                return "savings"
            if l in ['brokerage']:
                return "brokerage"
            if l in ['ira', 'roth']:
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
