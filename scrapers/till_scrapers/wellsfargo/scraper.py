"""Wells Fargo scraper.

Strategy: Login via Playwright, then use Wells Fargo's internal APIs directly
with the session cookies. Falls back to DOM scraping if API interception
doesn't capture account data.

Note: Wells Fargo has heavy bot detection — forced headful mode.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


class WellsfargoScraper(BaseScraper):
    LOGIN_URL = "https://connect.secure.wellsfargo.com/auth/login/present"
    DASHBOARD_URL = "https://connect.secure.wellsfargo.com/accounts/start"

    def __init__(self, headless: bool = True):
        # Wells Fargo has heavy bot detection — force headful
        super().__init__(headless=False)

        self.transaction_account = os.environ.get("TILL_WELLSFARGO_TRANSACTION_ACCOUNT", "")

        include_str = os.environ.get("TILL_WELLSFARGO_INCLUDE_ACCOUNTS", "")
        self.include_accounts = [s.strip() for s in include_str.split(",") if s.strip()] if include_str else []

    async def navigate_and_login(self, page, username: str, password: str):
        # Try accounts page first — session cookies may still be valid
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

        # Session is valid if we stayed on accounts page (not redirected to login)
        if "accounts" in current_url and "login" not in current_url.lower() and "auth" not in current_url.lower():
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — go to login page
        print("   Session expired, logging in...", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=300000)
        await page.wait_for_timeout(2000)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source wellsfargo` "
                "or set TILL_USERNAME/TILL_PASSWORD env vars."
            )

        print("   Auto-filling login credentials...", file=sys.stderr)
        try:
            # Wait for login form
            await page.wait_for_selector('#j_username', timeout=15000)

            # Username
            print("   Entering username...", file=sys.stderr)
            await page.locator('#j_username').click()
            await page.locator('#j_username').fill("")
            await page.locator('#j_username').type(username, delay=50)
            await page.wait_for_timeout(500)

            # Password
            print("   Entering password...", file=sys.stderr)
            await page.locator('#j_password').click()
            await page.locator('#j_password').type(password, delay=30)
            await page.wait_for_timeout(500)

            # Submit
            print("   Clicking login...", file=sys.stderr)
            await page.locator('#submitButton').click()

            print("   Waiting for login...", file=sys.stderr)
            try:
                await page.wait_for_url("**/accounts/**", timeout=120000)
            except Exception:
                pass

            if "accounts" in page.url and "login" not in page.url.lower():
                print("   Login successful!", file=sys.stderr)
            else:
                print(f"   Post-login: {page.url}", file=sys.stderr)
                print("   Waiting for 2FA...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_wellsfargo_2fa.png")
                try:
                    await page.wait_for_url("**/accounts/**", timeout=120000)
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await page.screenshot(path="/tmp/till_wellsfargo_login.png")
                    raise Exception(f"Login failed. URL: {page.url}")

        except Exception as e:
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_wellsfargo_login.png")
            raise

        await page.wait_for_timeout(2000)

    async def extract(self, page) -> dict:
        """Extract accounts and transactions using Wells Fargo's internal APIs."""

        # Step 1: Intercept API calls
        api_responses = {}

        async def capture_api(response):
            url = response.url
            if any(k in url for k in ['/api/', '/apis/', '/services/', '/das/', '/gw/']):
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

        # Step 2: Navigate to accounts summary to trigger API calls
        print("   Loading account summary...", file=sys.stderr)
        await page.goto(
            self.DASHBOARD_URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)

        await page.screenshot(path="/tmp/till_wellsfargo_accounts.png")

        # Step 3: Parse accounts from intercepted API responses
        accounts = []
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['account', 'summary', 'balance', 'portfolio']):
                accts = self._parse_accounts_from_api(data)
                if accts:
                    accounts.extend(accts)

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

        # Step 4: Get transactions if a transaction account is configured
        transactions = []
        if self.transaction_account:
            api_responses.clear()
            transactions = await self._extract_transactions(page, api_responses)

        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

        # Save API dump for debugging
        if api_responses:
            debug_path = "/tmp/till_wellsfargo_api_dump.json"
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
            id_key = f"wellsfargo_{suffix}" if suffix else acct["name"]
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
            "source": "wellsfargo",
            "accounts": account_results,
            "transactions": txn_results,
            "positions": [],
            "balance_history": [],
        }

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Wells Fargo's internal API response."""
        accounts = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'Accounts', 'accounts', 'accountList', 'AccountList',
                'accountSummary', 'summary', 'Items', 'items', 'data',
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
                                'balance', 'Balance', 'availableBalance',
                                'currentBalance', 'totalBalance',
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
                item.get('nickName') or item.get('productName') or ''
            )
            balance = (
                item.get('currentBalance') or item.get('CurrentBalance') or
                item.get('availableBalance') or item.get('AvailableBalance') or
                item.get('totalBalance') or item.get('TotalBalance') or
                item.get('balance') or item.get('Balance') or
                item.get('accountValue') or item.get('marketValue') or 0
            )
            acct_num = (
                item.get('accountNumber') or item.get('AccountNumber') or
                item.get('accountId') or item.get('AccountId') or
                item.get('maskedAccountNumber') or ''
            )

            if isinstance(balance, str):
                try:
                    balance = float(balance.replace(',', '').replace('$', ''))
                except ValueError:
                    balance = 0

            suffix = str(acct_num)[-4:] if acct_num else ""

            acct_type = self._infer_type(
                item.get('accountType', '') or item.get('AccountType') or
                item.get('productType', '') or name
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

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Fallback: extract accounts from DOM on Wells Fargo accounts page."""
        accounts = []

        # Wells Fargo groups accounts by type (checking, savings, etc.)
        for selector in [
            '.account-tile',
            '.account-card',
            '[data-testid*="account"]',
            '.account-row',
            'div[class*="AccountTile"]',
            'div[class*="account-summary"]',
            '.a-account',
        ]:
            rows = await page.query_selector_all(selector)
            if rows:
                print(f"   DOM: found {len(rows)} rows with {selector}", file=sys.stderr)
                for row in rows:
                    try:
                        acct = await self._parse_dom_account(row)
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
                    acct = await self._parse_dom_table_row(row)
                    if acct:
                        accounts.append(acct)
                except Exception:
                    continue

        # Final fallback: scan for any elements with dollar amounts near account names
        if not accounts:
            print("   DOM: trying broad scan for accounts...", file=sys.stderr)
            # Look for common WF account display patterns
            elements = await page.query_selector_all('a[href*="account"], div[class*="account" i]')
            for elem in elements:
                try:
                    acct = await self._parse_dom_account(elem)
                    if acct and acct["balance"] > 0:
                        accounts.append(acct)
                except Exception:
                    continue

        return accounts

    async def _parse_dom_account(self, elem) -> dict | None:
        """Parse a single account element."""
        text = await elem.inner_text()
        if not text or not text.strip():
            return None

        lines = [l.strip() for l in text.split('\n') if l.strip()]
        if len(lines) < 2:
            return None

        name = lines[0]
        if 'Total' in name or not name or len(name) < 3:
            return None

        # Look for account number suffix
        suffix = ""
        for line in lines:
            m = re.search(r'(?:\.{2,3}|ending\s+in\s+|x{2,}|\*{2,})(\d{3,4})', line, re.IGNORECASE)
            if m:
                suffix = m.group(1)
                break

        # Look for dollar amount as balance
        balance = 0.0
        for line in lines:
            m = re.search(r'\$?([\d,]+\.\d{2})', line)
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
            m = re.search(r'(?:\.{2,3}|ending\s+in\s+|x{2,}|\*{2,})(\d{3,4})', t, re.IGNORECASE)
            if m:
                suffix = m.group(1)
                break

        balance = 0.0
        for t in texts[1:]:
            m = re.search(r'\$?([\d,]+\.\d{2})', t)
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

    async def _extract_transactions(self, page, api_responses: dict) -> list[dict]:
        """Navigate to account activity and capture transactions."""
        transactions = []

        # Try navigating to the activity/transactions page
        print("   Loading transaction history...", file=sys.stderr)
        try:
            await page.goto(
                "https://connect.secure.wellsfargo.com/accounts/start",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.wait_for_timeout(5000)

            # Click on the target account to view its activity
            if self.transaction_account:
                print(f"   Looking for account ...{self.transaction_account}...", file=sys.stderr)
                found = False
                for elem in await page.query_selector_all('a, button, div[role="link"]'):
                    try:
                        text = await elem.inner_text()
                        if self.transaction_account in text and await elem.is_visible():
                            print(f"   Clicking: {text.strip()[:60]}", file=sys.stderr)
                            await elem.click()
                            found = True
                            break
                    except Exception:
                        continue

                if found:
                    await page.wait_for_timeout(8000)
                    await page.screenshot(path="/tmp/till_wellsfargo_history.png")
                else:
                    print(f"   Could not find account ...{self.transaction_account}", file=sys.stderr)
        except Exception as e:
            print(f"   Transaction navigation error: {e}", file=sys.stderr)

        # Parse transactions from API responses
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['transaction', 'activity', 'history']):
                txns = self._parse_transactions_from_api(data)
                if txns:
                    print(f"   Parsed {len(txns)} transactions from API", file=sys.stderr)
                    transactions.extend(txns)

        if transactions:
            return transactions

        # Fallback: DOM extraction
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

    def _parse_transactions_from_api(self, data) -> list[dict]:
        """Parse transactions from Wells Fargo's internal API response."""
        transactions = []
        items = []

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'transactions', 'Transactions', 'transactionList',
                'postedTransactions', 'pendingTransactions',
                'activity', 'Activity', 'Items', 'items', 'data',
            ]:
                if key in data and isinstance(data[key], list) and data[key]:
                    items = data[key]
                    print(f"   Found {len(items)} transactions in '{key}'", file=sys.stderr)
                    break

        acct_id = hashlib.md5(f"wellsfargo_{self.transaction_account}".encode()).hexdigest()[:16]

        for item in items:
            if not isinstance(item, dict):
                continue

            date = (
                item.get('transactionDate') or item.get('postingDate') or
                item.get('TransactionDate') or item.get('date') or
                item.get('Date') or item.get('postedDate') or ''
            )
            desc = (
                item.get('description') or item.get('Description') or
                item.get('transactionDescription') or item.get('payeeName') or ''
            )
            amount = (
                item.get('amount') or item.get('Amount') or
                item.get('transactionAmount') or 0
            )

            if isinstance(amount, str):
                try:
                    amount = float(amount.replace(',', '').replace('$', ''))
                except ValueError:
                    amount = 0

            # Handle debit/credit indicators
            txn_type = (
                item.get('transactionType') or item.get('type') or
                item.get('creditDebitIndicator') or ''
            ).lower()
            if txn_type in ['debit', 'withdrawal'] and amount > 0:
                amount = -amount

            if date and desc:
                # Normalize date to ISO format
                iso_date = date[:10]
                date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', date)
                if date_match:
                    raw = date_match.group(1)
                    sep = '/' if '/' in raw else '-'
                    parts = raw.split(sep)
                    if len(parts) == 3:
                        m, d, y = parts
                        if len(y) == 2:
                            y = '20' + y
                        iso_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

                txn_id = hashlib.md5(f"{iso_date}_{desc}_{amount}".encode()).hexdigest()[:16]
                transactions.append({
                    "id": txn_id,
                    "account_id": acct_id,
                    "date": iso_date,
                    "description": desc.strip(),
                    "amount": float(amount),
                    "category": self._infer_category(desc),
                    "status": "posted",
                })

        return transactions

    async def _extract_transactions_dom(self, page) -> list[dict]:
        """Scrape transactions from the visible DOM table."""
        transactions = []
        acct_id = hashlib.md5(f"wellsfargo_{self.transaction_account}".encode()).hexdigest()[:16]

        rows = await page.query_selector_all('table tbody tr')
        for row in rows:
            try:
                cells = await row.query_selector_all('td')
                if len(cells) < 3:
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

                # Description is typically the second column
                desc = texts[1] if len(texts) > 1 else ""

                # Amount — try last columns (withdrawal/deposit or single amount)
                amount = self._parse_signed_amount(texts[-1])
                if amount is None and len(texts) >= 4:
                    w = self._parse_amount(texts[-2])
                    dep = self._parse_amount(texts[-1])
                    if w:
                        amount = -w
                    elif dep:
                        amount = dep

                if amount is None or amount == 0:
                    continue

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
        if any(w in t for w in ['brokerage', 'investment', 'individual']):
            return "brokerage"
        if any(w in t for w in ['credit', 'card']):
            return "credit_card"
        if 'mortgage' in t or 'home' in t:
            return "mortgage"
        return "other"

    @staticmethod
    def _infer_category(description: str) -> str:
        d = description.upper()
        if any(w in d for w in ['PAYROLL', 'DIRECT DEP', 'SALARY', 'EMPLOYER']):
            return "Income"
        if any(w in d for w in ['TRANSFER', 'XFER', 'TFR']):
            return "Transfer"
        if any(w in d for w in ['ATM', 'WITHDRAWAL', 'CASH']):
            return "ATM"
        if any(w in d for w in ['INTEREST', 'DIVIDEND', 'INT PAYMENT']):
            return "Interest"
        if 'CHECK' in d:
            return "Check"
        if any(w in d for w in ['PURCHASE', 'POS', 'DEBIT CARD']):
            return "Purchase"
        if any(w in d for w in ['BILL PAY', 'ONLINE PMT', 'PAYMENT']):
            return "Bill Payment"
        return "Other"
