"""Bank of America scraper.

Strategy: Login via Playwright, then use BofA's internal APIs directly
with the session cookies. Falls back to DOM scraping.
BofA detects headless browsers — always runs headful.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


class BofaScraper(BaseScraper):
    LOGIN_URL = "https://www.bankofamerica.com/"
    DASHBOARD_URL = "https://secure.bankofamerica.com/myaccounts/brain/redirect.go"

    def __init__(self, headless: bool = True):
        # BofA always requires headful — heavy bot detection
        if headless:
            print("   Enforcing headful mode for BofA (bot detection).", file=sys.stderr)
        super().__init__(headless=False)

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

        # Session is valid if we're on the accounts overview (not redirected to login)
        if "myaccounts" in current_url and "login" not in current_url.lower() and "bankofamerica.com" in current_url:
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — go to login page
        print("   Session expired, logging in...", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source bofa` "
                "or set TILL_BOFA_USERNAME/TILL_BOFA_PASSWORD env vars."
            )

        print("   Auto-filling login credentials...", file=sys.stderr)
        try:
            await page.wait_for_timeout(2000)

            # Username field
            print("   Entering username...", file=sys.stderr)
            username_field = page.locator("#enterID-input")
            await username_field.wait_for(timeout=15000)
            await username_field.click()
            await username_field.fill("")
            await username_field.type(username, delay=50)
            await page.wait_for_timeout(500)

            # Password field
            print("   Entering password...", file=sys.stderr)
            password_field = page.locator("#tlpvt-passcode-input")
            await password_field.click()
            await password_field.type(password, delay=30)
            await page.wait_for_timeout(500)

            # Submit — BofA uses a sign-in button
            print("   Clicking sign in...", file=sys.stderr)
            submit_btn = None
            for selector in [
                '#enterID-submitButton',
                '#signIn',
                'button[type="submit"]',
                'input[type="submit"]',
                '#enter-btn',
            ]:
                try:
                    btn = page.locator(selector)
                    if await btn.is_visible(timeout=2000):
                        submit_btn = btn
                        break
                except Exception:
                    continue

            if submit_btn:
                await submit_btn.click()
            else:
                # Try pressing Enter as fallback
                await password_field.press("Enter")

            # Wait for navigation after login
            print("   Waiting for login...", file=sys.stderr)
            try:
                await page.wait_for_url("**/secure.bankofamerica.com/**", timeout=30000)
            except Exception:
                pass

            if "myaccounts" in page.url or "secure.bankofamerica.com" in page.url:
                print("   Login successful!", file=sys.stderr)
            else:
                print(f"   Post-login: {page.url}", file=sys.stderr)
                print("   Waiting for 2FA / security challenge...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_bofa_2fa.png")
                try:
                    await page.wait_for_url("**/myaccounts/**", timeout=30000)
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await page.screenshot(path="/tmp/till_bofa_login.png")
                    raise Exception(f"Login failed. URL: {page.url}")

        except Exception as e:
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_bofa_login.png")
            raise

        await page.wait_for_timeout(2000)

    async def extract(self, page) -> dict:
        """Extract accounts and transactions using BofA's internal APIs."""

        # Step 1: Intercept API calls
        api_responses = {}

        async def capture_api(response):
            url = response.url
            # BofA uses various API paths
            if any(p in url for p in ['/api/', '/myaccounts/', '/aries/']) and response.status == 200:
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

        # Step 2: Navigate to accounts overview
        print("   Loading accounts overview...", file=sys.stderr)
        await page.goto(
            self.DASHBOARD_URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_timeout(8000)

        await page.screenshot(path="/tmp/till_bofa_accounts.png")

        # Step 3: Parse accounts from API responses
        accounts = []
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['account', 'summary', 'balance', 'overview']):
                accounts_from_api = self._parse_accounts_from_api(data)
                if accounts_from_api:
                    accounts.extend(accounts_from_api)

        # Fallback: DOM extraction
        if not accounts or all(a['balance'] == 0 for a in accounts):
            print("   API didn't return accounts, falling back to DOM", file=sys.stderr)
            dom_accounts = await self._extract_accounts_dom(page)
            if dom_accounts:
                accounts = dom_accounts

        # Deduplicate
        seen = set()
        unique_accounts = []
        for acct in accounts:
            key = acct.get("account_suffix", acct["name"])
            if key not in seen:
                seen.add(key)
                unique_accounts.append(acct)
        accounts = unique_accounts

        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Step 4: Get transactions for each account
        transactions = []
        api_responses.clear()

        # Navigate to activity page
        print("   Loading recent activity...", file=sys.stderr)
        try:
            await page.goto(
                "https://secure.bankofamerica.com/myaccounts/brain/redirect.go?source=overview&target=acctDetails",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.wait_for_timeout(8000)
            await page.screenshot(path="/tmp/till_bofa_activity.png")

            # Parse transactions from API
            for url, data in api_responses.items():
                if any(k in url.lower() for k in ['transaction', 'activity', 'statement', 'detail']):
                    txns = self._parse_transactions_from_api(data, accounts)
                    if txns:
                        transactions.extend(txns)

            # Fallback: DOM extraction
            if not transactions:
                print("   No transactions from API, trying DOM...", file=sys.stderr)
                dom_txns = await self._extract_transactions_dom(page, accounts)
                if dom_txns:
                    transactions = dom_txns
        except Exception as e:
            print(f"   Transaction extraction error: {e}", file=sys.stderr)

        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

        # Save API dump for debugging
        if api_responses:
            debug_path = "/tmp/till_bofa_api_dump.json"
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
            id_key = f"bofa_{suffix}" if suffix else acct["name"]
            account_id = hashlib.md5(id_key.encode()).hexdigest()[:16]
            account_results.append({
                "account_id": account_id,
                "account_name": f"{acct['name']} ...{suffix}" if suffix else acct["name"],
                "account_type": acct.get("type", "other"),
                "balance": acct["balance"],
                "available_balance": acct.get("available_balance"),
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
            "source": "bofa",
            "accounts": account_results,
            "transactions": txn_results,
            "positions": [],
            "balance_history": [],
        }

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from BofA's internal API response."""
        accounts = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'accounts', 'accountDetails', 'accountSummary',
                'depositAccounts', 'creditCardAccounts', 'loanAccounts',
                'data', 'items', 'accountList',
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
                                'accountNumber', 'accountName', 'balance',
                                'currentBalance', 'availableBalance',
                            ]
                        ):
                            items = val
                            break

        for item in items:
            if not isinstance(item, dict):
                continue

            name = (
                item.get('accountName') or item.get('displayName') or
                item.get('productName') or item.get('nickName') or
                item.get('description') or ''
            )
            balance = (
                item.get('currentBalance') or item.get('balance') or
                item.get('availableBalance') or item.get('ledgerBalance') or
                item.get('accountBalance') or 0
            )
            available_balance = (
                item.get('availableBalance') or item.get('available') or None
            )
            acct_num = (
                item.get('accountNumber') or item.get('displayAccountNumber') or
                item.get('maskedAccountNumber') or item.get('lastFourDigits') or ''
            )

            if isinstance(balance, str):
                try:
                    balance = float(balance.replace(',', '').replace('$', ''))
                except ValueError:
                    balance = 0
            if isinstance(available_balance, str):
                try:
                    available_balance = float(available_balance.replace(',', '').replace('$', ''))
                except ValueError:
                    available_balance = None

            # Extract last 4 digits
            suffix = str(acct_num).replace('-', '').replace(' ', '').replace('*', '')[-4:] if acct_num else ""

            acct_type = self._infer_type(
                item.get('accountType', '') or item.get('productType', '') or name
            )

            if name:
                print(f"   API: {name} ...{suffix}: ${balance:,.2f} ({acct_type})", file=sys.stderr)
                accounts.append({
                    "name": name,
                    "balance": float(balance) if balance else 0.0,
                    "type": acct_type,
                    "account_suffix": suffix,
                    "available_balance": available_balance,
                })

        return accounts

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Fallback: extract accounts from the BofA accounts overview DOM."""
        accounts = []

        # BofA groups accounts by type: checking, savings, credit cards
        account_groups = {
            "checking": [
                '#checkingAccounts',
                '.account-group-checking',
                '[data-account-type="checking"]',
            ],
            "savings": [
                '#savingsAccounts',
                '.account-group-savings',
                '[data-account-type="savings"]',
            ],
            "credit": [
                '#creditCardAccounts',
                '.account-group-credit',
                '[data-account-type="credit"]',
            ],
        }

        # Try grouped extraction first
        for acct_type, selectors in account_groups.items():
            for selector in selectors:
                group = await page.query_selector(selector)
                if group:
                    rows = await group.query_selector_all(
                        '.account-row, .account-item, tr, [class*="account"]'
                    )
                    for row in rows:
                        acct = await self._parse_account_row(row, acct_type)
                        if acct:
                            accounts.append(acct)
                    break

        # Fallback: generic account extraction
        if not accounts:
            print("   Trying generic account selectors...", file=sys.stderr)
            generic_selectors = [
                '.account-tile',
                '.AccountItem',
                '[class*="AccountItem"]',
                '.account-info',
                'li[class*="account"]',
            ]
            for selector in generic_selectors:
                rows = await page.query_selector_all(selector)
                if rows:
                    print(f"   Found {len(rows)} account elements with: {selector}", file=sys.stderr)
                    for row in rows:
                        acct = await self._parse_account_row(row, "other")
                        if acct:
                            accounts.append(acct)
                    break

        # Last-resort: scan the full page for account patterns
        if not accounts:
            print("   Last-resort fallback: scanning full page", file=sys.stderr)
            accounts = await self._extract_accounts_fullpage(page)

        return accounts

    async def _parse_account_row(self, element, default_type: str) -> dict | None:
        """Parse a single account row element."""
        try:
            text = await element.inner_text()
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            if not lines:
                return None

            name = lines[0]
            balance = None
            available_balance = None

            for line in lines:
                if "$" in line:
                    amount = self._parse_dollar(line)
                    if amount is not None:
                        if "available" in line.lower():
                            available_balance = amount
                        elif balance is None:
                            balance = amount

            if balance is None:
                return None

            # Extract last 4 digits
            suffix_match = re.search(r'[\-\s*x](\d{4})\b', text)
            suffix = suffix_match.group(1) if suffix_match else ""

            # Infer type from text if generic
            acct_type = default_type
            if acct_type == "other":
                acct_type = self._infer_type(text)

            print(f"   DOM: {name} ...{suffix}: ${balance:,.2f} ({acct_type})", file=sys.stderr)
            return {
                "name": name,
                "balance": balance,
                "type": acct_type,
                "account_suffix": suffix,
                "available_balance": available_balance,
            }
        except Exception as e:
            print(f"   DOM parse error: {e}", file=sys.stderr)
            return None

    async def _extract_accounts_fullpage(self, page) -> list[dict]:
        """Last-resort: extract accounts by scanning the full page text."""
        accounts = []
        try:
            content = await page.content()

            # Look for checking/savings patterns
            patterns = [
                (r'(Checking)[^$]*\$?([\d,]+\.?\d*)', 'checking'),
                (r'(Savings)[^$]*\$?([\d,]+\.?\d*)', 'savings'),
                (r'(Credit\s*Card)[^$]*\$?([\d,]+\.?\d*)', 'credit'),
            ]
            for pattern, acct_type in patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    name_text, balance_str = match
                    try:
                        balance = float(balance_str.replace(',', ''))
                        if balance > 0:
                            accounts.append({
                                "name": f"BofA {name_text.strip()}",
                                "balance": balance,
                                "type": acct_type,
                                "account_suffix": "",
                            })
                    except ValueError:
                        continue
        except Exception:
            pass
        return accounts

    def _parse_transactions_from_api(self, data, accounts: list[dict]) -> list[dict]:
        """Parse transactions from BofA's internal API response."""
        transactions = []
        items = []

        # Determine default account_id
        default_acct_id = ""
        if accounts:
            suffix = accounts[0].get("account_suffix", "")
            id_key = f"bofa_{suffix}" if suffix else accounts[0]["name"]
            default_acct_id = hashlib.md5(id_key.encode()).hexdigest()[:16]

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'transactions', 'postedTransactions', 'pendingTransactions',
                'transactionList', 'recentTransactions', 'activityList',
                'data', 'items',
            ]:
                if key in data and isinstance(data[key], list) and data[key]:
                    items = data[key]
                    print(f"   Found {len(items)} transactions in '{key}'", file=sys.stderr)
                    break

        for item in items:
            if not isinstance(item, dict):
                continue

            date = (
                item.get('date') or item.get('transactionDate') or
                item.get('postDate') or item.get('postingDate') or ''
            )
            desc = (
                item.get('description') or item.get('payeeName') or
                item.get('merchantName') or item.get('transactionDescription') or ''
            )
            amount = (
                item.get('amount') or item.get('transactionAmount') or 0
            )

            # BofA may have separate debit/credit fields
            if not amount:
                debit = item.get('debitAmount') or item.get('withdrawal') or ''
                credit = item.get('creditAmount') or item.get('deposit') or ''
                if debit:
                    if isinstance(debit, str):
                        debit = debit.replace(',', '').replace('$', '').strip()
                    if debit:
                        amount = -abs(float(debit))
                elif credit:
                    if isinstance(credit, str):
                        credit = credit.replace(',', '').replace('$', '').strip()
                    if credit:
                        amount = abs(float(credit))

            if isinstance(amount, str):
                try:
                    amount = float(amount.replace(',', '').replace('$', ''))
                except ValueError:
                    amount = 0

            if date and desc:
                iso_date = self._normalize_date(date)

                txn_id = hashlib.md5(f"{iso_date}_{desc}_{amount}".encode()).hexdigest()[:16]

                status = "posted"
                if item.get('isPending') or item.get('pending') or item.get('status', '').lower() == 'pending':
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

    async def _extract_transactions_dom(self, page, accounts: list[dict]) -> list[dict]:
        """Fallback: extract transactions from the activity page DOM."""
        transactions = []

        default_acct_id = ""
        if accounts:
            suffix = accounts[0].get("account_suffix", "")
            id_key = f"bofa_{suffix}" if suffix else accounts[0]["name"]
            default_acct_id = hashlib.md5(id_key.encode()).hexdigest()[:16]

        # BofA transaction table rows
        selectors = [
            'table.activity-table tbody tr',
            '[class*="transaction-row"]',
            '[class*="activity-row"]',
            'tr[class*="trans"]',
            '.transaction-records tr',
        ]

        rows = []
        for selector in selectors:
            rows = await page.query_selector_all(selector)
            if rows:
                print(f"   Found {len(rows)} transaction rows with: {selector}", file=sys.stderr)
                break

        for row in rows:
            try:
                cells = await row.query_selector_all('td')
                if len(cells) < 3:
                    # Try inner_text fallback
                    text = await row.inner_text()
                    txn = self._parse_transaction_text(text, default_acct_id)
                    if txn:
                        transactions.append(txn)
                    continue

                texts = [await c.inner_text() for c in cells]
                texts = [t.strip() for t in texts]

                # Find date
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', texts[0])
                if not date_match:
                    continue

                iso_date = self._normalize_date(date_match.group(1))

                # Description is typically the second column
                desc = texts[1] if len(texts) > 1 else ""

                # Amount: try last column, or separate debit/credit columns
                amount = self._parse_signed_amount(texts[-1])
                if amount is None and len(texts) >= 4:
                    # Debit column (negative) and credit column (positive)
                    debit = self._parse_amount(texts[-2])
                    credit = self._parse_amount(texts[-1])
                    if debit:
                        amount = -debit
                    elif credit:
                        amount = credit

                if amount is None or not desc:
                    continue

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

    def _parse_transaction_text(self, text: str, account_id: str) -> dict | None:
        """Parse a transaction from raw text (fallback for non-table rows)."""
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        if len(lines) < 2:
            return None

        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', text)
        if not date_match:
            return None

        iso_date = self._normalize_date(date_match.group(1))

        # Find amount
        amount = None
        for line in reversed(lines):
            amount = self._parse_signed_amount(line)
            if amount is not None:
                break

        if amount is None:
            return None

        # Description: skip date line and amount line
        desc = ""
        for line in lines:
            if line == date_match.group(0) or "$" in line:
                continue
            if len(line) > 3:
                desc = line
                break

        if not desc:
            return None

        txn_id = hashlib.md5(f"{iso_date}_{desc}_{amount}".encode()).hexdigest()[:16]
        return {
            "id": txn_id,
            "account_id": account_id,
            "date": iso_date,
            "description": desc.strip(),
            "amount": float(amount),
            "category": self._infer_category(desc),
            "status": "posted",
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
    def _parse_dollar(text: str) -> float | None:
        """Parse a dollar amount from text."""
        match = re.search(r'([+-])?\$?([\d,]+\.?\d*)', text)
        if match:
            sign = match.group(1) or ''
            amount = float(match.group(2).replace(',', ''))
            return -amount if sign == '-' else amount
        return None

    @staticmethod
    def _parse_signed_amount(text: str) -> float | None:
        """Parse a signed dollar amount."""
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
        """Parse an unsigned dollar amount."""
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
        """Infer account type from text."""
        t = text.lower()
        if 'checking' in t:
            return "checking"
        if 'saving' in t:
            return "savings"
        if any(w in t for w in ['credit', 'card']):
            return "credit"
        if any(w in t for w in ['cd', 'certificate']):
            return "cd"
        if any(w in t for w in ['mortgage', 'loan', 'auto']):
            return "loan"
        if any(w in t for w in ['ira', 'retirement', '401']):
            return "retirement"
        return "other"

    @staticmethod
    def _infer_category(description: str) -> str:
        """Infer transaction category from description."""
        d = description.upper()
        if any(w in d for w in ['PAYROLL', 'DIRECT DEP', 'SALARY', 'EMPLOYER']):
            return "Income"
        if any(w in d for w in ['TRANSFER', 'XFER', 'ZELLE']):
            return "Transfer"
        if any(w in d for w in ['ATM', 'WITHDRAWAL', 'CASH']):
            return "ATM"
        if any(w in d for w in ['INTEREST', 'DIVIDEND']):
            return "Interest"
        if 'CHECK' in d and 'CHECKING' not in d:
            return "Check"
        if any(w in d for w in ['PAYMENT', 'BILL PAY']):
            return "Payment"
        if any(w in d for w in ['RESTAURANT', 'FOOD', 'DINING']):
            return "Dining"
        if any(w in d for w in ['GROCERY', 'WHOLE FOODS', 'TRADER JOE']):
            return "Groceries"
        if any(w in d for w in ['GAS', 'FUEL', 'SHELL', 'CHEVRON']):
            return "Gas"
        if any(w in d for w in ['AMAZON', 'AMZN']):
            return "Shopping"
        return "Other"
