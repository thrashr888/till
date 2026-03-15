"""American Express scraper.

Strategy: Login via Playwright, then use Amex's internal APIs directly
with the session cookies. Falls back to DOM scraping.
Amex heavily detects bots — always runs headful.
"""

import re
import sys
import hashlib
import json
from pathlib import Path
from till_scrapers.base import BaseScraper


def _mask_acct(num: str) -> str:
    """Mask all but last 4-5 characters of an account number."""
    if len(num) <= 5:
        return f"...{num}"
    return f"...{num[-5:]}"


class AmexScraper(BaseScraper):
    LOGIN_URL = "https://www.americanexpress.com/en-us/account/login"
    DASHBOARD_URL = "https://global.americanexpress.com/dashboard"

    def __init__(self, headless: bool = True):
        # Amex always requires headful — heavy bot detection
        if headless:
            print("   Enforcing headful mode for Amex (bot detection).", file=sys.stderr)
        super().__init__(headless=False)

    async def _save_debug_snapshot(self, page, label: str):
        """Save screenshot + HTML for debugging."""
        try:
            await page.screenshot(path=f"/tmp/till_amex_{label}.png")
            html = await page.content()
            Path(f"/tmp/till_amex_{label}.html").write_text(html)
        except Exception:
            pass

    async def navigate_and_login(self, page, username: str, password: str):
        # Try dashboard first — session cookies may still be valid
        print("   Checking for active session...", file=sys.stderr)
        try:
            await page.goto(
                self.DASHBOARD_URL,
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

        # Session is valid if we stayed on dashboard (not redirected to login)
        if "dashboard" in current_url and "login" not in current_url.lower():
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — go to login page
        print("   Session expired, logging in...", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source amex` "
                "or set TILL_AMEX_USERNAME/TILL_AMEX_PASSWORD env vars."
            )

        print("   Auto-filling login credentials...", file=sys.stderr)
        try:
            await page.wait_for_timeout(2000)

            # Username field
            username_field = page.locator("#eliloUserID")
            await username_field.wait_for(timeout=15000)
            await username_field.click()
            await username_field.fill("")
            await username_field.type(username, delay=50)
            await page.wait_for_timeout(500)

            # Password field
            password_field = page.locator("#eliloPassword")
            await password_field.click()
            await password_field.type(password, delay=30)
            await page.wait_for_timeout(500)

            # Submit
            await page.locator("#loginSubmit").click()

            # Wait for navigation after login
            print("   Waiting for login...", file=sys.stderr)
            try:
                await page.wait_for_url("**/global.americanexpress.com/**", timeout=30000)
            except Exception:
                pass

            if "dashboard" in page.url:
                print("   Login successful!", file=sys.stderr)
            else:
                print("   Waiting for 2FA...", file=sys.stderr)
                await self._save_debug_snapshot(page, "2fa")
                try:
                    await page.wait_for_url("**/dashboard**", timeout=30000)
                    print("   Login successful after 2FA!", file=sys.stderr)
                except Exception:
                    await self._save_debug_snapshot(page, "login_fail")
                    raise Exception(
                        f"Login failed. URL: {page.url}  "
                        f"-- Try running headful with --pause"
                    )

        except Exception as e:
            await self._save_debug_snapshot(page, "login_fail")
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            raise

        await page.wait_for_timeout(2000)
        await self._save_debug_snapshot(page, "post_login")

    async def extract(self, page) -> dict:
        """Extract accounts and transactions using Amex's internal APIs."""

        # Step 1: Intercept API calls
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

        # Step 2: Navigate to dashboard to trigger account API calls
        print("   Loading dashboard...", file=sys.stderr)
        await page.goto(
            self.DASHBOARD_URL,
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await page.wait_for_timeout(5000)

        # Step 2b: Try direct API calls for account data
        print("   Trying direct API calls...", file=sys.stderr)
        api_endpoints = [
            "https://global.americanexpress.com/api/servicing/v1/financials/transaction_summary",
            "https://global.americanexpress.com/api/servicing/v1/member/accounts",
            "https://global.americanexpress.com/api/servicing/v1/financials/balances",
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

        await self._save_debug_snapshot(page, "accounts")

        # Step 3: Parse accounts from API responses
        accounts = []
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['account', 'summary', 'balance', 'card']):
                accounts_from_api = self._parse_accounts_from_api(data)
                if accounts_from_api:
                    accounts.extend(accounts_from_api)

        # Fallback: DOM extraction
        if not accounts or all(a['balance'] == 0 for a in accounts):
            print("   API didn't return accounts, falling back to DOM", file=sys.stderr)
            dom_accounts = await self._extract_accounts_dom(page)
            if dom_accounts:
                accounts = dom_accounts

        # Deduplicate by account_suffix
        seen = set()
        unique_accounts = []
        for acct in accounts:
            key = acct.get("account_suffix", acct["name"])
            if key not in seen:
                seen.add(key)
                unique_accounts.append(acct)
        accounts = unique_accounts

        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Step 4: Get transactions
        transactions = []
        api_responses.clear()

        # Navigate to activity page to trigger transaction API calls
        print("   Loading activity...", file=sys.stderr)
        try:
            await page.goto(
                "https://global.americanexpress.com/activity",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(5000)
            await self._save_debug_snapshot(page, "activity")

            # Try direct API calls for transactions
            txn_endpoints = [
                "https://global.americanexpress.com/api/servicing/v1/financials/transactions?limit=50&status=posted",
                "https://global.americanexpress.com/api/servicing/v1/financials/transactions?limit=50&status=pending",
            ]
            for endpoint in txn_endpoints:
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

            # Parse transactions from API
            for url, data in api_responses.items():
                if any(k in url.lower() for k in ['transaction', 'activity', 'statement']):
                    txns = self._parse_transactions_from_api(data, accounts)
                    if txns:
                        transactions.extend(txns)

            # Fallback: DOM extraction for transactions
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
            debug_path = "/tmp/till_amex_api_dump.json"
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
            id_key = f"amex_{suffix}" if suffix else acct["name"]
            account_id = hashlib.md5(id_key.encode()).hexdigest()[:16]
            account_results.append({
                "account_id": account_id,
                "account_name": f"{acct['name']} {_mask_acct(suffix)}" if suffix else acct["name"],
                "account_type": "credit",
                "balance": acct["balance"],
                "available_credit": acct.get("available_credit"),
                "payment_due": acct.get("payment_due"),
                "payment_due_date": acct.get("payment_due_date"),
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
            "source": "amex",
            "accounts": account_results,
            "transactions": txn_results,
            "positions": [],
            "balance_history": [],
        }

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Amex's internal API response."""
        accounts = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'cardAccounts', 'accounts', 'cardSummary', 'cards',
                'accountSummary', 'summaryData', 'data',
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            if not items:
                for val in data.values():
                    if isinstance(val, list) and len(val) > 0:
                        if isinstance(val[0], dict) and any(
                            k in val[0] for k in [
                                'accountNumber', 'cardNumber', 'balance',
                                'totalBalance', 'displayAccountNumber',
                            ]
                        ):
                            items = val
                            break

        for item in items:
            if not isinstance(item, dict):
                continue

            name = (
                item.get('cardProductName') or item.get('productName') or
                item.get('accountName') or item.get('displayName') or
                item.get('cardName') or item.get('description') or ''
            )
            balance = (
                item.get('totalBalance') or item.get('balance') or
                item.get('statementBalance') or item.get('currentBalance') or
                item.get('outstandingBalance') or 0
            )
            acct_num = (
                item.get('displayAccountNumber') or item.get('accountNumber') or
                item.get('cardNumber') or item.get('lastFourDigits') or ''
            )
            available_credit = (
                item.get('availableCredit') or item.get('creditAvailable') or
                item.get('remainingCredit') or None
            )
            payment_due = (
                item.get('paymentDueAmount') or item.get('minimumPaymentDue') or
                item.get('totalMinimumPaymentDue') or None
            )
            payment_due_date = (
                item.get('paymentDueDate') or item.get('nextPaymentDueDate') or None
            )

            if isinstance(balance, str):
                try:
                    balance = float(balance.replace(',', '').replace('$', ''))
                except ValueError:
                    balance = 0
            if isinstance(available_credit, str):
                try:
                    available_credit = float(available_credit.replace(',', '').replace('$', ''))
                except ValueError:
                    available_credit = None
            if isinstance(payment_due, str):
                try:
                    payment_due = float(payment_due.replace(',', '').replace('$', ''))
                except ValueError:
                    payment_due = None

            # Extract last digits from account number
            suffix = str(acct_num).replace('-', '').replace(' ', '')[-5:] if acct_num else ""

            if name:
                print(f"   API: {name} {_mask_acct(suffix)}: ${balance:,.2f}", file=sys.stderr)
                accounts.append({
                    "name": name,
                    "balance": float(balance) if balance else 0.0,
                    "type": "credit",
                    "account_suffix": suffix,
                    "available_credit": available_credit,
                    "payment_due": payment_due,
                    "payment_due_date": payment_due_date,
                })

        return accounts

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Fallback: extract credit card accounts from DOM."""
        accounts = []

        # Amex dashboard shows card tiles with balance info
        for selector in [
            '[data-testid*="account"]', '[class*="card-chapter"]',
            'section[class*="card"]',
        ]:
            cards = await page.query_selector_all(selector)
            if cards:
                print(f"   Found {len(cards)} card elements with: {selector}", file=sys.stderr)
                for card in cards:
                    try:
                        text = await card.inner_text()
                        lines = [l.strip() for l in text.split("\n") if l.strip()]
                        if not lines:
                            continue

                        name = lines[0]
                        balance = None
                        available_credit = None

                        for line in lines:
                            if "$" in line:
                                amount = self._parse_dollar(line)
                                if amount is not None:
                                    if "available" in line.lower() or "credit" in line.lower():
                                        available_credit = amount
                                    elif "payment" in line.lower() or "due" in line.lower():
                                        pass  # skip payment amounts for balance
                                    elif balance is None:
                                        balance = amount

                        if balance is not None:
                            suffix_match = re.search(r'[\-\s*x](\d{4,5})\b', text)
                            suffix = suffix_match.group(1) if suffix_match else ""

                            print(f"   DOM: {name} {_mask_acct(suffix)}: ${balance:,.2f}", file=sys.stderr)
                            accounts.append({
                                "name": name,
                                "balance": balance,
                                "type": "credit",
                                "account_suffix": suffix,
                                "available_credit": available_credit,
                            })
                    except Exception as e:
                        print(f"   DOM parse error: {e}", file=sys.stderr)
                if accounts:
                    break

        return accounts

    def _parse_transactions_from_api(self, data, accounts: list[dict]) -> list[dict]:
        """Parse transactions from Amex's internal API response."""
        transactions = []
        items = []

        # Determine default account_id
        default_acct_id = ""
        if accounts:
            suffix = accounts[0].get("account_suffix", "")
            id_key = f"amex_{suffix}" if suffix else accounts[0]["name"]
            default_acct_id = hashlib.md5(id_key.encode()).hexdigest()[:16]

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'transactions', 'pendingTransactions', 'postedTransactions',
                'activityList', 'activities', 'transactionList',
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
                item.get('chargeDate') or item.get('postDate') or
                item.get('statementDate') or ''
            )
            desc = (
                item.get('description') or item.get('merchantName') or
                item.get('merchant', {}).get('name', '') if isinstance(item.get('merchant'), dict) else
                item.get('merchant') or ''
            )
            amount = (
                item.get('amount') or item.get('transactionAmount') or
                item.get('chargeAmount') or 0
            )

            if isinstance(amount, str):
                try:
                    amount = float(amount.replace(',', '').replace('$', ''))
                except ValueError:
                    amount = 0

            if isinstance(amount, dict):
                # Amex sometimes nests amount: {value: 123.45, currencyCode: "USD"}
                amount = amount.get('value', 0)

            if date and desc:
                # Normalize date
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

    async def _extract_transactions_dom(self, page, accounts: list[dict]) -> list[dict]:
        """Fallback: extract transactions from the activity page DOM."""
        transactions = []

        default_acct_id = ""
        if accounts:
            suffix = accounts[0].get("account_suffix", "")
            id_key = f"amex_{suffix}" if suffix else accounts[0]["name"]
            default_acct_id = hashlib.md5(id_key.encode()).hexdigest()[:16]

        # Amex transaction rows — top selectors only
        for selector in [
            '[data-testid*="transaction"]', 'tr[class*="transaction"]',
        ]:
            rows = await page.query_selector_all(selector)
            if rows:
                print(f"   Found {len(rows)} transaction rows with: {selector}", file=sys.stderr)
                break
        else:
            rows = []

        for row in rows:
            try:
                text = await row.inner_text()
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                if len(lines) < 2:
                    continue

                # Find date
                date_match = re.search(
                    r'(\d{1,2}/\d{1,2}/\d{2,4})|(\w{3}\s+\d{1,2},?\s*\d{0,4})',
                    text,
                )
                if not date_match:
                    continue

                date_str = date_match.group(0)
                iso_date = self._normalize_date(date_str)

                # Find amount
                amount = None
                for line in reversed(lines):
                    amount = self._parse_dollar(line)
                    if amount is not None:
                        break

                if amount is None:
                    continue

                # Description: skip the date line, take next meaningful line
                desc = ""
                for line in lines:
                    if line == date_str or "$" in line:
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

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Normalize various date formats to ISO (YYYY-MM-DD)."""
        # Already ISO-ish
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]

        # MM/DD/YYYY or MM/DD/YY
        m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', date_str)
        if m:
            month, day, year = m.group(1), m.group(2), m.group(3)
            if len(year) == 2:
                year = '20' + year
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # "Mar 15, 2026" or "Mar 15"
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
    def _infer_category(description: str) -> str:
        """Infer transaction category from description."""
        d = description.upper()
        if any(w in d for w in ['PAYMENT', 'THANK YOU']):
            return "Payment"
        if any(w in d for w in ['RESTAURANT', 'FOOD', 'DINING', 'GRUBHUB', 'DOORDASH', 'UBER EATS']):
            return "Dining"
        if any(w in d for w in ['GROCERY', 'WHOLE FOODS', 'TRADER JOE', 'SAFEWAY', 'COSTCO']):
            return "Groceries"
        if any(w in d for w in ['GAS', 'FUEL', 'SHELL', 'CHEVRON', 'BP ']):
            return "Gas"
        if any(w in d for w in ['AMAZON', 'AMZN']):
            return "Shopping"
        if any(w in d for w in ['UBER', 'LYFT', 'TRANSIT', 'METRO']):
            return "Transport"
        if any(w in d for w in ['NETFLIX', 'SPOTIFY', 'HULU', 'DISNEY', 'SUBSCRIPTION']):
            return "Subscription"
        if any(w in d for w in ['TRAVEL', 'AIRLINE', 'HOTEL', 'AIRBNB', 'DELTA', 'UNITED']):
            return "Travel"
        return "Other"
