"""Fidelity scraper.

Strategy: Login via Playwright, then try Fidelity's internal APIs directly
with the session cookies. Falls back to API response interception, then
DOM scraping if direct API calls don't return data.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


# Known Fidelity internal API endpoints (relative to digital.fidelity.com)
_PORTFOLIO_API = "https://digital.fidelity.com/ftgw/digital/portfolio/api/portfolio-summary"
_POSITIONS_API = "https://digital.fidelity.com/ftgw/digital/portfolio/api/positions"
_ACTIVITY_API = "https://digital.fidelity.com/ftgw/digital/activity/api/activities"
_BALANCES_API = "https://digital.fidelity.com/ftgw/digital/portfolio/api/balances"

# Activity/history page for transaction scraping
_ACTIVITY_URL = "https://digital.fidelity.com/ftgw/digital/activity"

# Max timeout for any single network operation (ms)
_MAX_TIMEOUT_MS = 30000


class FidelityScraper(BaseScraper):
    LOGIN_URL = "https://digital.fidelity.com/prgw/digital/login/full-page"
    DASHBOARD_URL = "https://digital.fidelity.com/ftgw/digital/portfolio/summary"

    def __init__(self, headless: bool = True):
        # Fidelity works headless with stealth
        super().__init__(headless=headless)

        include_str = os.environ.get("TILL_FIDELITY_INCLUDE_ACCOUNTS", "")
        self.include_accounts = [s.strip() for s in include_str.split(",") if s.strip()] if include_str else []

    async def navigate_and_login(self, page, username: str, password: str):
        # Try dashboard first -- session cookies may still be valid
        print("   Checking for active session...", file=sys.stderr)
        try:
            await page.goto(
                self.DASHBOARD_URL,
                wait_until="domcontentloaded",
                timeout=_MAX_TIMEOUT_MS,
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

        # Session is valid if we stayed on the portfolio page (not redirected to login)
        if "portfolio" in current_url and "login" not in current_url.lower():
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired -- go to login page
        print("   Session expired, logging in...", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=_MAX_TIMEOUT_MS)
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
                await page.wait_for_url("**/portfolio/**", timeout=_MAX_TIMEOUT_MS)
            except Exception:
                pass

            if "portfolio" in page.url and "login" not in page.url.lower():
                print("   Login successful!", file=sys.stderr)
            else:
                print(f"   Post-login: {page.url}", file=sys.stderr)
                print("   Waiting for 2FA...", file=sys.stderr)
                await page.screenshot(path="/tmp/till_fidelity_2fa.png")
                try:
                    await page.wait_for_url("**/portfolio/**", timeout=_MAX_TIMEOUT_MS)
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
        """Extract accounts, positions, and transactions from Fidelity."""

        # Step 1: Set up API response interception as a safety net
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
            timeout=_MAX_TIMEOUT_MS,
        )
        await page.wait_for_timeout(5000)

        await page.screenshot(path="/tmp/till_fidelity_accounts.png")

        # Step 3: Try direct API calls first (faster, more reliable)
        accounts = []
        positions = []
        direct_api_worked = False

        print("   Trying direct API calls...", file=sys.stderr)
        try:
            portfolio_data = await self._direct_api_get(page, _PORTFOLIO_API)
            if portfolio_data:
                accts = self._parse_accounts_from_api(portfolio_data)
                if accts:
                    accounts.extend(accts)
                    direct_api_worked = True
                    print(f"   Direct API: found {len(accts)} accounts from portfolio-summary", file=sys.stderr)
                pos = self._parse_positions_from_api(portfolio_data)
                if pos:
                    positions.extend(pos)
                    print(f"   Direct API: found {len(pos)} positions from portfolio-summary", file=sys.stderr)
        except Exception as e:
            print(f"   Direct portfolio API failed: {e}", file=sys.stderr)

        # Try positions endpoint for more detail
        try:
            positions_data = await self._direct_api_get(page, _POSITIONS_API)
            if positions_data:
                pos = self._parse_positions_from_api(positions_data)
                if pos:
                    # Merge: prefer direct positions data (may have cost basis, lots)
                    existing_symbols = {p.get("symbol") for p in positions}
                    new_pos = [p for p in pos if p.get("symbol") not in existing_symbols]
                    if new_pos:
                        positions.extend(new_pos)
                        print(f"   Direct API: found {len(new_pos)} additional positions", file=sys.stderr)
                    elif pos and not positions:
                        positions = pos
                        print(f"   Direct API: found {len(pos)} positions from positions endpoint", file=sys.stderr)
        except Exception as e:
            print(f"   Direct positions API failed: {e}", file=sys.stderr)

        # Try balances endpoint for supplemental data
        try:
            balances_data = await self._direct_api_get(page, _BALANCES_API)
            if balances_data:
                bal_accts = self._parse_accounts_from_api(balances_data)
                if bal_accts and not accounts:
                    accounts.extend(bal_accts)
                    direct_api_worked = True
                    print(f"   Direct API: found {len(bal_accts)} accounts from balances", file=sys.stderr)
        except Exception as e:
            print(f"   Direct balances API failed: {e}", file=sys.stderr)

        # Step 4: Fall back to intercepted API responses if direct calls didn't work
        if not accounts or all(a['balance'] == 0 for a in accounts):
            print("   Direct API didn't return accounts, checking intercepted responses...", file=sys.stderr)
            for url, data in api_responses.items():
                if any(k in url.lower() for k in ['account', 'portfolio', 'position', 'summary']):
                    accts = self._parse_accounts_from_api(data)
                    if accts:
                        accounts.extend(accts)
                    pos = self._parse_positions_from_api(data)
                    if pos:
                        positions.extend(pos)

        # Step 5: Final fallback -- DOM extraction
        if not accounts or all(a['balance'] == 0 for a in accounts):
            print("   API didn't return account balances, falling back to DOM", file=sys.stderr)
            dom_accounts = await self._extract_accounts_dom(page)
            if dom_accounts:
                accounts = dom_accounts

        # Step 6: Fetch transactions
        transactions = await self._fetch_transactions(page, api_responses, accounts)

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
        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

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
                "cost_basis": pos.get("cost_basis"),
                "total_gain": pos.get("total_gain"),
                "total_gain_percent": pos.get("total_gain_percent"),
                "day_change": pos.get("day_change"),
                "day_change_percent": pos.get("day_change_percent"),
                "lots": pos.get("lots"),
                "account_id": pos.get("account_id", ""),
            })

        return {
            "status": "ok",
            "source": "fidelity",
            "accounts": account_results,
            "transactions": transactions,
            "positions": position_results,
            "balance_history": [],
        }

    async def _direct_api_get(self, page, url: str) -> dict | list | None:
        """Make a direct API GET request using the page's session cookies.

        Returns parsed JSON or None on failure.
        """
        try:
            response = await page.request.get(
                url,
                headers={
                    "Accept": "application/json",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=_MAX_TIMEOUT_MS,
            )
            if response.status == 200:
                ct = response.headers.get("content-type", "")
                if "json" in ct:
                    body = await response.text()
                    if body and body.strip() and body.strip()[0] in '{[':
                        data = json.loads(body)
                        print(f"   Direct API [200]: {url[:80]}", file=sys.stderr)
                        return data
            else:
                print(f"   Direct API [{response.status}]: {url[:80]}", file=sys.stderr)
        except Exception as e:
            print(f"   Direct API error for {url[:60]}: {e}", file=sys.stderr)
        return None

    async def _fetch_transactions(self, page, api_responses: dict, accounts: list) -> list:
        """Fetch transactions by navigating to activity page and intercepting API responses."""
        transactions = []

        # First check if any intercepted responses already have transactions
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['activity', 'transaction', 'history', 'order']):
                txns = self._parse_transactions_from_api(data, accounts)
                if txns:
                    transactions.extend(txns)

        if transactions:
            print(f"   Found {len(transactions)} transactions from intercepted APIs", file=sys.stderr)
            return transactions

        # Navigate to activity page to trigger transaction API calls
        print("   Loading activity page for transactions...", file=sys.stderr)
        activity_responses = {}

        async def capture_activity(response):
            url = response.url
            if any(k in url.lower() for k in ['activity', 'transaction', 'history', 'order']):
                if response.status == 200:
                    try:
                        ct = response.headers.get('content-type', '')
                        if 'json' not in ct:
                            return
                        body = await response.text()
                        if body and body.strip() and body.strip()[0] in '{[':
                            activity_responses[url] = json.loads(body)
                            print(f"   Activity API [{response.status}]: {url[:100]}", file=sys.stderr)
                    except Exception:
                        pass

        page.on("response", capture_activity)

        try:
            await page.goto(
                _ACTIVITY_URL,
                wait_until="domcontentloaded",
                timeout=_MAX_TIMEOUT_MS,
            )
            await page.wait_for_timeout(5000)
        except Exception as e:
            print(f"   Activity page navigation failed: {e}", file=sys.stderr)
            return transactions

        # Try direct API call to activity endpoint
        try:
            activity_data = await self._direct_api_get(page, _ACTIVITY_API)
            if activity_data:
                txns = self._parse_transactions_from_api(activity_data, accounts)
                if txns:
                    transactions.extend(txns)
                    print(f"   Direct API: found {len(txns)} transactions", file=sys.stderr)
        except Exception as e:
            print(f"   Direct activity API failed: {e}", file=sys.stderr)

        # Parse any intercepted activity responses
        if not transactions:
            for url, data in activity_responses.items():
                txns = self._parse_transactions_from_api(data, accounts)
                if txns:
                    transactions.extend(txns)

        # Remove the activity-specific listener (the general one is still active)
        page.remove_listener("response", capture_activity)

        return transactions

    def _parse_transactions_from_api(self, data, accounts: list) -> list:
        """Parse transactions from Fidelity's activity API response."""
        transactions = []

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in [
                'activities', 'Activities', 'transactions', 'Transactions',
                'orders', 'Orders', 'history', 'History',
                'activityList', 'transactionList', 'items', 'Items',
            ]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            # Check nested
            if not items:
                for val in data.values():
                    if isinstance(val, dict):
                        for key in ['activities', 'transactions', 'orders', 'items']:
                            if key in val and isinstance(val[key], list):
                                items = val[key]
                                break
                    if items:
                        break

        # Build account lookup by suffix for matching
        suffix_to_id = {}
        for acct in accounts:
            suffix = acct.get("account_suffix", "")
            if suffix:
                id_key = f"fidelity_{suffix}"
                suffix_to_id[suffix] = hashlib.md5(id_key.encode()).hexdigest()[:16]

        for item in items:
            if not isinstance(item, dict):
                continue

            txn_date = (
                item.get('date') or item.get('Date') or
                item.get('transactionDate') or item.get('TransactionDate') or
                item.get('activityDate') or item.get('settlementDate') or ''
            )
            txn_type = (
                item.get('type') or item.get('Type') or
                item.get('transactionType') or item.get('TransactionType') or
                item.get('activityType') or item.get('action') or ''
            )
            description = (
                item.get('description') or item.get('Description') or
                item.get('activityDescription') or item.get('memo') or ''
            )
            symbol = (
                item.get('symbol') or item.get('Symbol') or
                item.get('ticker') or ''
            )
            amount = (
                item.get('amount') or item.get('Amount') or
                item.get('netAmount') or item.get('totalAmount') or 0
            )
            quantity = (
                item.get('quantity') or item.get('Quantity') or
                item.get('shares') or 0
            )
            price = (
                item.get('price') or item.get('Price') or
                item.get('executionPrice') or 0
            )

            # Parse amount from string if needed
            for field_name in ['amount', 'quantity', 'price']:
                val = locals()[field_name]
                if isinstance(val, str):
                    try:
                        locals()[field_name] = float(val.replace(',', '').replace('$', '').replace('(', '-').replace(')', ''))
                    except ValueError:
                        locals()[field_name] = 0

            # Try to match account
            acct_num = (
                item.get('accountNumber') or item.get('AccountNumber') or
                item.get('accountId') or ''
            )
            acct_suffix = str(acct_num)[-4:] if acct_num else ""
            account_id = suffix_to_id.get(acct_suffix, "")

            txn_id = (
                item.get('transactionId') or item.get('activityId') or
                item.get('orderId') or item.get('id') or
                hashlib.md5(f"{txn_date}_{symbol}_{amount}".encode()).hexdigest()[:16]
            )

            if txn_date or description or symbol:
                transactions.append({
                    "transaction_id": str(txn_id),
                    "account_id": account_id,
                    "date": txn_date,
                    "type": txn_type,
                    "description": description,
                    "symbol": symbol,
                    "quantity": float(quantity) if quantity else 0,
                    "price": float(price) if price else 0,
                    "amount": float(amount) if amount else 0,
                })

        return transactions

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
            quantity = self._parse_number(
                item.get('quantity') or item.get('Quantity') or
                item.get('shares') or item.get('Shares') or 0
            )
            price = self._parse_number(
                item.get('lastPrice') or item.get('LastPrice') or
                item.get('price') or item.get('Price') or
                item.get('currentPrice') or 0
            )
            value = self._parse_number(
                item.get('marketValue') or item.get('MarketValue') or
                item.get('value') or item.get('Value') or
                item.get('currentValue') or 0
            )
            cost_basis = self._parse_number(
                item.get('costBasis') or item.get('CostBasis') or
                item.get('totalCost') or item.get('TotalCost') or
                item.get('costBasisTotal') or 0
            )
            total_gain = self._parse_number(
                item.get('totalGain') or item.get('TotalGain') or
                item.get('unrealizedGain') or item.get('gainLoss') or 0
            )
            total_gain_pct = self._parse_number(
                item.get('totalGainPercent') or item.get('TotalGainPercent') or
                item.get('unrealizedGainPercent') or item.get('gainLossPercent') or 0
            )
            day_change = item.get('dayChange') or item.get('DayChange')
            day_change_pct = item.get('dayChangePercent') or item.get('DayChangePercent')

            # If we have cost basis but no total gain, compute it
            if cost_basis and value and not total_gain:
                total_gain = value - cost_basis
                if cost_basis != 0:
                    total_gain_pct = (total_gain / cost_basis) * 100

            # Parse lot information if available
            lots = None
            raw_lots = item.get('lots') or item.get('Lots') or item.get('taxLots') or item.get('TaxLots')
            if raw_lots and isinstance(raw_lots, list):
                lots = []
                for lot in raw_lots:
                    if isinstance(lot, dict):
                        lots.append({
                            "acquired_date": lot.get('acquiredDate') or lot.get('purchaseDate') or lot.get('openDate'),
                            "quantity": self._parse_number(lot.get('quantity') or lot.get('shares') or 0),
                            "cost_basis": self._parse_number(lot.get('costBasis') or lot.get('cost') or 0),
                            "market_value": self._parse_number(lot.get('marketValue') or lot.get('value') or 0),
                            "gain_loss": self._parse_number(lot.get('gainLoss') or lot.get('unrealizedGain') or 0),
                            "term": lot.get('term') or lot.get('holdingPeriod') or '',
                        })

            if symbol or name:
                positions.append({
                    "symbol": symbol,
                    "name": name,
                    "quantity": quantity,
                    "price": price,
                    "value": value,
                    "cost_basis": cost_basis if cost_basis else None,
                    "total_gain": total_gain if total_gain else None,
                    "total_gain_percent": total_gain_pct if total_gain_pct else None,
                    "day_change": day_change,
                    "day_change_percent": day_change_pct,
                    "lots": lots,
                    "account_id": item.get('accountId') or item.get('accountNumber') or "",
                })

        return positions

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Fallback: extract accounts from DOM on Fidelity portfolio summary."""
        accounts = []

        # Fidelity groups accounts in sections -- look for account rows
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
    def _parse_number(val) -> float:
        """Parse a number from string or numeric value."""
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            try:
                return float(val.replace(',', '').replace('$', '').replace('(', '-').replace(')', ''))
            except ValueError:
                return 0.0
        return 0.0

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
