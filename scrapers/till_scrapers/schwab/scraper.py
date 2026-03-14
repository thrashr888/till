"""Charles Schwab scraper — migrated from Argos."""

import re
import sys
import hashlib
import os
from till_scrapers.base import BaseScraper


class SchwabScraper(BaseScraper):
    LOGIN_URL = "https://client.schwab.com/app/accounts/summary/"

    def __init__(self, headless: bool = True):
        # Schwab always requires headful
        if headless:
            print("   Enforcing headful mode for Schwab (bot detection).", file=sys.stderr)
        super().__init__(headless=False)

        self.transaction_account = os.environ.get("TILL_SCHWAB_TRANSACTION_ACCOUNT", "")
        account_types = os.environ.get("TILL_SCHWAB_ACCOUNT_TYPES", "Bank,Investment")
        self.allowed_account_types = [t.strip() for t in account_types.split(",")]

    async def navigate_and_login(self, page, username: str, password: str):
        print(f"   Navigating to {self.LOGIN_URL}", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=300000)

        current_url = page.url
        if "login" in current_url.lower() or "auth" in current_url.lower():
            if username and password:
                print("   Auto-filling login credentials...", file=sys.stderr)
                try:
                    await page.wait_for_timeout(2000)
                    login_frame = page.frame(name="lmsSecondaryLogin") or page
                    await login_frame.fill("#loginIdInput", username)
                    await login_frame.fill("#passwordInput", password)
                    await login_frame.click("#btnLogin")
                    print("   Waiting for login...", file=sys.stderr)
                    await page.wait_for_timeout(5000)
                except Exception as e:
                    print(f"   Auto-login failed: {e}", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_schwab_login.png")
                    raise
            else:
                print("   Waiting for manual login...", file=sys.stderr)
                await page.wait_for_timeout(30000)

        await page.wait_for_timeout(3000)

    async def extract(self, page) -> dict:
        accounts = []
        transactions = []

        await page.screenshot(path="/tmp/till_schwab_accounts.png")
        print("   Screenshot: /tmp/till_schwab_accounts.png", file=sys.stderr)

        # Extract accounts
        accounts = await self._extract_accounts(page)
        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Extract transactions if configured
        if self.transaction_account:
            transactions = await self._extract_transactions(page)
            print(f"   Found {len(transactions)} transactions", file=sys.stderr)

        # Build account results
        account_results = []
        for acct in accounts:
            account_id = hashlib.md5(acct["name"].encode()).hexdigest()[:16]
            account_results.append({
                "account_id": account_id,
                "account_name": acct["name"],
                "account_type": acct.get("type", "other"),
                "balance": acct["balance"],
                "day_change": acct.get("day_change"),
                "day_change_percent": acct.get("day_change_percent"),
            })

        # Build transaction results
        txn_results = []
        for txn in transactions:
            txn_results.append({
                "txn_id": txn.get("id", hashlib.md5(
                    f"{txn['date']}:{txn['description']}:{txn['amount']}".encode()
                ).hexdigest()[:16]),
                "account_id": txn.get("account_id", ""),
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

    async def _extract_accounts(self, page) -> list[dict]:
        """Extract Schwab accounts from the summary page."""
        accounts = []

        # Try Schwab-specific selectors
        selectors = [
            'div[class*="account-group"]',
            'div[class*="AccountCard"]',
            'section[class*="account"]',
            '[data-testid*="account"]',
        ]

        account_cards = []
        for selector in selectors:
            account_cards = await page.query_selector_all(selector)
            if account_cards:
                print(f"   Found {len(account_cards)} cards with: {selector}", file=sys.stderr)
                break

        for card in account_cards:
            try:
                text = await card.inner_text()
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                if len(lines) < 2:
                    continue

                name = lines[0]
                balance = None
                day_change = None
                day_change_percent = None
                account_type = "other"

                # Determine type from name
                name_lower = name.lower()
                if any(t.lower() in name_lower for t in self.allowed_account_types):
                    if "bank" in name_lower or "check" in name_lower or "saving" in name_lower:
                        account_type = "checking"
                    else:
                        account_type = "brokerage"

                # Extract dollar values
                for line in lines:
                    if "$" in line and balance is None:
                        match = re.search(r'\$?([\d,]+\.?\d*)', line)
                        if match:
                            balance = float(match.group(1).replace(',', ''))
                    elif "%" in line and day_change_percent is None:
                        match = re.search(r'([+-]?\d+\.?\d*)%', line)
                        if match:
                            day_change_percent = float(match.group(1))

                if balance and balance > 0:
                    accounts.append({
                        "name": name,
                        "balance": balance,
                        "type": account_type,
                        "day_change": day_change,
                        "day_change_percent": day_change_percent,
                    })

            except Exception as e:
                print(f"   Error parsing account: {e}", file=sys.stderr)

        # Fallback: extract from page text
        if not accounts:
            print("   Fallback: extracting from page text", file=sys.stderr)
            content = await page.content()
            dollar_amounts = re.findall(r'\$[\d,]+\.?\d*', content)
            if dollar_amounts:
                amounts = [float(d.replace('$', '').replace(',', '')) for d in dollar_amounts]
                if amounts:
                    accounts.append({
                        "name": "Schwab Portfolio",
                        "balance": max(amounts),
                        "type": "brokerage",
                    })

        return accounts

    async def _extract_transactions(self, page) -> list[dict]:
        """Extract recent transactions."""
        transactions = []

        try:
            history_url = f"https://client.schwab.com/app/accounts/transactionhistory/#/history/{self.transaction_account}"
            await page.goto(history_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(5000)

            await page.screenshot(path="/tmp/till_schwab_transactions.png")

            rows = await page.query_selector_all('tr[class*="transaction"], tr[data-testid*="transaction"]')
            for row in rows:
                try:
                    text = await row.inner_text()
                    lines = [l.strip() for l in text.split("\t") if l.strip()]
                    if len(lines) >= 3:
                        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', lines[0])
                        amount_match = re.search(r'\$?([\d,]+\.?\d*)', lines[-1])
                        if date_match and amount_match:
                            transactions.append({
                                "date": date_match.group(1),
                                "description": lines[1] if len(lines) > 1 else "Unknown",
                                "amount": float(amount_match.group(1).replace(',', '')),
                                "account_id": hashlib.md5(
                                    self.transaction_account.encode()
                                ).hexdigest()[:16],
                            })
                except Exception:
                    continue

        except Exception as e:
            print(f"   Error extracting transactions: {e}", file=sys.stderr)

        return transactions
