"""Chase scraper — migrated from Argos."""

import re
import sys
import hashlib
import os
from till_scrapers.base import BaseScraper


class ChaseScraper(BaseScraper):
    LOGIN_URL = "https://secure.chase.com/web/auth/dashboard"

    def __init__(self, headless: bool = True):
        # Chase always requires headful
        if headless:
            print("   Enforcing headful mode for Chase (bot detection).", file=sys.stderr)
        super().__init__(headless=False)

        # Config from env
        self.chase_accounts = []
        accounts_str = os.environ.get("TILL_CHASE_ACCOUNTS", "")
        if accounts_str:
            for acct in accounts_str.split(";"):
                parts = acct.split(",")
                if len(parts) == 2:
                    self.chase_accounts.append({"name": parts[0], "last4": parts[1]})

    async def navigate_and_login(self, page, username: str, password: str):
        print(f"   Navigating to {self.LOGIN_URL}", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=300000)

        current_url = page.url
        if "login" in current_url.lower() or "logon" in current_url.lower():
            if username and password:
                print("   Auto-filling login credentials...", file=sys.stderr)
                try:
                    await page.wait_for_timeout(2000)
                    await page.fill("#userId-text-input-field", username)
                    await page.fill("#password-text-input-field", password)
                    await page.click("#signin-button")
                    print("   Waiting for login...", file=sys.stderr)
                    await page.wait_for_timeout(5000)
                except Exception as e:
                    print(f"   Auto-login failed: {e}", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_chase_login.png")
                    raise
            else:
                raise Exception("No credentials found. Set via `till creds set --source chase`")

        await page.wait_for_timeout(3000)

    async def extract(self, page) -> dict:
        accounts = []
        transactions = []

        await page.screenshot(path="/tmp/till_chase_accounts.png")
        print("   Screenshot: /tmp/till_chase_accounts.png", file=sys.stderr)

        # Extract credit card accounts
        accounts = await self._extract_credit_accounts(page)
        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Extract transactions for each account
        for acct in accounts:
            acct_transactions = await self._extract_transactions(page, acct)
            transactions.extend(acct_transactions)
        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

        # Build results
        account_results = []
        for acct in accounts:
            account_results.append({
                "account_id": acct["id"],
                "account_name": acct["name"],
                "account_type": "credit",
                "balance": acct["balance"],
                "available_credit": acct.get("available_credit"),
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
                "is_recurring": txn.get("is_recurring", False),
                "recurring_group": txn.get("recurring_group"),
            })

        return {
            "status": "ok",
            "source": "chase",
            "accounts": account_results,
            "transactions": txn_results,
            "positions": [],
            "balance_history": [],
        }

    async def _extract_credit_accounts(self, page) -> list[dict]:
        """Extract Chase credit card accounts from dashboard."""
        accounts = []

        selectors = [
            'div[class*="credit-card"]',
            'div[class*="account-tile"]',
            'section[class*="credit"]',
            'mds-tile',
        ]

        cards = []
        for selector in selectors:
            cards = await page.query_selector_all(selector)
            if cards:
                print(f"   Found {len(cards)} cards with: {selector}", file=sys.stderr)
                break

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
                            if balance is None:
                                balance = amount
                            elif available_credit is None:
                                available_credit = amount

                if balance is not None:
                    account_id = hashlib.md5(name.encode()).hexdigest()[:16]
                    accounts.append({
                        "id": account_id,
                        "name": name,
                        "balance": balance,
                        "available_credit": available_credit,
                    })

            except Exception as e:
                print(f"   Error parsing account: {e}", file=sys.stderr)

        # Fallback
        if not accounts:
            print("   Fallback: extracting from page text", file=sys.stderr)
            content = await page.content()
            amounts = re.findall(r'\$[\d,]+\.?\d*', content)
            if amounts:
                vals = [float(d.replace('$', '').replace(',', '')) for d in amounts]
                if vals:
                    accounts.append({
                        "id": hashlib.md5(b"chase_default").hexdigest()[:16],
                        "name": "Chase Credit Card",
                        "balance": max(vals),
                    })

        return accounts

    async def _extract_transactions(self, page, account: dict) -> list[dict]:
        """Extract transactions for an account."""
        transactions = []

        try:
            # Try to navigate to account activity
            acct_links = await page.query_selector_all('a[href*="activity"]')
            for link in acct_links:
                link_text = await link.inner_text()
                if account["name"].split()[0].lower() in link_text.lower():
                    await link.click()
                    await page.wait_for_timeout(3000)
                    break

            await page.screenshot(path=f"/tmp/till_chase_{account['id']}_txns.png")

            # Extract transaction rows
            rows = await page.query_selector_all(
                'div[class*="transaction"], tr[class*="transaction"], [data-testid*="transaction"]'
            )

            for row in rows:
                try:
                    text = await row.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    if len(lines) < 2:
                        continue

                    date_match = re.search(r'(\w+ \d+)', lines[0])
                    if not date_match:
                        continue

                    description = lines[1] if len(lines) > 1 else "Unknown"
                    amount = None
                    for line in reversed(lines):
                        amount = self._parse_dollar(line)
                        if amount is not None:
                            break

                    if amount is not None:
                        txn_id = hashlib.md5(
                            f"{date_match.group(1)}:{description}:{amount}".encode()
                        ).hexdigest()[:16]
                        transactions.append({
                            "id": txn_id,
                            "account_id": account["id"],
                            "date": date_match.group(1),
                            "description": description,
                            "amount": -amount,  # Credits are negative for credit cards
                        })

                except Exception:
                    continue

        except Exception as e:
            print(f"   Error extracting transactions: {e}", file=sys.stderr)

        return transactions

    @staticmethod
    def _parse_dollar(text: str):
        match = re.search(r'([+-])?\$?([\d,]+\.?\d*)', text)
        if match:
            sign = match.group(1) or ''
            amount = float(match.group(2).replace(',', ''))
            return -amount if sign == '-' else amount
        return None
