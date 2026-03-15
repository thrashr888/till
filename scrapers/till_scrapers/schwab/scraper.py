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
        # Navigate directly to the client login page (has the iframe login form)
        login_url = "https://client.schwab.com/Login/SignOn/CustomerCenterLogin.aspx"
        print(f"   Navigating to {login_url}", file=sys.stderr)
        await page.goto(login_url, wait_until="domcontentloaded", timeout=300000)

        # Check if we're already logged in (redirected to summary)
        if "client.schwab.com/app/accounts" in page.url:
            print("   Already logged in (session active)", file=sys.stderr)
            return

        if username and password:
            print("   Auto-filling login credentials...", file=sys.stderr)
            try:
                await page.wait_for_timeout(3000)

                # Login form is inside an iframe with title "log in form" or id "lmsIframe"
                frame = None
                for selector in [
                    'iframe[title="log in form"]',
                    'iframe#lmsIframe',
                    'iframe[aria-label="Login widget"]',
                ]:
                    try:
                        frame = page.frame_locator(selector)
                        # Test if frame is accessible by looking for any input
                        await frame.locator('input').first.wait_for(timeout=3000)
                        print(f"   Found login iframe: {selector}", file=sys.stderr)
                        break
                    except Exception:
                        frame = None
                        continue

                if frame is None:
                    # No iframe — try directly on page (some Schwab pages have inline form)
                    print("   No iframe found, trying direct page login", file=sys.stderr)
                    await page.fill('#loginIdInput', username)
                    await page.fill('#passwordInput', password)
                    await page.click('#btnLogin')
                else:
                    # Fill username
                    print("   Entering username...", file=sys.stderr)
                    try:
                        username_field = frame.get_by_role("textbox", name="Login ID")
                        await username_field.fill(username)
                    except Exception:
                        await frame.locator('#loginIdInput').fill(username)
                    await page.wait_for_timeout(500)

                    # Fill password
                    print("   Entering password...", file=sys.stderr)
                    try:
                        password_field = frame.get_by_role("textbox", name="Password")
                        await password_field.fill(password)
                    except Exception:
                        await frame.locator('#passwordInput').fill(password)
                    await page.wait_for_timeout(500)

                    # Click login
                    print("   Clicking login...", file=sys.stderr)
                    try:
                        login_button = frame.get_by_role("button", name="Log in")
                        await login_button.click()
                    except Exception:
                        await frame.locator('#btnLogin').click()

                # Wait for redirect — could go to summary, 2FA, or security challenge
                print("   Waiting for login to complete...", file=sys.stderr)
                try:
                    await page.wait_for_url(
                        "**/client.schwab.com/**",
                        timeout=120000,
                    )
                except Exception:
                    pass  # May need 2FA — continue and check URL below

                # Check if we landed on 2FA or security challenge
                current_url = page.url
                if "client.schwab.com/app/accounts" in current_url:
                    print("   Login successful!", file=sys.stderr)
                else:
                    print(f"   Post-login page: {current_url}", file=sys.stderr)
                    print("   Waiting 60s for 2FA/security challenge...", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_schwab_2fa.png")
                    try:
                        await page.wait_for_url(
                            "**/app/accounts/**",
                            timeout=120000,
                        )
                        print("   Login successful after 2FA!", file=sys.stderr)
                    except Exception:
                        await page.screenshot(path="/tmp/till_schwab_login.png")
                        raise Exception(
                            f"Login did not reach accounts page. Current URL: {page.url}. "
                            "Check /tmp/till_schwab_login.png"
                        )

            except Exception as e:
                print(f"   Auto-login failed: {e}", file=sys.stderr)
                await page.screenshot(path="/tmp/till_schwab_login.png")
                raise
        else:
            raise Exception(
                "No credentials found. Use `till creds set --source schwab` "
                "or set TILL_SCHWAB_USERNAME/TILL_SCHWAB_PASSWORD env vars."
            )

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
        """Extract Schwab accounts from the summary page.

        Schwab summary page has a table inside div.allAccountsWrapper with columns:
        Account Name | Type | Cash & Cash Investments | Account Value | Day Change $ | Day Change %
        """
        accounts = []

        # Target the Accounts section wrapper
        accounts_wrapper = await page.query_selector('div.allAccountsWrapper')
        if not accounts_wrapper:
            accounts_wrapper = await page.query_selector(
                '[class*="allAccounts"], [class*="AccountsWrapper"]'
            )

        if accounts_wrapper:
            print("   Found accounts wrapper", file=sys.stderr)
            account_rows = await accounts_wrapper.query_selector_all(
                '[data-testid="account-row"], .account-row, [class*="AccountRow"], table tbody tr'
            )
            if not account_rows:
                account_rows = await accounts_wrapper.query_selector_all(
                    'tr, [class*="row"], [class*="Row"]'
                )
        else:
            print("   No accounts wrapper found, using page-level search", file=sys.stderr)
            account_rows = await page.query_selector_all(
                '[data-testid="account-row"], .account-row, table tbody tr'
            )

        for row in account_rows:
            try:
                cells = await row.query_selector_all('td')

                if cells and len(cells) >= 4:
                    cell_texts = []
                    for cell in cells:
                        text = await cell.inner_text()
                        cell_texts.append(text.strip())

                    name = cell_texts[0] if cell_texts else ""

                    # Skip headers, totals, and securities
                    if not name or name.upper() in ['ACCOUNT NAME', 'NAME', 'CLICK']:
                        continue
                    if not re.search(r'[A-Za-z]', name):
                        continue
                    if name == "Investing Total" or '††' in name:
                        continue
                    if 'Held in' in name and 'Account' in name:
                        continue
                    # Skip stock tickers (all caps, 1-5 chars)
                    first_word = name.split()[0] if name.split() else name
                    if re.match(r'^[A-Z]{1,5}$', first_word):
                        continue

                    account_type = None
                    account_value = None
                    day_change = None
                    day_change_percent = None

                    for i, cell_text in enumerate(cell_texts[1:], 1):
                        # Column 1: Type (text like "Brokerage", "IRA")
                        if i == 1 and not cell_text.startswith('$') and '%' not in cell_text:
                            if cell_text and cell_text not in ['-', '\u2013']:
                                account_type = cell_text

                        # Parse dollar amounts by column position
                        dollar_match = re.search(r'([+-])?\$?([\d,]+\.?\d*)', cell_text)
                        if dollar_match:
                            sign = dollar_match.group(1) or ''
                            amount = float(dollar_match.group(2).replace(',', ''))
                            if sign == '-':
                                amount = -amount
                            # Columns: Name(0), Type(1), Cash(2), Value(3), Day$(4)
                            if i == 3:
                                account_value = amount
                            elif i == 4:
                                day_change = amount

                        pct_match = re.search(r'([+-]?\d+\.?\d*)%', cell_text)
                        if pct_match:
                            day_change_percent = float(pct_match.group(1))

                    if not account_type:
                        account_type = self._infer_account_type(name)

                    if account_value and account_value > 0:
                        clean_name = name.rstrip('\u2020').strip()
                        print(f"   {clean_name}: ${account_value:,.2f}", file=sys.stderr)
                        accounts.append({
                            "name": clean_name,
                            "balance": account_value,
                            "type": account_type or "other",
                            "day_change": day_change,
                            "day_change_percent": day_change_percent,
                        })
                    continue

                # Fallback: parse from inner text
                text = await row.inner_text()
                lines = [line.strip() for line in text.split("\n") if line.strip()]
                if len(lines) < 1:
                    continue

                name = lines[0]
                if not re.search(r'[A-Za-z]', name) or len(name) <= 2:
                    continue

                all_text = " ".join(lines)
                dollar_amounts = []
                for match in re.finditer(r'([+-])?\$([\d,]+\.?\d*)', all_text):
                    sign = match.group(1) or ''
                    amount = float(match.group(2).replace(',', ''))
                    if sign == '-':
                        amount = -amount
                    dollar_amounts.append(amount)

                # Columns: Cash(0), Value(1), Day$(2)
                account_value = dollar_amounts[1] if len(dollar_amounts) > 1 else (
                    dollar_amounts[0] if dollar_amounts else None
                )
                day_change = dollar_amounts[2] if len(dollar_amounts) > 2 else None

                pct_match = re.search(r'([+-]?\d+\.?\d*)%', all_text)
                day_change_percent = float(pct_match.group(1)) if pct_match else None

                if account_value and account_value > 100:
                    accounts.append({
                        "name": name.rstrip('\u2020').strip(),
                        "balance": account_value,
                        "type": self._infer_account_type(name),
                        "day_change": day_change,
                        "day_change_percent": day_change_percent,
                    })

            except Exception as e:
                print(f"   Error parsing row: {e}", file=sys.stderr)
                continue

        # Last resort fallback
        if not accounts:
            print("   Fallback: extracting from page text", file=sys.stderr)
            content = await page.content()
            dollar_amounts = re.findall(r'\$([\d,]+\.?\d*)', content)
            valid = [amt for amt in dollar_amounts if amt.strip()]
            if valid:
                try:
                    balance = float(valid[0].replace(',', ''))
                    accounts.append({
                        "name": "Total Portfolio",
                        "balance": balance,
                        "type": "brokerage",
                    })
                except ValueError:
                    pass

        # Filter by allowed account types
        if self.allowed_account_types:
            filtered = [a for a in accounts if a.get("type") in self.allowed_account_types]
            if len(filtered) < len(accounts):
                skipped = len(accounts) - len(filtered)
                print(f"   Filtered out {skipped} entries not matching types: {self.allowed_account_types}", file=sys.stderr)
            return filtered

        return accounts

    def _infer_account_type(self, name: str) -> str:
        """Infer account type from account name."""
        name_lower = name.lower()
        if any(w in name_lower for w in ['check', 'saving', 'bank']):
            return "Bank"
        if any(w in name_lower for w in ['ira', 'roth', 'traditional', 'rollover']):
            return "Investment"
        if any(w in name_lower for w in ['brokerage', 'individual', 'joint']):
            return "Investment"
        if any(w in name_lower for w in ['401k', '401(k)', 'hsa']):
            return "Investment"
        return "Investment"

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
