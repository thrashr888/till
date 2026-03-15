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
                await page.wait_for_timeout(1000)

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
                    # No iframe — try directly on page
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
                    # Fill username — use type() with delay to simulate human input
                    print("   Entering username...", file=sys.stderr)
                    try:
                        username_field = frame.get_by_role("textbox", name="Login ID")
                        await username_field.click()
                        await username_field.fill("")
                        await username_field.type(username, delay=50)
                    except Exception:
                        login_input = frame.locator('#loginIdInput')
                        await login_input.click()
                        await login_input.fill("")
                        await login_input.type(username, delay=50)
                    await page.wait_for_timeout(500)

                    # Fill password
                    print("   Entering password...", file=sys.stderr)
                    try:
                        password_field = frame.get_by_role("textbox", name="Password")
                        await password_field.click()
                        await password_field.type(password, delay=30)
                    except Exception:
                        pw_input = frame.locator('#passwordInput')
                        await pw_input.click()
                        await pw_input.type(password, delay=30)
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
            # Use account suffix for stable ID when available
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

        Real accounts have a sr-only span with "Account number ending in XXX".
        Group headers and total rows do not. We use this to filter.

        Columns: Account Name | Type | Cash & Cash Investments | Account Value | Day Change $ | Day Change %
        """
        accounts = []

        # Target the Accounts section wrapper
        accounts_wrapper = await page.query_selector('div.allAccountsWrapper')
        if not accounts_wrapper:
            accounts_wrapper = await page.query_selector(
                '[class*="allAccounts"], [class*="AccountsWrapper"]'
            )

        search_context = accounts_wrapper or page

        # Get all table rows
        account_rows = await search_context.query_selector_all(
            'sdps-table-row, table tbody tr, [class*="AccountRow"]'
        )

        for row in account_rows:
            try:
                # Skip group headers (they have account-list-table-group-header attribute)
                is_header = await row.get_attribute('account-list-table-group-header')
                if is_header is not None:
                    continue

                # Only include rows with "Account number ending in" — these are real accounts
                account_num_el = await row.query_selector('span.sr-only')
                account_suffix = None
                if account_num_el:
                    sr_text = await account_num_el.inner_text()
                    match = re.search(r'Account number ending in\s+(\S+)', sr_text)
                    if match:
                        account_suffix = match.group(1)

                if account_suffix is None:
                    # No account number = total/header row, skip
                    continue

                # Get account name from the first cell
                cells = await row.query_selector_all('sdps-table-cell, td')
                if len(cells) < 4:
                    continue

                cell_texts = []
                for cell in cells:
                    text = await cell.inner_text()
                    cell_texts.append(text.strip())

                # First cell contains: "Account Name\nCompanyName\n..."
                raw_name = cell_texts[0]
                # Extract just the account name (first line, before company name)
                name_lines = [l.strip() for l in raw_name.split('\n') if l.strip()]
                name = name_lines[0] if name_lines else raw_name

                # Clean up name
                name = name.rstrip('\u2020').strip()
                if not name or 'Total' in name:
                    continue

                # Extract account type
                # Check Type column (cell index 1) — can be multi-line:
                # "Schwab Bank\nType\nChecking" or just "Brokerage"
                account_type = "other"
                type_text = cell_texts[1] if len(cell_texts) > 1 else ""
                # Also check the full row text for type keywords
                full_text = " ".join(cell_texts).lower()
                type_lines = [l.strip().lower() for l in type_text.split('\n') if l.strip()]
                for tl in type_lines:
                    if tl in ['checking']:
                        account_type = "checking"
                        break
                    elif tl in ['savings']:
                        account_type = "savings"
                        break
                    elif tl in ['brokerage']:
                        account_type = "brokerage"
                        break
                    elif tl in ['ira', 'roth', 'roth contributory ira', 'traditional ira']:
                        account_type = "ira"
                        break

                # Infer from name if type column didn't help
                if account_type == "other":
                    name_lower = name.lower()
                    if 'checking' in name_lower:
                        account_type = "checking"
                    elif 'saving' in name_lower:
                        account_type = "savings"
                    elif '401' in name_lower or '401(k)' in name_lower:
                        account_type = "401k"
                    elif 'ira' in name_lower or 'roth' in name_lower or 'retirement' in name_lower:
                        account_type = "ira"
                    elif '529' in name_lower:
                        account_type = "brokerage"
                    elif 'etrade' in name_lower or 'individual' in name_lower:
                        account_type = "brokerage"

                # Parse dollar values from remaining cells
                account_value = None
                day_change = None
                day_change_percent = None

                for i, cell_text in enumerate(cell_texts[1:], 1):
                    dollar_match = re.search(r'([+-])?\$?([\d,]+\.?\d*)', cell_text)
                    if dollar_match:
                        sign = dollar_match.group(1) or ''
                        amount = float(dollar_match.group(2).replace(',', ''))
                        if sign == '-':
                            amount = -amount
                        # Columns: Type(1), Cash(2), Value(3), Day$(4)
                        if i == 3:
                            account_value = amount
                        elif i == 4:
                            day_change = amount

                    pct_match = re.search(r'([+-]?\d+\.?\d*)%', cell_text)
                    if pct_match:
                        day_change_percent = float(pct_match.group(1))

                if account_value is None:
                    account_value = 0.0

                print(f"   {name} ...{account_suffix}: ${account_value:,.2f} ({account_type})", file=sys.stderr)
                accounts.append({
                    "name": name,
                    "balance": account_value,
                    "type": account_type,
                    "account_suffix": account_suffix,
                    "day_change": day_change,
                    "day_change_percent": day_change_percent,
                })

            except Exception as e:
                print(f"   Error parsing row: {e}", file=sys.stderr)
                continue

        if not accounts:
            print("   No accounts found via structured extraction", file=sys.stderr)

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
