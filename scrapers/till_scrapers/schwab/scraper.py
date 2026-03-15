"""Charles Schwab scraper — migrated from Argos."""

import re
import sys
import hashlib
import os
from till_scrapers.base import BaseScraper


class SchwabScraper(BaseScraper):
    LOGIN_URL = "https://client.schwab.com/app/accounts/summary/"

    def __init__(self, headless: bool = True):
        # Respect TILL_HEADFUL env var; otherwise use config/default
        # Note: Schwab tends to block headless browsers
        super().__init__(headless=headless)

        self.transaction_account = os.environ.get("TILL_SCHWAB_TRANSACTION_ACCOUNT", "")

        # Only include accounts matching these suffixes (empty = include all)
        include_str = os.environ.get("TILL_SCHWAB_INCLUDE_ACCOUNTS", "")
        self.include_accounts = [s.strip() for s in include_str.split(",") if s.strip()] if include_str else []

    async def navigate_and_login(self, page, username: str, password: str):
        login_url = "https://client.schwab.com/Login/SignOn/CustomerCenterLogin.aspx"
        print(f"   Navigating to {login_url}", file=sys.stderr)
        await page.goto(login_url, wait_until="domcontentloaded", timeout=300000)

        if "client.schwab.com/app/accounts" in page.url:
            print("   Already logged in (session active)", file=sys.stderr)
            return

        if username and password:
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
                        continue

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

                    print("   Clicking login...", file=sys.stderr)
                    try:
                        login_button = frame.get_by_role("button", name="Log in")
                        await login_button.click()
                    except Exception:
                        await frame.locator('#btnLogin').click()

                print("   Waiting for login to complete...", file=sys.stderr)
                try:
                    await page.wait_for_url("**/client.schwab.com/**", timeout=120000)
                except Exception:
                    pass

                current_url = page.url
                if "client.schwab.com/app/accounts" in current_url:
                    print("   Login successful!", file=sys.stderr)
                else:
                    print(f"   Post-login page: {current_url}", file=sys.stderr)
                    print("   Waiting for 2FA/security challenge...", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_schwab_2fa.png")
                    try:
                        await page.wait_for_url("**/app/accounts/**", timeout=120000)
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
        await page.screenshot(path="/tmp/till_schwab_accounts.png")
        print("   Screenshot: /tmp/till_schwab_accounts.png", file=sys.stderr)

        # Extract accounts
        all_accounts = await self._extract_accounts(page)

        # Filter by include_accounts if configured
        if self.include_accounts:
            accounts = [a for a in all_accounts if a.get("account_suffix") in self.include_accounts]
            skipped = len(all_accounts) - len(accounts)
            if skipped:
                print(f"   Filtered to {len(accounts)} accounts (skipped {skipped} not in include_accounts)", file=sys.stderr)
        else:
            accounts = all_accounts

        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Extract transactions for configured account
        transactions = []
        if self.transaction_account:
            # Find the account_id for the transaction account
            txn_acct = next(
                (a for a in accounts if a.get("account_suffix") == self.transaction_account),
                None
            )
            if txn_acct:
                acct_id = hashlib.md5(f"schwab_{self.transaction_account}".encode()).hexdigest()[:16]
                transactions = await self._extract_transactions(page, acct_id)
                print(f"   Found {len(transactions)} transactions for ...{self.transaction_account}", file=sys.stderr)
            else:
                print(f"   Transaction account ...{self.transaction_account} not in filtered accounts", file=sys.stderr)

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

    async def _extract_accounts(self, page) -> list[dict]:
        """Extract accounts. Only real accounts with 'Account number ending in' are included."""
        accounts = []

        accounts_wrapper = await page.query_selector('div.allAccountsWrapper')
        if not accounts_wrapper:
            accounts_wrapper = await page.query_selector(
                '[class*="allAccounts"], [class*="AccountsWrapper"]'
            )

        search_context = accounts_wrapper or page

        account_rows = await search_context.query_selector_all(
            'sdps-table-row, table tbody tr, [class*="AccountRow"]'
        )

        for row in account_rows:
            try:
                is_header = await row.get_attribute('account-list-table-group-header')
                if is_header is not None:
                    continue

                account_num_el = await row.query_selector('span.sr-only')
                account_suffix = None
                if account_num_el:
                    sr_text = await account_num_el.inner_text()
                    match = re.search(r'Account number ending in\s+(\S+)', sr_text)
                    if match:
                        account_suffix = match.group(1)

                if account_suffix is None:
                    continue

                cells = await row.query_selector_all('sdps-table-cell, td')
                if len(cells) < 4:
                    continue

                cell_texts = []
                for cell in cells:
                    text = await cell.inner_text()
                    cell_texts.append(text.strip())

                raw_name = cell_texts[0]
                name_lines = [l.strip() for l in raw_name.split('\n') if l.strip()]
                name = name_lines[0] if name_lines else raw_name
                name = name.rstrip('\u2020').strip()
                if not name or 'Total' in name:
                    continue

                # Detect account type
                account_type = "other"
                type_text = cell_texts[1] if len(cell_texts) > 1 else ""
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

                if account_type == "other":
                    name_lower = name.lower()
                    if 'checking' in name_lower:
                        account_type = "checking"
                    elif 'saving' in name_lower:
                        account_type = "savings"
                    elif '401' in name_lower:
                        account_type = "401k"
                    elif any(w in name_lower for w in ['ira', 'roth', 'retirement']):
                        account_type = "ira"
                    elif any(w in name_lower for w in ['529', 'etrade', 'individual']):
                        account_type = "brokerage"

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

    async def _extract_transactions(self, page, account_id: str) -> list[dict]:
        """Extract transactions from Schwab transaction history.

        Columns: Date | Type | Check Number | Description | Withdrawal | Deposit | Running Balance
        """
        transactions = []

        try:
            history_url = "https://client.schwab.com/app/accounts/history/#/"
            print(f"   Navigating to transaction history...", file=sys.stderr)
            await page.goto(history_url, wait_until="domcontentloaded", timeout=60000)
            # SPA needs extra time to render
            await page.wait_for_timeout(8000)

            # Take a screenshot to see what loaded
            await page.screenshot(path="/tmp/till_schwab_history_pre.png")

            # Try to select the right account via dropdown
            if self.transaction_account:
                await self._select_account_dropdown(page, self.transaction_account)
                # Wait for table to re-render after account selection
                await page.wait_for_timeout(5000)

            # Wait for the Schwab table — click Search first to trigger loading
            try:
                search_btn = await page.query_selector('button:has-text("Search")')
                if search_btn and await search_btn.is_visible():
                    print("   Clicking Search to load transactions...", file=sys.stderr)
                    await search_btn.click()
                    await page.wait_for_timeout(5000)
            except Exception:
                pass

            try:
                await page.wait_for_selector('table.sdps-table', timeout=30000)
                print("   Found transaction table", file=sys.stderr)
            except Exception:
                try:
                    await page.wait_for_selector('table', timeout=10000)
                    print("   Found generic table", file=sys.stderr)
                except Exception:
                    print("   Transaction table not found", file=sys.stderr)

            await page.screenshot(path="/tmp/till_schwab_transactions.png")
            await page.wait_for_timeout(2000)

            # Scroll to load more content
            for _ in range(3):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1000)

            # Click "Show More" buttons to load all transactions
            for attempt in range(10):
                clicked = False
                for selector in [
                    'button:has-text("Show More")',
                    'button:has-text("Load More")',
                    'button:has-text("Show more")',
                    '[class*="show-more"]',
                    '[class*="load-more"]',
                ]:
                    btn = await page.query_selector(selector)
                    if btn:
                        is_visible = await btn.is_visible()
                        if is_visible:
                            print(f"   Loading more transactions (page {attempt + 1})...", file=sys.stderr)
                            await btn.click()
                            await page.wait_for_timeout(2000)
                            clicked = True
                            break
                if not clicked:
                    break

            await page.screenshot(path="/tmp/till_schwab_transactions.png")

            # Find transaction rows
            txn_rows = []
            for selector in [
                'table.sdps-table tbody tr',
                'table[class*="sdps-table"] tbody tr',
                '.sdps-table tbody tr',
                'table tbody tr',
            ]:
                txn_rows = await page.query_selector_all(selector)
                if txn_rows:
                    break

            print(f"   Found {len(txn_rows)} transaction rows", file=sys.stderr)

            # Detect column layout from header
            # Bank/Checking: Date | Type | Check Number | Description | Withdrawal | Deposit | Running Balance
            # Brokerage:     Date | Transaction Type | Symbol | Description | Quantity | Price | Fees & Comm | Amount
            header_text = ""
            try:
                header_row = await page.query_selector('table.sdps-table thead tr, table thead tr')
                if header_row:
                    header_text = (await header_row.inner_text()).upper()
            except Exception:
                pass

            is_brokerage = 'SYMBOL' in header_text or 'QUANTITY' in header_text
            if is_brokerage:
                print("   Detected brokerage transaction layout", file=sys.stderr)
            else:
                print("   Detected bank/checking transaction layout", file=sys.stderr)

            parsed = 0
            for row in txn_rows:
                try:
                    cells = await row.query_selector_all('td')
                    if len(cells) < 4:
                        continue

                    cell_texts = []
                    for cell in cells:
                        text = await cell.inner_text()
                        cell_texts.append(text.strip())

                    # Skip header/total rows
                    if 'DATE' in cell_texts[0].upper() or 'TOTAL' in cell_texts[0].upper():
                        continue

                    # Parse date from first column
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', cell_texts[0])
                    if not date_match:
                        continue
                    raw_date = date_match.group(1)
                    parts = raw_date.split('/')
                    if len(parts) != 3:
                        continue
                    month, day, year = parts
                    if len(year) == 2:
                        year = '20' + year
                    date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

                    if is_brokerage:
                        # Brokerage: Date | Type | Symbol | Description | Quantity | Price | Fees | Amount
                        description = cell_texts[3] if len(cell_texts) > 3 else cell_texts[1]
                        # Amount is the last column
                        amount = self._parse_signed_amount(cell_texts[-1])
                        if amount is None:
                            continue
                    else:
                        # Bank: Date | Type | Check# | Description | Withdrawal | Deposit | Balance
                        if len(cell_texts) >= 7:
                            description = cell_texts[3]
                            withdrawal_amt = self._parse_amount(cell_texts[4])
                            deposit_amt = self._parse_amount(cell_texts[5])
                        elif len(cell_texts) >= 5:
                            description = cell_texts[1]
                            withdrawal_amt = self._parse_amount(cell_texts[2])
                            deposit_amt = self._parse_amount(cell_texts[3])
                        else:
                            continue

                        if withdrawal_amt:
                            amount = -withdrawal_amt
                        elif deposit_amt:
                            amount = deposit_amt
                        else:
                            continue

                    if parsed < 3:
                        print(f"   Row: {date} | {description[:40]} | ${amount:,.2f}", file=sys.stderr)

                    txn_id = hashlib.md5(
                        f"{date}_{description}_{amount}".encode()
                    ).hexdigest()[:16]

                    transactions.append({
                        "id": txn_id,
                        "account_id": account_id,
                        "date": date,
                        "description": description.strip(),
                        "amount": amount,
                        "category": self._infer_category(description),
                        "status": "posted",
                    })
                    parsed += 1

                except Exception:
                    continue

        except Exception as e:
            print(f"   Error extracting transactions: {e}", file=sys.stderr)

        return transactions

    async def _select_account_dropdown(self, page, account_suffix: str):
        """Try to select a specific account in the transaction history dropdown."""
        try:
            # The account dropdown on the history page shows current account like
            # "Paul Jenn Savings\n…337" in a styled button with a chevron
            dropdown_btn = None

            # Look for buttons that contain an account number pattern (…XXX or ...XXX)
            buttons = await page.query_selector_all('button')
            for btn in buttons:
                try:
                    text = await btn.inner_text()
                    # Account dropdown contains ellipsis + digits pattern
                    # Unicode ellipsis (…) or three dots (...) followed by digits
                    if re.search(r'(?:…|\.\.\.)[\d-]{2,4}', text) and await btn.is_visible():
                        dropdown_btn = btn
                        break
                except Exception:
                    continue

            if not dropdown_btn:
                print(f"   No account dropdown found", file=sys.stderr)
                return

            btn_text = await dropdown_btn.inner_text()
            print(f"   Account dropdown shows: {btn_text.strip()[:50]}", file=sys.stderr)

            # Check if already showing the right account
            if account_suffix in btn_text:
                print(f"   Already showing account ...{account_suffix}", file=sys.stderr)
                return

            # Click to open dropdown
            await dropdown_btn.click()
            await page.wait_for_timeout(2000)

            # Find the target account in dropdown options
            # Look for any clickable element containing the account suffix
            found = False
            for selector in ['li', 'a', 'button', 'div[role="option"]', 'span']:
                elements = await page.query_selector_all(selector)
                for elem in elements:
                    try:
                        text = await elem.inner_text()
                        if account_suffix in text and await elem.is_visible():
                            # Skip transaction descriptions that mention the account
                            if 'TRANSFER' in text.upper() or re.search(r'\d{2}/\d{2}/', text):
                                continue
                            print(f"   Selecting: {text.strip()[:60]}", file=sys.stderr)
                            await elem.click()
                            await page.wait_for_timeout(5000)
                            found = True
                            break
                    except Exception:
                        continue
                if found:
                    break

            if not found:
                print(f"   Could not find account ...{account_suffix} in dropdown", file=sys.stderr)

        except Exception as e:
            print(f"   Could not select account dropdown: {e}", file=sys.stderr)

    @staticmethod
    def _parse_signed_amount(text: str) -> float | None:
        """Parse a dollar amount that may have a sign, e.g. '-$0.02' or '$100.00'."""
        if not text or not text.strip():
            return None
        match = re.search(r'(-?)?\$?([\d,]+\.?\d*)', text)
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
    def _infer_category(description: str) -> str:
        desc = description.upper()
        if any(w in desc for w in ['PAYROLL', 'DIRECT DEP', 'SALARY']):
            return "Income"
        if any(w in desc for w in ['TRANSFER', 'XFER']):
            return "Transfer"
        if any(w in desc for w in ['ATM', 'WITHDRAWAL']):
            return "ATM"
        if any(w in desc for w in ['INTEREST', 'DIVIDEND']):
            return "Interest"
        if any(w in desc for w in ['CHECK']):
            return "Check"
        return "Other"
