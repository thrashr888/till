"""Chase credit card scraper.

Strategy: Login via Playwright, then use API interception (page.on("response"))
to capture account and transaction data from Chase's internal APIs.
Falls back to DOM/text extraction if API interception doesn't work.

Chase aggressively detects headless browsers, so headful mode is always forced.
"""

import re
import sys
import hashlib
import json
import os
from till_scrapers.base import BaseScraper


class ChaseScraper(BaseScraper):
    LOGIN_URL = "https://secure.chase.com/web/auth/dashboard"

    def __init__(self, headless: bool = True):
        # Chase always requires headful — it aggressively detects headless
        if headless:
            print("   Enforcing headful mode for Chase (bot detection).", file=sys.stderr)
        super().__init__(headless=False)

        # Config from env: semicolon-separated "name,last4" pairs
        self.chase_accounts = []
        accounts_str = os.environ.get("TILL_CHASE_ACCOUNTS", "")
        if accounts_str:
            for acct in accounts_str.split(";"):
                parts = acct.split(",")
                if len(parts) >= 2:
                    entry = {"name": parts[0].strip(), "last4": parts[1].strip()}
                    if len(parts) >= 3:
                        entry["url"] = parts[2].strip()
                    self.chase_accounts.append(entry)

    async def navigate_and_login(self, page, username: str, password: str):
        """Navigate to Chase and log in if needed."""
        # Check for active session first
        print("   Checking for active session...", file=sys.stderr)
        try:
            await page.goto(
                self.LOGIN_URL,
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(3000)
        except Exception as e:
            print(f"   Navigation error: {e}", file=sys.stderr)
            raise

        # Check if we're already logged in (dashboard loaded, not on login page)
        current_url = page.url
        needs_login = await self._needs_login(page)

        if not needs_login:
            print("   Session active, skipping login", file=sys.stderr)
            return

        # Session expired — need to log in
        print("   Session expired, logging in...", file=sys.stderr)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source chase` "
                "or set TILL_USERNAME/TILL_PASSWORD env vars."
            )

        await self._do_login(page, username, password)
        await page.wait_for_timeout(3000)

    async def _needs_login(self, page) -> bool:
        """Check if we're on a login page."""
        await page.wait_for_timeout(2000)
        url = page.url

        # Check URL patterns that indicate login is needed
        if any(x in url.lower() for x in ["logon", "login", "signin"]):
            return True

        # Check for login iframe (Chase uses #logonbox iframe)
        iframe = await page.query_selector('iframe#logonbox')
        if iframe:
            return True

        # Check for login form elements
        login_selectors = [
            '#userId-text-input-field',
            '#userId-input-field-input',
            'input[name="userId"]',
            '#password-text-input-field',
            '#password-input-field-input',
            'input[type="password"]',
        ]
        for selector in login_selectors:
            elem = await page.query_selector(selector)
            if elem:
                try:
                    if await elem.is_visible():
                        return True
                except Exception:
                    pass

        return False

    async def _do_login(self, page, username: str, password: str):
        """Perform login to Chase."""
        print("   Looking for login form...", file=sys.stderr)

        try:
            # Chase may use a login iframe (#logonbox)
            iframe_handle = await page.query_selector('iframe#logonbox')
            login_frame = page

            if iframe_handle:
                print("   Found login iframe, switching context...", file=sys.stderr)
                login_frame = await iframe_handle.content_frame()
                if not login_frame:
                    raise Exception("Could not access login iframe content")

            # Wait for the login form to appear
            print("   Waiting for login form...", file=sys.stderr)
            try:
                await login_frame.wait_for_selector(
                    '#userId-text-input-field, #userId-input-field-input, input[name="userId"]',
                    timeout=30000,
                )
            except Exception:
                await page.screenshot(path="/tmp/till_chase_login_form_wait.png")
                raise

            # Fill username with human-like typing
            print("   Entering username...", file=sys.stderr)
            username_field = None
            for selector in ['#userId-text-input-field', '#userId-input-field-input', 'input[name="userId"]']:
                username_field = await login_frame.query_selector(selector)
                if username_field:
                    break

            if username_field:
                await username_field.click()
                await page.wait_for_timeout(200)
                existing = await username_field.input_value()
                if existing and existing != username:
                    await username_field.fill("")
                    await page.wait_for_timeout(100)
                if not existing or existing != username:
                    await username_field.type(username, delay=50)
                await page.wait_for_timeout(300)
            else:
                print("   Warning: Could not find username field", file=sys.stderr)

            # Fill password
            print("   Entering password...", file=sys.stderr)
            password_field = None
            for selector in ['#password-text-input-field', '#password-input-field-input', 'input[name="password"]']:
                password_field = await login_frame.query_selector(selector)
                if password_field:
                    break

            if password_field:
                await password_field.click()
                await page.wait_for_timeout(200)
                existing_pw = await password_field.input_value()
                if existing_pw:
                    await password_field.fill("")
                    await page.wait_for_timeout(100)
                await password_field.type(password, delay=50)
                await page.wait_for_timeout(300)
            else:
                print("   Warning: Could not find password field", file=sys.stderr)

            # Check "Remember me" if available
            for sel in ['#rememberMe', 'input[name="rememberMe"]', '#rememberMeCheckbox']:
                remember = await login_frame.query_selector(sel)
                if remember:
                    try:
                        await remember.check()
                    except Exception:
                        pass
                    break

            # Submit login
            print("   Submitting login...", file=sys.stderr)
            await page.wait_for_timeout(500)

            submitted = False
            # Try pressing Enter on password field first (more human-like)
            if password_field:
                try:
                    await password_field.press("Enter")
                    print("   Pressed Enter to submit", file=sys.stderr)
                    submitted = True
                except Exception:
                    pass

            if not submitted:
                # Click the sign-in button
                for sel in ['#signin-button', '#signin-button-content', 'button[type="submit"]']:
                    btn = await login_frame.query_selector(sel)
                    if btn:
                        try:
                            await btn.click()
                            print("   Clicked sign in button", file=sys.stderr)
                            submitted = True
                            break
                        except Exception:
                            pass

            if not submitted:
                # Frame locator fallback
                try:
                    frame_locator = page.frame_locator('iframe#logonbox')
                    await frame_locator.locator('#signin-button').click(timeout=3000)
                    submitted = True
                except Exception:
                    print("   Warning: Could not submit login", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_chase_no_submit.png")

            # Wait for login/2FA to complete
            print("   Waiting for login to complete...", file=sys.stderr)

            try:
                await page.wait_for_selector(
                    '[class*="account"], [class*="Account"], [data-testid*="account"]',
                    timeout=30000,
                )
                print("   Login successful!", file=sys.stderr)
            except Exception:
                current_url = page.url
                if "dashboard" in current_url.lower() or "account" in current_url.lower():
                    print("   Appears to be logged in, continuing...", file=sys.stderr)
                elif "identity" in current_url.lower() or "verify" in current_url.lower():
                    print("   2FA required — approve on Chase mobile app...", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_chase_2fa.png")
                    raise Exception(
                        "2FA required. Run `till test --source chase --headful --pause` to approve."
                    )
                else:
                    print(f"   Post-login URL: {current_url}", file=sys.stderr)
                    raise Exception(f"Login may have failed. URL: {current_url}")

        except Exception as e:
            err_msg = str(e)
            # "Frame was detached" means login succeeded but iframe was replaced — not a real error
            if "Frame was detached" in err_msg or "frame was detached" in err_msg:
                print("   Login iframe detached (normal after submit), continuing...", file=sys.stderr)
                await page.wait_for_timeout(3000)
                return
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_chase_login_error.png")
            raise

    async def extract(self, page) -> dict:
        """Extract accounts and transactions using API interception + DOM fallback."""

        # Step 1: Set up API interception
        api_responses = {}

        async def capture_api(response):
            url = response.url
            if '/api/' in url or '/svc/' in url or 'api-' in url:
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

        # Step 2: Navigate to dashboard to trigger API calls
        print("   Loading dashboard...", file=sys.stderr)
        current_url = page.url
        if "dashboard" not in current_url.lower():
            await page.goto(
                "https://secure.chase.com/web/auth/dashboard",
                wait_until="domcontentloaded",
                timeout=30000,
            )
        await page.wait_for_timeout(8000)

        await page.screenshot(path="/tmp/till_chase_dashboard.png")
        print("   Screenshot: /tmp/till_chase_dashboard.png", file=sys.stderr)

        # Step 3: Try to get accounts from intercepted API responses
        accounts = []
        for url, data in api_responses.items():
            if any(k in url.lower() for k in ['account', 'card', 'credit']):
                api_accounts = self._parse_accounts_from_api(data)
                if api_accounts:
                    accounts.extend(api_accounts)

        # Step 4: Fallback to DOM/text extraction if API didn't find accounts
        if not accounts:
            print("   API didn't return accounts, falling back to DOM/text...", file=sys.stderr)
            accounts = await self._extract_accounts_dom(page)

        # Filter by configured accounts if set
        if self.chase_accounts and accounts:
            configured_last4s = {a["last4"] for a in self.chase_accounts}
            configured_names = {a["name"].lower() for a in self.chase_accounts}
            before = len(accounts)
            accounts = [
                a for a in accounts
                if a.get("last4") in configured_last4s
                or a.get("name", "").lower() in configured_names
            ]
            skipped = before - len(accounts)
            if skipped:
                print(f"   Filtered to {len(accounts)} accounts (skipped {skipped})", file=sys.stderr)

        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Step 5: Extract transactions for each account
        transactions = []
        for acct in accounts:
            api_responses.clear()
            acct_txns = await self._extract_transactions(page, acct, api_responses)
            transactions.extend(acct_txns)

        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

        # Save API dump for debugging
        if api_responses:
            debug_path = "/tmp/till_chase_api_dump.json"
            try:
                with open(debug_path, 'w') as f:
                    json.dump(api_responses, f, indent=2, default=str)
                print(f"   Saved API dump to {debug_path}", file=sys.stderr)
            except Exception:
                pass

        # Build results
        account_results = []
        for acct in accounts:
            account_results.append({
                "account_id": acct["id"],
                "account_name": acct["name"],
                "account_type": "credit",
                "balance": acct.get("balance", 0),
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
            })

        return {
            "status": "ok",
            "source": "chase",
            "accounts": account_results,
            "transactions": txn_results,
            "positions": [],
            "balance_history": [],
        }

    # ── API parsing ──────────────────────────────────────────────────

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Chase's internal API response.

        Chase card/list API shape:
        {
            "nickname": "Prime Visa",
            "mask": "6173",
            "accountId": 708766523,
            "detail": {
                "currentBalance": 1735.15,
                "availableCredit": 27003.17,
                "creditLimit": 28800.0,
                ...
            }
        }
        """
        accounts = []

        # Chase card/list returns a single card object (not a list)
        if isinstance(data, dict):
            # Direct card object with nickname + mask + detail
            if 'nickname' in data and 'mask' in data:
                acct = self._parse_chase_card(data)
                if acct:
                    accounts.append(acct)
                return accounts

            # Look for nested lists
            for key in ['accounts', 'Accounts', 'accountList', 'cards', 'cardList']:
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        acct = self._parse_chase_card(item)
                        if acct:
                            accounts.append(acct)
                    return accounts

        if isinstance(data, list):
            for item in data:
                acct = self._parse_chase_card(item)
                if acct:
                    accounts.append(acct)

        return accounts

    def _parse_chase_card(self, item: dict) -> dict | None:
        """Parse a single Chase credit card from API data."""
        if not isinstance(item, dict):
            return None

        name = item.get('nickname') or item.get('accountName') or item.get('displayName') or ''
        last4 = str(item.get('mask') or item.get('accountNumber') or '')[-4:]

        detail = item.get('detail', {})
        balance = float(detail.get('currentBalance', 0) or item.get('currentBalance', 0) or 0)
        available = float(detail.get('availableCredit', 0) or item.get('availableCredit', 0) or 0)
        credit_limit = float(detail.get('creditLimit', 0) or 0)

        if not name:
            return None

        account_id = hashlib.md5(f"chase_{name}_{last4}".encode()).hexdigest()[:16]
        print(f"   API: {name} ...{last4}: balance=${balance:,.2f} avail=${available:,.2f}", file=sys.stderr)

        return {
            "id": account_id,
            "name": name,
            "balance": balance,
            "available_credit": available,
            "credit_limit": credit_limit,
            "last4": last4,
            "chase_account_id": str(item.get('accountId', '')),
        }

    def _parse_transactions_from_api(self, data, account_id: str) -> list[dict]:
        """Parse transactions from Chase's internal API response.

        Chase activities API shape:
        {
            "activities": [{
                "transactionDate": "2026-03-14",
                "transactionAmount": 15.16,
                "creditDebitCode": "D",  // D=debit (charge), C=credit (payment)
                "transactionStatusCode": "Pending" or "Posted",
                "etuStandardTransactionTypeGroupName": "PURCHASE",
                "merchantDetails": {"rawMerchantDetails": {...}, "enrichedMerchants": [...]},
            }]
        }
        """
        transactions = []

        items = data.get('activities', []) if isinstance(data, dict) else []
        if not items:
            # Try other keys
            for key in ['transactions', 'postedTransactions', 'pendingTransactions']:
                if isinstance(data, dict) and key in data and isinstance(data[key], list):
                    items = data[key]
                    break

        for item in items:
            if not isinstance(item, dict):
                continue

            date = item.get('transactionDate') or item.get('postDate') or ''
            amount = float(item.get('transactionAmount', 0) or item.get('amount', 0) or 0)

            # Get merchant name from merchantDetails
            merchant_details = item.get('merchantDetails', {})
            desc = ''
            enriched = merchant_details.get('enrichedMerchants', [])
            if enriched and isinstance(enriched, list):
                desc = enriched[0].get('merchantName', '') if isinstance(enriched[0], dict) else ''
            if not desc:
                raw = merchant_details.get('rawMerchantDetails', {})
                desc = raw.get('name', '') if isinstance(raw, dict) else ''
            if not desc:
                desc = item.get('description') or item.get('merchantName') or ''
            if not desc:
                desc = item.get('etuStandardTransactionTypeGroupName', 'Unknown')

            if not date:
                continue

            iso_date = self._normalize_date(date)

            # D=debit (charge), C=credit (payment/refund)
            credit_debit = item.get('creditDebitCode', 'D')
            if credit_debit == 'D':
                amount = -abs(amount)  # charges negative
            else:
                amount = abs(amount)   # payments/credits positive

            status = 'pending' if item.get('transactionStatusCode', '').lower() == 'pending' else 'posted'

            category_group = item.get('etuStandardTransactionTypeGroupName', '')
            category = self._map_chase_category(category_group, desc)

            txn_id = hashlib.md5(
                f"chase_{iso_date}_{desc}_{amount}_{item.get('sorTransactionIdentifier', '')}".encode()
            ).hexdigest()[:16]

            transactions.append({
                "id": txn_id,
                "account_id": account_id,
                "date": iso_date,
                "description": desc.strip()[:100],
                "amount": amount,
                "category": category,
                "status": status,
            })

        return transactions

    @staticmethod
    def _map_chase_category(group: str, desc: str) -> str:
        """Map Chase category group to standard category, with description fallback."""
        group_map = {
            'PURCHASE': 'Shopping',
            'PAYMENT': 'Payment',
            'RETURN': 'Refund',
            'FEE': 'Fee',
            'INTEREST': 'Interest',
            'REWARD': 'Reward',
            'ADJUSTMENT': 'Adjustment',
        }
        mapped = group_map.get(group.upper(), '')
        if mapped:
            return mapped
        return ChaseScraper._infer_category(desc)

    # ── DOM / text extraction ────────────────────────────────────────

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Extract credit card accounts from the dashboard via DOM/text parsing."""
        accounts = []

        # Strategy 1: JavaScript to find card names with last-4 pattern
        try:
            card_data = await page.evaluate(r'''() => {
                const cards = [];
                const allElements = document.querySelectorAll('a, button, span, h2, h3, h4, [role="heading"]');
                for (const el of allElements) {
                    const text = (el.textContent || "").trim();
                    const match = text.match(/^(.+?)\s*\((?:\.{2,3}|\u2026)(\d{4})\)\s*$/);
                    if (!match) continue;
                    const name = match[1].trim();
                    const lastFour = match[2];
                    if (name.length < 3 || name.length > 50) continue;

                    let section = null;
                    let el2 = el;
                    for (let i = 0; i < 12; i++) {
                        el2 = el2.parentElement;
                        if (!el2) break;
                        const t = el2.innerText || "";
                        if ((t.includes("Current balance") || t.includes("Available credit"))
                            && t.length > 50) {
                            section = el2;
                            break;
                        }
                    }
                    if (!section) section = el.parentElement?.parentElement?.parentElement?.parentElement;
                    const sectionText = section ? section.innerText : "";
                    cards.push({
                        name: name,
                        last_four: lastFour,
                        text: sectionText.substring(0, 1500),
                        href: el.href || el.closest('a')?.href || ""
                    });
                }
                return cards;
            }''')

            page_text = await page.inner_text('body')

            for card in (card_data or []):
                last4 = card.get("last_four", "")
                name = card.get("name", "Chase Card")
                account_id = hashlib.md5(f"chase_{name}_{last4}".encode()).hexdigest()[:16]

                acct = {
                    "id": account_id,
                    "name": name,
                    "last4": last4,
                    "balance": 0,
                    "available_credit": None,
                }

                if card.get("href"):
                    acct["url"] = card["href"]

                # Parse balance from section text or full page text
                text = card.get("text", "")
                if not text or "Current balance" not in text:
                    text = page_text

                balance_match = re.search(
                    r'\$([\d,]+\.\d{2})\s*\n?\s*Current balance', text
                )
                if not balance_match:
                    balance_match = re.search(
                        r'Current balance\s*\n?\s*\$([\d,]+\.\d{2})', text
                    )
                if balance_match:
                    acct["balance"] = float(balance_match.group(1).replace(",", ""))

                avail_match = re.search(
                    r'\$([\d,]+\.\d{2})\s*\n?\s*Available credit', text
                )
                if not avail_match:
                    avail_match = re.search(
                        r'Available credit\s*\n?\s*\$([\d,]+\.\d{2})', text
                    )
                if avail_match:
                    acct["available_credit"] = float(avail_match.group(1).replace(",", ""))

                print(
                    f"   DOM: {name} ...{last4}: "
                    f"${acct['balance']:,.2f}, "
                    f"avail=${acct.get('available_credit') or 0:,.2f}",
                    file=sys.stderr,
                )
                accounts.append(acct)

        except Exception as e:
            print(f"   JS card detection failed: {e}", file=sys.stderr)

        # Strategy 2: Text-based regex fallback
        if not accounts:
            try:
                page_text = await page.inner_text('body')

                # Match "Card Name (...NNNN)" on its own line
                for match in re.finditer(
                    r'^([A-Z][\w\s]{2,40}?)\s*\((?:\.{2,3}|\u2026)(\d{4})\)',
                    page_text,
                    re.MULTILINE,
                ):
                    name = match.group(1).strip()
                    last4 = match.group(2)
                    if '\n' in name or len(name) > 40 or len(name) < 3:
                        continue

                    account_id = hashlib.md5(f"chase_{name}_{last4}".encode()).hexdigest()[:16]
                    acct = {
                        "id": account_id,
                        "name": name,
                        "last4": last4,
                        "balance": 0,
                        "available_credit": None,
                    }

                    # Parse from text after card name
                    section = page_text[match.end():match.end() + 800]
                    bal = re.search(r'\$([\d,]+\.\d{2})\s*\n?\s*Current balance', section)
                    if not bal:
                        bal = re.search(r'Current balance\s*\n?\s*\$([\d,]+\.\d{2})', section)
                    if bal:
                        acct["balance"] = float(bal.group(1).replace(",", ""))

                    avail = re.search(r'\$([\d,]+\.\d{2})\s*\n?\s*Available credit', section)
                    if not avail:
                        avail = re.search(r'Available credit\s*\n?\s*\$([\d,]+\.\d{2})', section)
                    if avail:
                        acct["available_credit"] = float(avail.group(1).replace(",", ""))

                    print(f"   Text: {name} ...{last4}: ${acct['balance']:,.2f}", file=sys.stderr)
                    accounts.append(acct)

            except Exception as e:
                print(f"   Text extraction failed: {e}", file=sys.stderr)

        # Strategy 3: CSS selector tiles
        if not accounts:
            print("   Trying CSS tile selectors...", file=sys.stderr)
            for selector in [
                'div[class*="credit-card"]',
                'div[class*="account-tile"]',
                'section[class*="credit"]',
                'mds-tile',
            ]:
                cards = await page.query_selector_all(selector)
                if cards:
                    print(f"   Found {len(cards)} tiles with: {selector}", file=sys.stderr)
                    for card in cards:
                        try:
                            text = await card.inner_text()
                            acct = self._parse_account_tile(text)
                            if acct:
                                accounts.append(acct)
                        except Exception as e:
                            print(f"   Tile parse error: {e}", file=sys.stderr)
                    break

        return accounts

    def _parse_account_tile(self, text: str) -> dict | None:
        """Parse a credit card account from tile/section text."""
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        if not lines:
            return None

        # Find the card name
        name = None
        skip_words = [
            "current balance", "available credit", "credit limit", "payment due",
            "minimum payment", "details", "view", "pay", "more", "menu",
            "pending", "posted", "recent", "activity", "transactions",
        ]
        for line in lines:
            ll = line.lower().strip()
            if line.startswith("$") or line.replace(",", "").replace(".", "").isdigit():
                continue
            if ll in skip_words or len(line) < 4 or line.startswith("..."):
                continue
            if any(kw in ll for kw in [
                "sapphire", "freedom", "slate", "ink", "amazon",
                "marriott", "united", "southwest", "aarp",
            ]):
                name = line
                break
            if not name:
                name = line

        if not name:
            name = "Chase Card"

        # Last 4 digits
        last4 = ""
        m = re.search(r'(?:\.{2,3}|\u2026)(\d{4})', text)
        if m:
            last4 = m.group(1)

        account_id = hashlib.md5(f"chase_{name}_{last4}".encode()).hexdigest()[:16]

        acct = {
            "id": account_id,
            "name": name,
            "last4": last4,
            "balance": 0,
            "available_credit": None,
        }

        all_text = " ".join(lines)

        # Current balance
        bal = re.search(r'\$([\d,]+\.?\d*)\s*(?:current\s+balance)', all_text, re.IGNORECASE)
        if not bal:
            bal = re.search(r'(?:current\s+balance)\s*\$?([\d,]+\.?\d*)', all_text, re.IGNORECASE)
        if bal:
            try:
                acct["balance"] = float(bal.group(1).replace(",", ""))
            except ValueError:
                pass

        # Available credit
        av = re.search(r'\$([\d,]+\.?\d*)\s*(?:available\s+credit)', all_text, re.IGNORECASE)
        if not av:
            av = re.search(r'(?:available\s+credit)\s*\$?([\d,]+\.?\d*)', all_text, re.IGNORECASE)
        if av:
            try:
                acct["available_credit"] = float(av.group(1).replace(",", ""))
            except ValueError:
                pass

        # Only return if we found some balance data
        if acct["balance"] or acct.get("available_credit"):
            return acct

        # Last resort: use first dollar amount
        amounts = re.findall(r'\$([\d,]+\.?\d*)', all_text)
        for amt in amounts:
            try:
                value = float(amt.replace(",", ""))
                if 1 <= value <= 100000:
                    acct["balance"] = value
                    return acct
            except ValueError:
                continue

        return None

    # ── Transaction extraction ───────────────────────────────────────

    async def _extract_transactions(self, page, account: dict, api_responses: dict) -> list[dict]:
        """Extract full transaction history using direct API calls with session cookies.

        Chase API: /svc/rr/accounts/secure/gateway/credit-card/transactions/
        inquiry-maintenance/etu-transactions/v4/accounts/transactions
        Supports record-count=200 and provides statementCycles for pagination.
        """
        transactions = []
        account_id = account["id"]
        account_name = account.get("name", "Chase Card")
        chase_acct_id = account.get("chase_account_id", "")

        if not chase_acct_id:
            print(f"   No Chase account ID for {account_name}, skipping", file=sys.stderr)
            return transactions

        base_url = (
            "https://secure.chase.com/svc/rr/accounts/secure/gateway/credit-card/"
            "transactions/inquiry-maintenance/etu-transactions/v4/accounts/transactions"
        )

        print(f"   Fetching transactions for {account_name} (all history)...", file=sys.stderr)
        try:
            # First call — get current transactions + statement cycles list
            url = (
                f"{base_url}?digital-account-identifier={chase_acct_id}"
                f"&provide-available-statement-indicator=true"
                f"&record-count=200&sort-order-code=D&sort-key-code=T"
            )
            response = await page.request.get(url, timeout=30000)
            if not response.ok:
                print(f"   Transaction API returned {response.status}", file=sys.stderr)
                return transactions

            data = await response.json()
            txns = self._parse_transactions_from_api(data, account_id)
            transactions.extend(txns)
            print(f"   Current cycle: {len(txns)} transactions", file=sys.stderr)

            # Save for debugging
            with open(f"/tmp/till_chase_{account_id}_txns.json", 'w') as f:
                json.dump(data, f, indent=2, default=str)

        except Exception as e:
            print(f"   Transaction API failed: {e}", file=sys.stderr)

        # Check intercepted responses as fallback
        if not transactions:
            for url, data in api_responses.items():
                if 'transaction' in url.lower():
                    txns = self._parse_transactions_from_api(data, account_id)
                    transactions.extend(txns)

        print(f"   Found {len(transactions)} transactions for {account_name}", file=sys.stderr)
        return transactions

    def _parse_transactions_from_text(self, page_text: str, account_id: str) -> list[dict]:
        """Parse transactions from page text using regex patterns.

        Chase shows transactions as:
          Dec 21, 2025
          MERCHANT NAME
          $17.18
        """
        transactions = []
        seen = set()
        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        }

        lines = page_text.split('\n')
        current_date = None
        i = 0

        while i < len(lines):
            line = lines[i].strip()

            # Check if this line is a date
            date_match = re.match(
                r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s*(\d{4})$',
                line, re.IGNORECASE,
            )
            if date_match:
                month = month_map[date_match.group(1).lower()]
                day = date_match.group(2).zfill(2)
                year = date_match.group(3)
                current_date = f"{year}-{month}-{day}"
                i += 1
                continue

            # Skip noise lines
            skip_patterns = [
                r'^(pending|transactions?|showing|activity|date|description|amount|category|balance)',
                r'^(pay\s|see\s|chase\s|add|close|sign|log|out|security)',
                r'^(current|available|credit\s*limit|minimum|payment\s*due)',
                r'^(accounts?|plan|investments|benefits|explore)',
                r'^\$?[\d,]+\.?\d*$',
                r'^\d+$',
                r'^[\s>]+$',
            ]
            if any(re.match(p, line.lower()) for p in skip_patterns) or len(line) < 3:
                i += 1
                continue

            # This could be a merchant description — look ahead for amount
            description = line
            amount = None

            for j in range(1, min(4, len(lines) - i)):
                next_line = lines[i + j].strip() if i + j < len(lines) else ""
                amount_match = re.match(r'^(-?)\$\s*([\d,]+\.?\d*)$', next_line)
                if amount_match:
                    sign = amount_match.group(1)
                    amt_str = amount_match.group(2).replace(",", "")
                    try:
                        value = float(amt_str)
                        # Credit card: charges negative, payments/credits positive
                        if sign == "-":
                            amount = abs(value)
                        else:
                            amount = -abs(value)
                        if 0.01 <= abs(amount) <= 100000:
                            break
                        else:
                            amount = None
                    except ValueError:
                        pass

            if amount and current_date and description:
                clean_desc = description
                for noise in ['Pay over time eligible', 'Pending']:
                    clean_desc = clean_desc.replace(noise, '').strip()

                txn_key = f"{current_date}_{clean_desc}_{amount}"
                if txn_key not in seen and len(clean_desc) > 2:
                    seen.add(txn_key)
                    txn_id = hashlib.md5(f"chase_{txn_key}".encode()).hexdigest()[:16]

                    # Check pending context
                    is_pending = False
                    if i > 0:
                        prev = '\n'.join(lines[max(0, i - 5):i]).lower()
                        is_pending = 'pending' in prev and 'posted' not in prev

                    transactions.append({
                        "id": txn_id,
                        "account_id": account_id,
                        "date": current_date,
                        "description": clean_desc[:100],
                        "amount": amount,
                        "category": self._infer_category(clean_desc),
                        "status": "pending" if is_pending else "posted",
                    })

            i += 1

        return transactions

    def _parse_transaction_row(self, text: str, account_id: str) -> dict | None:
        """Parse a single transaction from a DOM row's text."""
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        if not lines:
            return None

        all_text = " ".join(lines)

        # Skip headers
        if any(x in all_text.upper() for x in ["DATE", "DESCRIPTION", "AMOUNT"]):
            if len(lines) <= 2:
                return None

        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        }

        # Parse date: "Mon DD, YYYY"
        date = None
        m = re.search(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s*(\d{4})',
            all_text, re.IGNORECASE,
        )
        if m:
            month = month_map[m.group(1).lower()]
            day = m.group(2).zfill(2)
            year = m.group(3)
            date = f"{year}-{month}-{day}"
        else:
            # Try MM/DD/YYYY
            m2 = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', all_text)
            if m2:
                parts = m2.group(1).split("/")
                if len(parts) == 3:
                    mo, dy, yr = parts
                    if len(yr) == 2:
                        yr = "20" + yr
                    date = f"{yr}-{mo.zfill(2)}-{dy.zfill(2)}"

        if not date:
            return None

        # Parse amount
        amount_match = re.search(r'(-?)\$\s*([\d,]+\.?\d*)', all_text)
        if not amount_match:
            return None
        try:
            sign = amount_match.group(1)
            value = float(amount_match.group(2).replace(",", ""))
            if sign == "-":
                amount = abs(value)
            else:
                amount = -abs(value)
            if not (0.01 <= abs(amount) <= 100000):
                return None
        except ValueError:
            return None

        # Description: remove date and amount from text
        desc = all_text
        desc = re.sub(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s*\d{4}',
            '', desc, flags=re.IGNORECASE,
        )
        desc = re.sub(r'\d{1,2}/\d{1,2}/\d{2,4}', '', desc)
        desc = re.sub(r'-?\$[\d,]+\.?\d*', '', desc)
        desc = re.sub(r'\s+', ' ', desc).strip()

        if not desc:
            return None

        txn_id = hashlib.md5(f"chase_{date}_{desc}_{amount}".encode()).hexdigest()[:16]
        return {
            "id": txn_id,
            "account_id": account_id,
            "date": date,
            "description": desc[:100],
            "amount": amount,
            "category": self._infer_category(desc),
            "status": "posted",
        }

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Normalize a date string to ISO YYYY-MM-DD format."""
        if not date_str:
            return ""

        # Already ISO-ish
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]

        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        }

        # "Mon DD, YYYY"
        m = re.search(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s*(\d{4})',
            date_str, re.IGNORECASE,
        )
        if m:
            month = month_map[m.group(1).lower()]
            day = m.group(2).zfill(2)
            return f"{m.group(3)}-{month}-{day}"

        # MM/DD/YYYY
        m2 = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', date_str)
        if m2:
            mo, dy, yr = m2.group(1), m2.group(2), m2.group(3)
            if len(yr) == 2:
                yr = "20" + yr
            return f"{yr}-{mo.zfill(2)}-{dy.zfill(2)}"

        return date_str[:10]

    @staticmethod
    def _infer_category(description: str) -> str:
        """Infer a transaction category from the description."""
        d = description.upper()
        if any(w in d for w in ['PAYMENT', 'AUTOPAY', 'THANK YOU']):
            return "Payment"
        if any(w in d for w in ['TRANSFER', 'XFER']):
            return "Transfer"
        if any(w in d for w in ['RESTAURANT', 'DOORDASH', 'UBER EATS', 'GRUBHUB', 'MCDONALD']):
            return "Dining"
        if any(w in d for w in ['AMAZON', 'WALMART', 'TARGET', 'COSTCO']):
            return "Shopping"
        if any(w in d for w in ['UBER', 'LYFT', 'PARKING', 'TOLL']):
            return "Transportation"
        if any(w in d for w in ['NETFLIX', 'SPOTIFY', 'HULU', 'DISNEY', 'APPLE.COM/BILL', 'SUBSCRIPTION']):
            return "Subscription"
        if any(w in d for w in ['GROCERY', 'SAFEWAY', 'WHOLE FOODS', 'TRADER JOE']):
            return "Groceries"
        if any(w in d for w in ['GAS', 'SHELL', 'CHEVRON', 'EXXON', 'BP ']):
            return "Gas"
        if any(w in d for w in ['AIRLINE', 'HOTEL', 'AIRBNB', 'BOOKING']):
            return "Travel"
        if any(w in d for w in ['INTEREST', 'FEE', 'CHARGE']):
            return "Fee"
        return "Other"
