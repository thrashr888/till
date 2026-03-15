"""Chase credit card scraper.

Strategy: Login via Playwright, then use direct API calls to Chase's internal
endpoints (/svc/rr/accounts/...) with session cookies. Falls back to API
interception and DOM extraction if direct calls fail.

Supports multiple credit cards: after fetching the first card from card/list,
checks singleCreditCardUser flag and switches accounts to fetch additional cards.

Chase aggressively detects headless browsers, so headful mode is always forced.
"""

import re
import sys
import hashlib
import json
import os
from pathlib import Path
from till_scrapers.base import BaseScraper


def _save_html(page_content: str, stage: str):
    """Save HTML snapshot for offline debugging."""
    try:
        path = f"/tmp/till_chase_{stage}.html"
        Path(path).write_text(page_content)
        print(f"   Saved HTML snapshot: {path}", file=sys.stderr)
    except Exception as e:
        print(f"   Failed to save HTML snapshot ({stage}): {e}", file=sys.stderr)


class ChaseScraper(BaseScraper):
    LOGIN_URL = "https://secure.chase.com/web/auth/dashboard"

    # Known Chase API endpoints
    CARD_LIST_API = (
        "https://secure.chase.com/svc/rr/accounts/secure/gateway/"
        "card/list"
    )
    TXN_API_BASE = (
        "https://secure.chase.com/svc/rr/accounts/secure/gateway/"
        "credit-card/transactions/inquiry-maintenance/etu-transactions/"
        "v4/accounts/transactions"
    )

    def __init__(self, headless: bool = True):
        # Chase always requires headful -- it aggressively detects headless
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
        print("   Checking for active session...", file=sys.stderr)
        try:
            await page.goto(
                self.LOGIN_URL,
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"   Navigation error at {self.LOGIN_URL}: {e}", file=sys.stderr)
            raise

        _save_html(await page.content(), "session_check")

        needs_login = await self._needs_login(page)

        if not needs_login:
            print("   Session active, skipping login", file=sys.stderr)
            return

        print("   Session expired, logging in...", file=sys.stderr)

        if not (username and password):
            raise Exception(
                "No credentials found. Use `till creds set --source chase` "
                "or set TILL_USERNAME/TILL_PASSWORD env vars."
            )

        await self._do_login(page, username, password)
        _save_html(await page.content(), "post_login")
        await page.wait_for_timeout(2000)

    async def _needs_login(self, page) -> bool:
        """Check if we're on a login page."""
        await page.wait_for_timeout(1000)
        url = page.url

        if any(x in url.lower() for x in ["logon", "login", "signin"]):
            return True

        iframe = await page.query_selector('iframe#logonbox')
        if iframe:
            return True

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
            iframe_handle = await page.query_selector('iframe#logonbox')
            login_frame = page

            if iframe_handle:
                print("   Found login iframe, switching context...", file=sys.stderr)
                login_frame = await iframe_handle.content_frame()
                if not login_frame:
                    raise Exception(
                        "Could not access login iframe content. "
                        "Run `till test --source chase --headful --pause` to re-auth."
                    )

            print("   Waiting for login form...", file=sys.stderr)
            try:
                await login_frame.wait_for_selector(
                    '#userId-text-input-field, #userId-input-field-input, input[name="userId"]',
                    timeout=30000,
                )
            except Exception:
                await page.screenshot(path="/tmp/till_chase_login_form_wait.png")
                _save_html(await page.content(), "login_form_missing")
                raise Exception(
                    f"Login form not found at {page.url}. "
                    "Run `till test --source chase --headful --pause` to re-auth."
                )

            # Fill username
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
            if password_field:
                try:
                    await password_field.press("Enter")
                    print("   Pressed Enter to submit", file=sys.stderr)
                    submitted = True
                except Exception:
                    pass

            if not submitted:
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
                    print("   2FA required -- approve on Chase mobile app...", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_chase_2fa.png")
                    _save_html(await page.content(), "2fa_required")
                    raise Exception(
                        f"2FA required at {current_url}. "
                        "Run `till test --source chase --headful --pause` to approve."
                    )
                else:
                    print(f"   Post-login URL: {current_url}", file=sys.stderr)
                    _save_html(await page.content(), "login_unknown_state")
                    raise Exception(
                        f"Login may have failed at {current_url}. "
                        "Run `till test --source chase --headful --pause` to re-auth."
                    )

        except Exception as e:
            err_msg = str(e)
            if "Frame was detached" in err_msg or "frame was detached" in err_msg:
                print("   Login iframe detached (normal after submit), continuing...", file=sys.stderr)
                await page.wait_for_timeout(2000)
                return
            print(f"   Auto-login failed: {e}", file=sys.stderr)
            await page.screenshot(path="/tmp/till_chase_login_error.png")
            raise

    async def extract(self, page) -> dict:
        """Extract accounts and transactions using direct API calls + fallbacks."""

        # Step 1: Set up API interception as fallback
        api_responses = {}

        async def capture_api(response):
            url = response.url
            if '/svc/' in url or '/api/' in url:
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

        # Step 2: Navigate to dashboard if not already there
        print("   Loading dashboard...", file=sys.stderr)
        current_url = page.url
        if "dashboard" not in current_url.lower():
            await page.goto(
                "https://secure.chase.com/web/auth/dashboard",
                wait_until="domcontentloaded",
                timeout=30000,
            )
        await page.wait_for_timeout(3000)

        await page.screenshot(path="/tmp/till_chase_dashboard.png")
        _save_html(await page.content(), "dashboard")

        # Step 3: Try direct API call for card/list FIRST
        accounts = await self._fetch_accounts_direct(page)

        # Fallback to intercepted API responses
        if not accounts:
            print("   Direct API didn't return accounts, checking intercepted responses...", file=sys.stderr)
            for url, data in api_responses.items():
                if any(k in url.lower() for k in ['account', 'card', 'credit']):
                    api_accounts = self._parse_accounts_from_api(data)
                    if api_accounts:
                        accounts.extend(api_accounts)

        # Fallback to DOM extraction
        if not accounts:
            print("   No accounts from API, falling back to DOM...", file=sys.stderr)
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

        # Step 4: Extract transactions for each account
        transactions = []
        for acct in accounts:
            api_responses.clear()
            acct_txns = await self._extract_transactions(page, acct, api_responses)
            transactions.extend(acct_txns)

        print(f"   Found {len(transactions)} transactions", file=sys.stderr)

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

    # -- Direct API calls -------------------------------------------------

    async def _fetch_accounts_direct(self, page) -> list[dict]:
        """Fetch accounts via direct API calls to Chase's card/list endpoint.

        The card/list API returns one card at a time based on the selected account.
        If singleCreditCardUser is false, we switch accounts and fetch additional cards.
        """
        accounts = []

        print("   Trying direct card/list API...", file=sys.stderr)
        try:
            response = await page.request.get(self.CARD_LIST_API, timeout=15000)
            if not response.ok:
                print(f"   Card/list API returned {response.status}", file=sys.stderr)
                return accounts

            data = await response.json()

            # Save for debugging
            try:
                with open("/tmp/till_chase_card_list.json", 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except Exception:
                pass

            card = self._parse_chase_card(data)
            if card:
                accounts.append(card)
                print(f"   Direct API: {card['name']} ...{card['last4']}", file=sys.stderr)

                # Check if user has multiple credit cards
                is_single = data.get('singleCreditCardUser', True)
                if not is_single:
                    print("   Multi-card user detected, fetching additional cards...", file=sys.stderr)
                    extra = await self._fetch_additional_cards(page, data, card)
                    accounts.extend(extra)
            else:
                # Try parsing as a list or nested structure
                api_accounts = self._parse_accounts_from_api(data)
                if api_accounts:
                    accounts.extend(api_accounts)

        except Exception as e:
            print(f"   Direct card/list API failed: {e}", file=sys.stderr)

        return accounts

    async def _fetch_additional_cards(self, page, first_response: dict, first_card: dict) -> list[dict]:
        """Fetch additional credit cards by switching the selected account.

        Chase's card/list API returns one card at a time. The response includes
        account selector data that lists all available accounts.
        """
        additional = []
        seen_ids = {first_card.get("chase_account_id", "")}

        # Look for account list in the response
        account_list = (
            first_response.get('accountSelectorData', {}).get('accounts', [])
            or first_response.get('accounts', [])
        )

        if not account_list:
            # Try to find accounts in the dashboard page itself
            print("   No account list in API response, trying dashboard navigation...", file=sys.stderr)
            return additional

        for acct_entry in account_list:
            acct_id = str(acct_entry.get('accountId', '') or acct_entry.get('digitalAccountIdentifier', ''))
            if not acct_id or acct_id in seen_ids:
                continue
            seen_ids.add(acct_id)

            try:
                # Fetch card/list for this specific account
                url = f"{self.CARD_LIST_API}?digital-account-identifier={acct_id}"
                response = await page.request.get(url, timeout=15000)
                if not response.ok:
                    continue

                data = await response.json()
                card = self._parse_chase_card(data)
                if card and card.get("chase_account_id") not in seen_ids:
                    additional.append(card)
                    seen_ids.add(card.get("chase_account_id", ""))
                    print(f"   Additional card: {card['name']} ...{card['last4']}", file=sys.stderr)

            except Exception as e:
                print(f"   Failed to fetch card for account {acct_id}: {e}", file=sys.stderr)

        return additional

    # -- API parsing ------------------------------------------------------

    def _parse_accounts_from_api(self, data) -> list[dict]:
        """Parse accounts from Chase's internal API response."""
        accounts = []

        if isinstance(data, dict):
            if 'nickname' in data and 'mask' in data:
                acct = self._parse_chase_card(data)
                if acct:
                    accounts.append(acct)
                return accounts

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
        """Parse transactions from Chase's internal API response."""
        transactions = []

        items = data.get('activities', []) if isinstance(data, dict) else []
        if not items:
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
                amount = -abs(amount)
            else:
                amount = abs(amount)

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
        """Map Chase category group to standard category."""
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

    # -- DOM extraction (fallback) ----------------------------------------

    async def _extract_accounts_dom(self, page) -> list[dict]:
        """Extract credit card accounts from the dashboard via DOM/text parsing."""
        accounts = []

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

        if not accounts:
            print(
                f"   No accounts found via DOM. URL: {page.url}. "
                "Run `till test --source chase --headful --pause` to re-auth.",
                file=sys.stderr,
            )

        return accounts

    # -- Transaction extraction -------------------------------------------

    async def _extract_transactions(self, page, account: dict, api_responses: dict) -> list[dict]:
        """Extract transactions using direct API calls with session cookies."""
        transactions = []
        account_id = account["id"]
        account_name = account.get("name", "Chase Card")
        chase_acct_id = account.get("chase_account_id", "")

        if not chase_acct_id:
            print(
                f"   No Chase account ID for {account_name}, skipping transactions. "
                "This card was likely found via DOM only.",
                file=sys.stderr,
            )
            return transactions

        print(f"   Fetching transactions for {account_name}...", file=sys.stderr)
        try:
            url = (
                f"{self.TXN_API_BASE}?digital-account-identifier={chase_acct_id}"
                f"&provide-available-statement-indicator=true"
                f"&record-count=200&sort-order-code=D&sort-key-code=T"
            )
            response = await page.request.get(url, timeout=30000)
            if not response.ok:
                print(
                    f"   Transaction API returned {response.status} for {account_name}. "
                    f"URL: {url[:120]}",
                    file=sys.stderr,
                )
                return transactions

            data = await response.json()
            txns = self._parse_transactions_from_api(data, account_id)
            transactions.extend(txns)
            print(f"   Current cycle: {len(txns)} transactions for {account_name}", file=sys.stderr)

            try:
                with open(f"/tmp/till_chase_{account_id}_txns.json", 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except Exception:
                pass

        except Exception as e:
            print(
                f"   Transaction API failed for {account_name}: {e}. "
                "Run `till test --source chase --headful --pause` to re-auth.",
                file=sys.stderr,
            )

        # Check intercepted responses as fallback
        if not transactions:
            for url, data in api_responses.items():
                if 'transaction' in url.lower():
                    txns = self._parse_transactions_from_api(data, account_id)
                    transactions.extend(txns)

        print(f"   Total {len(transactions)} transactions for {account_name}", file=sys.stderr)
        return transactions

    # -- Helpers ----------------------------------------------------------

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Normalize a date string to ISO YYYY-MM-DD format."""
        if not date_str:
            return ""

        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]

        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        }

        m = re.search(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s*(\d{4})',
            date_str, re.IGNORECASE,
        )
        if m:
            month = month_map[m.group(1).lower()]
            day = m.group(2).zfill(2)
            return f"{m.group(3)}-{month}-{day}"

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
