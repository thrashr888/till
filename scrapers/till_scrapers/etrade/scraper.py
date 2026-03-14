"""E*Trade scraper — migrated from Argos."""

import re
import sys
import hashlib
from till_scrapers.base import BaseScraper


class EtradeScraper(BaseScraper):
    LOGIN_URL = "https://us.etrade.com/etx/hw/v2/accountshome"

    async def navigate_and_login(self, page, username: str, password: str):
        print(f"   Navigating to {self.LOGIN_URL}", file=sys.stderr)
        await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=300000)

        current_url = page.url
        if "login" in current_url.lower() or "logon" in current_url.lower():
            if username and password:
                print("   Auto-filling login credentials...", file=sys.stderr)
                try:
                    await page.wait_for_timeout(2000)
                    await page.wait_for_selector("#USER", timeout=5000)
                    await page.fill("#USER", username)
                    await page.wait_for_timeout(500)
                    await page.wait_for_selector("#password", timeout=5000)
                    await page.fill("#password", password)
                    await page.wait_for_timeout(500)
                    await page.wait_for_selector("#mfaLogonButton", timeout=5000)
                    await page.click("#mfaLogonButton")
                    print("   Waiting for login...", file=sys.stderr)
                    await page.wait_for_timeout(5000)
                except Exception as e:
                    print(f"   Auto-login failed: {e}", file=sys.stderr)
                    await page.screenshot(path="/tmp/till_etrade_login.png")
                    raise
            else:
                raise Exception("No credentials found. Set via `till creds set --source etrade`")

        await page.wait_for_timeout(3000)

    async def extract(self, page) -> dict:
        accounts = []
        positions = []

        await page.screenshot(path="/tmp/till_etrade_accounts.png")
        print("   Screenshot: /tmp/till_etrade_accounts.png", file=sys.stderr)

        accounts = await self._extract_accounts(page)
        print(f"   Found {len(accounts)} accounts", file=sys.stderr)

        # Extract positions
        print("   Navigating to positions page...", file=sys.stderr)
        positions_url = "https://us.etrade.com/etx/pxy/portfolios/positions?_formtarget=portfoliolist"
        await page.goto(positions_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)

        target_account = accounts[0] if accounts else None
        if target_account:
            account_id = hashlib.md5(target_account["name"].encode()).hexdigest()[:16]
            positions = await self._extract_positions(page, account_id, target_account["name"])
            print(f"   Found {len(positions)} positions", file=sys.stderr)

        # Build results
        account_results = []
        for acct in accounts:
            account_id = hashlib.md5(acct["name"].encode()).hexdigest()[:16]
            account_results.append({
                "account_id": account_id,
                "account_name": acct["name"],
                "account_type": "brokerage",
                "balance": acct["balance"],
                "day_change": acct.get("day_change"),
                "day_change_percent": acct.get("day_change_percent"),
            })

        position_results = []
        for pos in positions:
            position_results.append({
                "position_id": pos["position_id"],
                "account_id": pos["account_id"],
                "symbol": pos["symbol"],
                "description": pos.get("description"),
                "quantity": pos.get("quantity"),
                "last_price": pos.get("last_price"),
                "market_value": pos["market_value"],
                "day_gain": pos.get("days_gain"),
                "total_gain": pos.get("total_gain"),
                "total_gain_percent": pos.get("total_gain_percent"),
            })

        return {
            "status": "ok",
            "source": "etrade",
            "accounts": account_results,
            "transactions": [],
            "positions": position_results,
            "balance_history": [],
        }

    async def _extract_accounts(self, page) -> list[dict]:
        """Extract E*Trade accounts."""
        accounts = []

        selectors = [
            'div[class*="Account---account"]',
            'div[class*="AccountCardView"]',
            'div[class*="account-card"]',
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
                if not lines:
                    continue

                name = lines[0]

                # Look for account suffix
                for line in lines:
                    match = re.search(r'(?:ending with|ending in|\.{3}|-)(\d{4})\b', line, re.IGNORECASE)
                    if match and match.group(1) not in name:
                        name = f"{name} - {match.group(1)}"
                        break

                net_value = None
                day_gain = None
                day_gain_percent = None

                for i, line in enumerate(lines):
                    line_lower = line.lower()
                    next_line = lines[i + 1] if i + 1 < len(lines) else ""

                    if 'net account value' in line_lower:
                        net_value = self._parse_dollar(line) or self._parse_dollar(next_line)
                    elif "day's gain" in line_lower or "days gain" in line_lower:
                        day_gain = self._parse_dollar(line) or self._parse_dollar(next_line)
                        day_gain_percent = self._parse_percent(line) or self._parse_percent(next_line)

                # Fallback: first dollar amount
                if net_value is None:
                    for line in lines:
                        if "$" in line:
                            net_value = self._parse_dollar(line)
                            if net_value:
                                break

                if net_value and net_value > 0:
                    accounts.append({
                        "name": name,
                        "balance": net_value,
                        "day_change": day_gain,
                        "day_change_percent": day_gain_percent,
                    })

            except Exception as e:
                print(f"   Error parsing account: {e}", file=sys.stderr)

        return accounts

    async def _extract_positions(self, page, account_id: str, account_name: str) -> list[dict]:
        """Extract positions from the portfolio page."""
        await page.screenshot(path="/tmp/till_etrade_positions.png")
        content = await page.inner_text("body")
        return self._parse_positions_from_text(content, account_id, account_name)

    def _parse_positions_from_text(self, content: str, account_id: str, account_name: str) -> list[dict]:
        positions = []
        lines = content.split('\n')

        for i, line in enumerate(lines):
            line = line.strip()
            symbol_match = re.match(r'^([A-Z]{1,5})(?:\s|$|\()', line)
            if not symbol_match:
                continue

            symbol = symbol_match.group(1)
            skip = {"CASH", "TOTAL", "DAY", "GAIN", "LOSS", "BUY", "SELL", "TYPE",
                     "LAST", "PRICE", "QTY", "VALUE", "TRADE", "ACTIONS", "SYMBOL", "NET",
                     "IRA", "ROTH", "SEP", "HSA", "PLAN", "ESPP", "RSU", "TRAD"}
            if symbol in skip:
                continue

            context = "\n".join(lines[i:min(len(lines), i + 15)])
            symbol_pos = context.find(symbol)
            after = context[symbol_pos + len(symbol):] if symbol_pos >= 0 else context

            all_numbers = re.findall(r'([\d,]+\.?\d*)', after)
            all_numbers = [float(n.replace(',', '')) for n in all_numbers if n and float(n.replace(',', '')) > 0]
            all_percents = [float(p) for p in re.findall(r'([\d.]+)%', after) if p]

            if len(all_numbers) < 3:
                continue

            market_value = None
            last_price = None
            quantity = None

            # Try positional parsing
            percent_set = set(all_percents)
            main_numbers = [n for n in all_numbers if n not in percent_set]

            if len(main_numbers) >= 7:
                last_price = main_numbers[0] if main_numbers[0] < 5000 else None
                quantity = main_numbers[2] if len(main_numbers) > 2 else None
                market_value = main_numbers[6] if len(main_numbers) > 6 else None
            else:
                large = [n for n in all_numbers if n > 10000]
                if large:
                    market_value = max(large)
                prices = [n for n in all_numbers if 10 < n < 2000]
                if prices:
                    last_price = prices[0]
                if market_value and last_price and last_price > 0:
                    quantity = market_value / last_price

            if market_value and market_value > 100:
                position_id = hashlib.md5(f"{account_id}_{symbol}".encode()).hexdigest()[:16]
                positions.append({
                    "position_id": position_id,
                    "account_id": account_id,
                    "account_name": account_name,
                    "symbol": symbol,
                    "quantity": quantity,
                    "last_price": last_price,
                    "market_value": market_value,
                    "total_gain_percent": all_percents[-1] if all_percents else None,
                })

        # Cash position
        for j, line in enumerate(lines):
            if line.strip().lower() in ("cash", "cash "):
                for k in range(j, min(j + 5, len(lines))):
                    m = re.search(r'\$?([\d,]+\.?\d*)', lines[k])
                    if m:
                        val = float(m.group(1).replace(',', ''))
                        if val > 0:
                            positions.append({
                                "position_id": hashlib.md5(f"{account_id}_CASH".encode()).hexdigest()[:16],
                                "account_id": account_id,
                                "account_name": account_name,
                                "symbol": "CASH",
                                "description": "Cash & Sweep Vehicle",
                                "quantity": None,
                                "last_price": None,
                                "market_value": val,
                                "days_gain": 0,
                                "total_gain": 0,
                                "total_gain_percent": 0,
                            })
                            break
                break

        return positions

    @staticmethod
    def _parse_dollar(text: str):
        match = re.search(r'([+-])?\$?([\d,]+\.?\d*)', text)
        if match:
            sign = match.group(1) or ''
            amount = float(match.group(2).replace(',', ''))
            return -amount if sign == '-' else amount
        return None

    @staticmethod
    def _parse_percent(text: str):
        match = re.search(r'([+-]?\d+\.?\d*)%', text)
        return float(match.group(1)) if match else None
