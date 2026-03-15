"""Base scraper with shared browser setup and replay support."""

import os
import sys
from pathlib import Path
from playwright.async_api import async_playwright
from playwright_stealth import Stealth


class BaseScraper:
    """Base class for all till scrapers.

    Handles browser launch, stealth setup, and replay mode.
    Subclasses implement `extract(page)` for data extraction.
    """

    LOGIN_URL = ""

    def __init__(self, headless: bool = True):
        # Check env overrides
        if os.environ.get("TILL_HEADFUL") == "1":
            headless = False
        self.headless = headless
        self.pause = os.environ.get("TILL_PAUSE") == "1"
        self.save_html = os.environ.get("TILL_SAVE_HTML") == "1"
        self.test_mode = os.environ.get("TILL_TEST_MODE") == "1"
        self.replay_file = None

    def _get_user_data_dir(self) -> Path:
        """Get or create browser profile directory."""
        config_dir = Path.home() / ".config" / "till"
        user_data_dir = config_dir / "chromium-data"
        user_data_dir.mkdir(parents=True, exist_ok=True)
        return user_data_dir

    # Realistic Chrome user agent to avoid headless detection
    _USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )

    def _get_launch_args(self) -> list[str]:
        args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        ]
        if self.headless:
            # Extra flags to evade headless detection
            args.extend([
                "--disable-features=IsolateOrigins,site-per-process,HeadlessMode",
                "--window-size=1280,720",
            ])
        else:
            args.extend([
                "--start-minimized",
                "--window-position=0,10000",
                "--window-size=1280,720",
            ])
        return args

    async def scrape(self, username: str | None = None, password: str | None = None) -> dict:
        """Run the scraper. Handles browser setup and teardown."""
        # Get credentials from env if not passed directly
        if username is None:
            username = os.environ.get("TILL_USERNAME", "")
        if password is None:
            password = os.environ.get("TILL_PASSWORD", "")

        user_data_dir = self._get_user_data_dir()
        print(f"   Using session data: {user_data_dir}", file=sys.stderr)

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                str(user_data_dir),
                headless=self.headless,
                args=self._get_launch_args(),
                accept_downloads=True,
                bypass_csp=False,
                user_agent=self._USER_AGENT,
            )
            page = context.pages[0] if context.pages else await context.new_page()

            # Apply stealth
            stealth = Stealth()
            await stealth.apply_stealth_async(page)

            try:
                source_name = self.__class__.__name__.replace("Scraper", "").lower()

                if self.replay_file:
                    # Replay mode: load local HTML
                    print(f"   Replaying from: {self.replay_file}", file=sys.stderr)
                    html = Path(self.replay_file).read_text()
                    await page.set_content(html)
                else:
                    # Live mode: navigate and optionally login
                    await self.navigate_and_login(page, username, password)

                # Always save HTML + screenshot after login for offline debugging
                try:
                    html_path = f"/tmp/till_{source_name}_post_login.html"
                    content = await page.content()
                    Path(html_path).write_text(content)
                    await page.screenshot(path=f"/tmp/till_{source_name}_post_login.png")
                    print(f"   Saved post-login snapshot to {html_path}", file=sys.stderr)
                except Exception:
                    pass

                if self.pause:
                    print("   PAUSED — inspect the browser, then press Enter...", file=sys.stderr)
                    input()

                result = await self.extract(page)

                # Save post-extract snapshot too
                try:
                    html_path = f"/tmp/till_{source_name}_post_extract.html"
                    content = await page.content()
                    Path(html_path).write_text(content)
                    await page.screenshot(path=f"/tmp/till_{source_name}_post_extract.png")
                except Exception:
                    pass

                return result

            finally:
                try:
                    await page.close()
                except Exception:
                    pass
                try:
                    await context.close()
                except Exception:
                    pass

    async def navigate_and_login(self, page, username: str, password: str):
        """Navigate to LOGIN_URL and handle login if needed.

        Override this in subclasses for custom login flows.
        """
        if self.LOGIN_URL:
            print(f"   Navigating to {self.LOGIN_URL}", file=sys.stderr)
            await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=300000)
            await page.wait_for_timeout(3000)

    async def extract(self, page) -> dict:
        """Extract data from the page. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement extract()")
