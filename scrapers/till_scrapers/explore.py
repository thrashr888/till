"""Browser exploration tool for building scrapers.

Connects to an EXISTING Chrome session via CDP (Chrome DevTools Protocol)
to explore bank websites without triggering new logins.

Usage:
    1. Start Chrome with remote debugging:
       /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222

    2. Navigate to a bank website in Chrome and log in normally

    3. Run the explorer:
       uv run --directory scrapers python -m till_scrapers.explore --url "https://client.schwab.com/app/accounts/summary/"
       uv run --directory scrapers python -m till_scrapers.explore --url "https://secure.chase.com/web/auth/dashboard"

    The tool will:
    - Connect to your existing Chrome session
    - Capture ALL network API calls (JSON responses)
    - Save full HTML source
    - Take a screenshot
    - Dump everything to /tmp/till_explore_* for offline analysis

    No new logins, no risk of lockouts.
"""

import asyncio
import json
import re
import sys
from pathlib import Path
from playwright.async_api import async_playwright


async def explore(url: str, output_dir: str = "/tmp", label: str = ""):
    """Connect to existing Chrome and capture everything from a URL."""

    if not label:
        # Derive label from URL domain
        m = re.search(r'//([^/]+)', url)
        label = m.group(1).replace('.', '_') if m else "page"

    print(f"Connecting to Chrome on port 9222...", file=sys.stderr)

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        except Exception as e:
            print(f"\nERROR: Could not connect to Chrome.\n", file=sys.stderr)
            print(f"Start Chrome with remote debugging first:", file=sys.stderr)
            print(f'  /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222\n', file=sys.stderr)
            print(f"Or if Chrome is already running, restart it with the flag.", file=sys.stderr)
            sys.exit(1)

        # Get the first browser context (existing session with cookies)
        contexts = browser.contexts
        if not contexts:
            print("No browser contexts found. Open a tab in Chrome first.", file=sys.stderr)
            sys.exit(1)

        context = contexts[0]
        pages = context.pages

        # Find existing page matching the URL, or use the active tab
        target_page = None
        for page in pages:
            if url.split('//')[1].split('/')[0] in page.url:
                target_page = page
                print(f"Found existing tab: {page.url}", file=sys.stderr)
                break

        if not target_page:
            # Navigate the first page to the URL
            target_page = pages[0] if pages else await context.new_page()
            print(f"Navigating to {url}...", file=sys.stderr)
            await target_page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await target_page.wait_for_timeout(3000)

        # Set up API interception for future navigations
        api_responses = {}

        async def capture_api(response):
            resp_url = response.url
            if response.status == 200:
                try:
                    ct = response.headers.get('content-type', '')
                    if 'json' in ct:
                        body = await response.text()
                        if body and body.strip() and body.strip()[0] in '{[':
                            api_responses[resp_url] = json.loads(body)
                            print(f"  API: {resp_url[:120]}", file=sys.stderr)
                except Exception:
                    pass

        target_page.on("response", capture_api)

        # Capture current page state
        print(f"\nCapturing page: {target_page.url}", file=sys.stderr)

        # 1. Screenshot
        screenshot_path = f"{output_dir}/till_explore_{label}.png"
        await target_page.screenshot(path=screenshot_path, full_page=True)
        print(f"  Screenshot: {screenshot_path}", file=sys.stderr)

        # 2. HTML source
        html_path = f"{output_dir}/till_explore_{label}.html"
        content = await target_page.content()
        Path(html_path).write_text(content)
        print(f"  HTML: {html_path} ({len(content):,} chars)", file=sys.stderr)

        # 3. Extract all text content (for understanding page structure)
        text_path = f"{output_dir}/till_explore_{label}.txt"
        try:
            text = await target_page.inner_text("body")
            Path(text_path).write_text(text)
            print(f"  Text: {text_path} ({len(text):,} chars)", file=sys.stderr)
        except Exception:
            pass

        # 4. Extract page structure (headings, forms, tables, links)
        structure = await target_page.evaluate(r'''() => {
            const result = {
                title: document.title,
                url: window.location.href,
                headings: [],
                forms: [],
                tables: [],
                links: [],
                iframes: [],
                scripts_with_data: [],
            };

            // Headings
            document.querySelectorAll('h1, h2, h3, h4').forEach(h => {
                result.headings.push({tag: h.tagName, text: h.textContent.trim().substring(0, 100)});
            });

            // Forms
            document.querySelectorAll('form').forEach(f => {
                const inputs = [];
                f.querySelectorAll('input, select, textarea').forEach(i => {
                    inputs.push({
                        tag: i.tagName.toLowerCase(),
                        type: i.type || '',
                        id: i.id || '',
                        name: i.name || '',
                        placeholder: i.placeholder || '',
                    });
                });
                result.forms.push({action: f.action || '', method: f.method || '', inputs});
            });

            // Tables
            document.querySelectorAll('table').forEach(t => {
                const headers = [];
                t.querySelectorAll('th').forEach(th => headers.push(th.textContent.trim()));
                const rowCount = t.querySelectorAll('tbody tr').length;
                result.tables.push({headers, rowCount, id: t.id || '', className: t.className || ''});
            });

            // Iframes
            document.querySelectorAll('iframe').forEach(f => {
                result.iframes.push({src: f.src || '', id: f.id || '', title: f.title || ''});
            });

            // Script tags with inline JSON data
            document.querySelectorAll('script[type="text/template"], script[type="application/json"]').forEach(s => {
                const text = s.textContent.trim();
                if (text.length > 10 && text.length < 50000) {
                    result.scripts_with_data.push({
                        id: s.id || '',
                        type: s.type || '',
                        preview: text.substring(0, 200),
                    });
                }
            });

            // Links with interesting hrefs
            document.querySelectorAll('a[href]').forEach(a => {
                const href = a.href || '';
                if (href.includes('/api/') || href.includes('/svc/') || href.includes('activity') || href.includes('history') || href.includes('transaction')) {
                    result.links.push({text: a.textContent.trim().substring(0, 50), href});
                }
            });

            return result;
        }''')

        structure_path = f"{output_dir}/till_explore_{label}_structure.json"
        Path(structure_path).write_text(json.dumps(structure, indent=2))
        print(f"  Structure: {structure_path}", file=sys.stderr)

        # 5. Wait a moment to capture any lazy-loaded API calls
        print(f"\n  Waiting 5s for API calls...", file=sys.stderr)
        await target_page.wait_for_timeout(5000)

        # 6. Save captured API responses
        if api_responses:
            api_path = f"{output_dir}/till_explore_{label}_apis.json"
            Path(api_path).write_text(json.dumps(api_responses, indent=2, default=str))
            print(f"  APIs: {api_path} ({len(api_responses)} endpoints)", file=sys.stderr)

        # 7. Print summary
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"EXPLORATION SUMMARY: {label}", file=sys.stderr)
        print(f"{'='*60}", file=sys.stderr)
        print(f"  URL: {target_page.url}", file=sys.stderr)
        print(f"  Title: {structure.get('title', '')}", file=sys.stderr)
        print(f"  Headings: {len(structure.get('headings', []))}", file=sys.stderr)
        print(f"  Forms: {len(structure.get('forms', []))}", file=sys.stderr)
        print(f"  Tables: {len(structure.get('tables', []))}", file=sys.stderr)
        print(f"  Iframes: {len(structure.get('iframes', []))}", file=sys.stderr)
        print(f"  Inline data scripts: {len(structure.get('scripts_with_data', []))}", file=sys.stderr)
        print(f"  API calls captured: {len(api_responses)}", file=sys.stderr)
        print(f"  Interesting links: {len(structure.get('links', []))}", file=sys.stderr)

        # Print table info (most useful for scraping)
        for t in structure.get('tables', []):
            print(f"\n  Table: id={t.get('id','')}, {t.get('rowCount',0)} rows", file=sys.stderr)
            if t.get('headers'):
                print(f"    Headers: {t['headers']}", file=sys.stderr)

        # Print API endpoints
        if api_responses:
            print(f"\n  API endpoints:", file=sys.stderr)
            for url in sorted(api_responses.keys()):
                data = api_responses[url]
                if isinstance(data, dict):
                    keys = list(data.keys())[:5]
                    print(f"    {url[:100]}", file=sys.stderr)
                    print(f"      keys: {keys}", file=sys.stderr)

        print(f"\nFiles saved to {output_dir}/till_explore_{label}_*", file=sys.stderr)

        # Don't close the browser — it's the user's Chrome session!
        browser.close()


async def explore_clicks(url: str, output_dir: str = "/tmp", label: str = ""):
    """Connect to Chrome, navigate through multiple pages, capturing everything."""

    if not label:
        m = re.search(r'//([^/]+)', url)
        label = m.group(1).replace('.', '_') if m else "page"

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp("http://localhost:9222")
        except Exception:
            print("ERROR: Start Chrome with --remote-debugging-port=9222", file=sys.stderr)
            sys.exit(1)

        context = browser.contexts[0]
        pages = context.pages

        # Find or navigate to target
        target_page = None
        for page in pages:
            if url.split('//')[1].split('/')[0] in page.url:
                target_page = page
                break

        if not target_page:
            target_page = pages[0] if pages else await context.new_page()
            await target_page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await target_page.wait_for_timeout(3000)

        # Capture API calls across all navigations
        all_api_responses = {}
        page_snapshots = []

        async def capture_api(response):
            if response.status == 200:
                try:
                    ct = response.headers.get('content-type', '')
                    if 'json' in ct:
                        body = await response.text()
                        if body and body.strip() and body.strip()[0] in '{[':
                            all_api_responses[response.url] = json.loads(body)
                except Exception:
                    pass

        target_page.on("response", capture_api)

        # Capture initial page
        snapshot = {
            "url": target_page.url,
            "html": await target_page.content(),
            "text": "",
        }
        try:
            snapshot["text"] = await target_page.inner_text("body")
        except Exception:
            pass
        await target_page.screenshot(path=f"{output_dir}/till_explore_{label}_0.png")
        page_snapshots.append(snapshot)

        # Interactive: let the user navigate in Chrome, capture each page
        print(f"\nExploration mode active. Navigate in Chrome and press Enter here to capture each page.", file=sys.stderr)
        print(f"Type 'done' to finish.\n", file=sys.stderr)

        step = 1
        while True:
            try:
                cmd = input(f"[{step}] Press Enter to capture (or 'done'): ").strip()
            except EOFError:
                break
            if cmd.lower() == 'done':
                break

            snapshot = {
                "url": target_page.url,
                "html": await target_page.content(),
                "text": "",
            }
            try:
                snapshot["text"] = await target_page.inner_text("body")
            except Exception:
                pass
            await target_page.screenshot(path=f"{output_dir}/till_explore_{label}_{step}.png")
            page_snapshots.append(snapshot)

            print(f"  Captured: {target_page.url}", file=sys.stderr)
            print(f"  APIs so far: {len(all_api_responses)}", file=sys.stderr)
            step += 1

        # Save everything
        for i, snap in enumerate(page_snapshots):
            Path(f"{output_dir}/till_explore_{label}_{i}.html").write_text(snap["html"])
            if snap["text"]:
                Path(f"{output_dir}/till_explore_{label}_{i}.txt").write_text(snap["text"])

        if all_api_responses:
            Path(f"{output_dir}/till_explore_{label}_all_apis.json").write_text(
                json.dumps(all_api_responses, indent=2, default=str)
            )

        print(f"\nCaptured {len(page_snapshots)} pages, {len(all_api_responses)} API calls", file=sys.stderr)
        browser.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Explore bank websites via existing Chrome session")
    parser.add_argument("--url", required=True, help="URL to explore")
    parser.add_argument("--output", default="/tmp", help="Output directory")
    parser.add_argument("--label", default="", help="Label for output files")
    parser.add_argument("--interactive", action="store_true", help="Interactive multi-page capture mode")
    args = parser.parse_args()

    if args.interactive:
        asyncio.run(explore_clicks(args.url, args.output, args.label))
    else:
        asyncio.run(explore(args.url, args.output, args.label))


if __name__ == "__main__":
    main()
