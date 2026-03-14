"""CLI entry point for till scrapers.

Usage:
    till-scrape --list                    # List available sources
    till-scrape --source schwab           # Run schwab scraper
    till-scrape --source schwab --replay /tmp/till_schwab_page.html
"""

import sys
import json
import asyncio
import argparse

# Redirect prints to stderr so stdout is clean JSON
_real_stdout = sys.stdout
sys.stdout = sys.stderr


def main():
    parser = argparse.ArgumentParser(description="Till finance scraper runner")
    parser.add_argument("--list", action="store_true", help="List available sources")
    parser.add_argument("--source", help="Source to scrape")
    parser.add_argument("--replay", help="Replay HTML file instead of live scraping")
    parser.add_argument("--config", help="Path to config file")
    args = parser.parse_args()

    if args.list:
        from till_scrapers.registry import list_sources
        for source in list_sources():
            _real_stdout.write(source + "\n")
        _real_stdout.flush()
        return

    if not args.source:
        parser.error("--source is required (or use --list)")

    asyncio.run(async_main(args))


async def async_main(args):
    from till_scrapers.registry import load_scraper

    scraper_class = load_scraper(args.source)
    scraper = scraper_class()

    if args.replay:
        scraper.replay_file = args.replay

    try:
        result = await scraper.scrape()
        if "source" not in result:
            result["source"] = args.source
        if "status" not in result:
            result["status"] = "ok"
    except Exception as e:
        result = {
            "status": "error",
            "source": args.source,
            "error": str(e),
            "accounts": [],
            "transactions": [],
            "positions": [],
            "balance_history": [],
        }

    _real_stdout.write(json.dumps(result, default=str) + "\n")
    _real_stdout.flush()


if __name__ == "__main__":
    main()
