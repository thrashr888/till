"""Plugin discovery from manifest.json files."""

import json
import importlib
from pathlib import Path


def discover_plugins() -> dict:
    """Discover all plugins by scanning for manifest.json files."""
    plugins = {}
    package_dir = Path(__file__).parent

    for manifest_path in package_dir.glob("*/manifest.json"):
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
            name = manifest["name"]
            plugins[name] = {
                "manifest": manifest,
                "path": manifest_path.parent,
            }
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: invalid manifest at {manifest_path}: {e}", flush=True)

    return plugins


def list_sources() -> list[str]:
    """Return sorted list of available source names."""
    return sorted(discover_plugins().keys())


def load_scraper(name: str):
    """Load and instantiate a scraper by source name."""
    plugins = discover_plugins()
    if name not in plugins:
        raise ValueError(f"Unknown source: {name}. Available: {', '.join(list_sources())}")

    # Import the scraper module dynamically
    module = importlib.import_module(f"till_scrapers.{name}.scraper")

    # Find the scraper class (convention: <Name>Scraper)
    class_name = f"{name.capitalize()}Scraper"
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if isinstance(obj, type) and attr_name.endswith("Scraper") and attr_name != "BaseScraper":
            return obj
    raise ValueError(f"No scraper class found in till_scrapers.{name}.scraper")
