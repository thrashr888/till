"""Interactive test harness for developing scrapers."""

import os


def setup_test_env(source: str, headful: bool = False, pause: bool = False, save_html: bool = False):
    """Configure environment variables for test mode."""
    os.environ["TILL_TEST_MODE"] = "1"
    if headful:
        os.environ["TILL_HEADFUL"] = "1"
    if pause:
        os.environ["TILL_PAUSE"] = "1"
    if save_html:
        os.environ["TILL_SAVE_HTML"] = "1"
