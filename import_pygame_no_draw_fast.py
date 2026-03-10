"""Canonical headless entry point.

Delegates to the legacy script filename for backward compatibility.
"""

import runpy
from pathlib import Path

runpy.run_path(str(Path(__file__).with_name("import pygrame no draw fast.py")), run_name="__main__")
