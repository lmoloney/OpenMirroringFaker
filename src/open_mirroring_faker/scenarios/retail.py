"""Built-in retail demo scenario."""

from __future__ import annotations

from pathlib import Path

SCENARIO_PATH = Path(__file__).resolve().parent.parent.parent.parent / "scenarios" / "retail.yaml"

def get_scenario_path() -> Path:
    """Return the path to the built-in retail scenario YAML."""
    return SCENARIO_PATH
