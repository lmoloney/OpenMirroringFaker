"""Built-in HR demo scenario."""

from __future__ import annotations

from pathlib import Path

SCENARIO_PATH = Path(__file__).resolve().parent.parent.parent.parent / "scenarios" / "hr.yaml"


def get_scenario_path() -> Path:
    """Return the path to the built-in HR scenario YAML."""
    return SCENARIO_PATH
