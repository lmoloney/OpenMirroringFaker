"""Scenario and application configuration loader.

Parses scenario YAML files that define table schemas and data-generation
settings, and reads OneLake connection info from environment variables.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# ── Valid enumerations ──────────────────────────────────────────────

VALID_COLUMN_TYPES: frozenset[str] = frozenset({"int", "float", "string", "datetime", "boolean"})

VALID_GENERATORS: frozenset[str] = frozenset(
    {
        "sequence",
        "random_int",
        "random_float",
        "first_name",
        "last_name",
        "email",
        "city",
        "now",
        "date_this_year",
        "choice",
        "uuid",
        "boolean",
        "company",
        "address",
        "phone_number",
        "text",
        "sentence",
    }
)

VALID_MODES: frozenset[str] = frozenset({"continuous", "batch"})


# ── Dataclasses ─────────────────────────────────────────────────────


@dataclass
class ColumnDef:
    """Definition of a single table column and its data generator."""

    name: str
    type: str  # "int", "float", "string", "datetime", "boolean"
    generator: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableSchema:
    """Schema for one table within a scenario."""

    name: str
    schema_name: str  # e.g. "dbo"
    key_columns: list[str]
    columns: list[ColumnDef]


@dataclass
class GenerationConfig:
    """Controls how synthetic data is generated."""

    mode: str = "continuous"
    batch_size: int = 50
    interval_seconds: float = 5.0
    total_batches: int | None = None
    operations: dict[str, float] = field(default_factory=lambda: {"insert": 0.7, "update": 0.2, "delete": 0.1})
    seed: int | None = None


@dataclass
class ScenarioConfig:
    """Top-level scenario definition loaded from YAML."""

    name: str
    partner_name: str
    source_type: str
    tables: list[TableSchema]
    generation: GenerationConfig


@dataclass
class AppConfig:
    """Application-level config loaded from environment variables."""

    workspace_id: str
    mirrored_db_id: str
    log_level: str = "INFO"


# ── Internal helpers ────────────────────────────────────────────────

_DEFAULT_OPERATIONS: dict[str, float] = {"insert": 0.7, "update": 0.2, "delete": 0.1}


def _require(data: dict[str, Any], key: str, context: str) -> Any:
    """Return *data[key]* or raise ``ValueError`` with a helpful message."""
    if key not in data:
        raise ValueError(f"Missing required field '{key}' in {context}")
    return data[key]


def _parse_column(raw: dict[str, Any], table_name: str) -> ColumnDef:
    name = _require(raw, "name", f"column of table '{table_name}'")
    col_type = _require(raw, "type", f"column '{name}' of table '{table_name}'")
    generator = _require(raw, "generator", f"column '{name}' of table '{table_name}'")

    if col_type not in VALID_COLUMN_TYPES:
        raise ValueError(
            f"Invalid column type '{col_type}' for column '{name}' in table "
            f"'{table_name}'. Valid types: {sorted(VALID_COLUMN_TYPES)}"
        )

    if generator not in VALID_GENERATORS:
        raise ValueError(
            f"Unknown generator '{generator}' for column '{name}' in table "
            f"'{table_name}'. Valid generators: {sorted(VALID_GENERATORS)}"
        )

    return ColumnDef(
        name=name,
        type=col_type,
        generator=generator,
        args=raw.get("args", {}),
    )


def _parse_table(raw: dict[str, Any]) -> TableSchema:
    name = _require(raw, "name", "table definition")
    schema_name = raw.get("schema", "dbo")
    key_columns = _require(raw, "key_columns", f"table '{name}'")

    if not isinstance(key_columns, list) or not key_columns:
        raise ValueError(f"'key_columns' for table '{name}' must be a non-empty list")

    raw_columns = _require(raw, "columns", f"table '{name}'")
    if not isinstance(raw_columns, list) or not raw_columns:
        raise ValueError(f"'columns' for table '{name}' must be a non-empty list")

    columns = [_parse_column(c, name) for c in raw_columns]

    col_names = {c.name for c in columns}
    for kc in key_columns:
        if kc not in col_names:
            raise ValueError(f"Key column '{kc}' in table '{name}' does not appear in the column list")

    return TableSchema(
        name=name,
        schema_name=schema_name,
        key_columns=key_columns,
        columns=columns,
    )


def _parse_generation(raw: dict[str, Any] | None) -> GenerationConfig:
    if raw is None:
        logger.info("No 'generation' section in scenario — using defaults")
        return GenerationConfig()

    mode = raw.get("mode", "continuous")
    if mode not in VALID_MODES:
        raise ValueError(f"Invalid generation mode '{mode}'. Valid modes: {sorted(VALID_MODES)}")

    ops = raw.get("operations")
    if ops is not None:
        if not isinstance(ops, dict):
            raise ValueError("'operations' must be a mapping of operation → weight")
        for op_name in ops:
            if op_name not in {"insert", "update", "delete"}:
                raise ValueError(
                    f"Unknown operation '{op_name}' in generation.operations. Valid operations: insert, update, delete"
                )

    return GenerationConfig(
        mode=mode,
        batch_size=raw.get("batch_size", 50),
        interval_seconds=float(raw.get("interval_seconds", 5.0)),
        total_batches=raw.get("total_batches"),
        operations=ops if ops is not None else dict(_DEFAULT_OPERATIONS),
        seed=raw.get("seed"),
    )


# ── Public API ──────────────────────────────────────────────────────


def load_scenario(path: str | Path) -> ScenarioConfig:
    """Load and validate a scenario YAML file.

    Parameters
    ----------
    path:
        Filesystem path to the YAML scenario file.

    Returns
    -------
    ScenarioConfig
        Fully validated scenario configuration.

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    ValueError
        If required fields are missing or values are invalid.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Scenario file not found: {path}")

    logger.info("Loading scenario from %s", path)
    with path.open() as fh:
        data: dict[str, Any] = yaml.safe_load(fh)

    if not isinstance(data, dict):
        raise ValueError(f"Scenario file must be a YAML mapping, got {type(data).__name__}")

    # ── scenario metadata ──
    scenario_section = _require(data, "scenario", "root")
    name = _require(scenario_section, "name", "scenario")
    partner_name = _require(scenario_section, "partner_name", "scenario")
    source_type = _require(scenario_section, "source_type", "scenario")

    # ── tables ──
    raw_tables = _require(data, "tables", "root")
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ValueError("'tables' must be a non-empty list")
    tables = [_parse_table(t) for t in raw_tables]

    # ── generation ──
    generation = _parse_generation(data.get("generation"))

    logger.info(
        "Loaded scenario '%s' with %d table(s), mode=%s",
        name,
        len(tables),
        generation.mode,
    )

    return ScenarioConfig(
        name=name,
        partner_name=partner_name,
        source_type=source_type,
        tables=tables,
        generation=generation,
    )


def load_app_config() -> AppConfig:
    """Load application config from environment variables.

    Expected variables:

    * ``ONELAKE_WORKSPACE_ID`` (required)
    * ``ONELAKE_MIRRORED_DB_ID`` (required)
    * ``LOG_LEVEL`` (optional, default ``"INFO"``)

    Raises
    ------
    ValueError
        If a required environment variable is not set.
    """
    workspace_id = os.environ.get("ONELAKE_WORKSPACE_ID", "")
    mirrored_db_id = os.environ.get("ONELAKE_MIRRORED_DB_ID", "")

    missing: list[str] = []
    if not workspace_id:
        missing.append("ONELAKE_WORKSPACE_ID")
    if not mirrored_db_id:
        missing.append("ONELAKE_MIRRORED_DB_ID")

    if missing:
        raise ValueError(f"Missing required environment variable(s): {', '.join(missing)}")

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    return AppConfig(
        workspace_id=workspace_id,
        mirrored_db_id=mirrored_db_id,
        log_level=log_level,
    )
