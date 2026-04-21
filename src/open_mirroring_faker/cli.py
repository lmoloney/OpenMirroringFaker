"""CLI entry point for Open Mirroring Faker."""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Callable
from pathlib import Path

import click
from dotenv import load_dotenv

from .config import AppConfig, ScenarioConfig, load_app_config, load_scenario
from .data_generator import DataGenerator
from .onelake_writer import OneLakeWriter
from .parquet_builder import build_parquet
from .scenarios import BUILTIN_SCENARIOS
from .scenarios.hr import get_scenario_path as get_hr_path
from .scenarios.retail import get_scenario_path as get_retail_path

logger = logging.getLogger(__name__)

_BUILTIN_PATHS: dict[str, Callable[[], Path]] = {
    "retail": get_retail_path,
    "hr": get_hr_path,
}


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet Azure SDK noise
    logging.getLogger("azure").setLevel(logging.WARNING)


def _resolve_scenario(scenario: str) -> Path:
    """Resolve a scenario path — check built-in names first, then filesystem."""
    if scenario in _BUILTIN_PATHS:
        return _BUILTIN_PATHS[scenario]()
    path = Path(scenario)
    if path.exists():
        return path
    raise click.BadParameter(f"Scenario not found: {scenario!r} (not a built-in name or file path)")


def _build_column_types(scenario: ScenarioConfig) -> dict[str, dict[str, str]]:
    """Build per-table column type maps from scenario config."""
    result: dict[str, dict[str, str]] = {}
    for table in scenario.tables:
        key = f"{table.schema_name}.{table.name}"
        result[key] = {col.name: col.type for col in table.columns}
    return result


def _run_scenario(scenario: ScenarioConfig, app_config: AppConfig | None, dry_run: bool) -> None:
    """Execute a scenario — generate data and upload to OneLake (or write locally in dry-run)."""
    gen_config = scenario.generation
    column_types = _build_column_types(scenario)

    # Set up generators per table
    generators: dict[str, DataGenerator] = {}
    for table in scenario.tables:
        generators[f"{table.schema_name}.{table.name}"] = DataGenerator(table, seed=gen_config.seed)

    # Set up OneLake writer (unless dry-run)
    writer: OneLakeWriter | None = None
    if not dry_run:
        if app_config is None:
            raise click.ClickException("OneLake config required (set ONELAKE_WORKSPACE_ID and ONELAKE_MIRRORED_DB_ID)")
        writer = OneLakeWriter(app_config.workspace_id, app_config.mirrored_db_id)
        writer.ensure_partner_events(scenario.partner_name, scenario.source_type)
        for table in scenario.tables:
            writer.ensure_table(table.schema_name, table.name, table.key_columns)

    # Dry-run output directory
    dry_run_dir: Path | None = None
    if dry_run:
        dry_run_dir = Path("omf-output")
        dry_run_dir.mkdir(exist_ok=True)
        click.echo(f"Dry-run mode — writing Parquet files to {dry_run_dir}/")

    batch_num = 0
    total_rows = 0

    click.echo(f"Running scenario '{scenario.name}' ({gen_config.mode} mode, batch_size={gen_config.batch_size})")
    click.echo(f"Tables: {', '.join(t.name for t in scenario.tables)}")
    if gen_config.mode == "continuous":
        click.echo(f"Interval: {gen_config.interval_seconds}s between batches (Ctrl+C to stop)")
    elif gen_config.total_batches is not None:
        click.echo(f"Total batches: {gen_config.total_batches}")
    click.echo("---")

    try:
        while True:
            batch_num += 1
            if gen_config.total_batches is not None and batch_num > gen_config.total_batches:
                break

            for table in scenario.tables:
                table_key = f"{table.schema_name}.{table.name}"
                gen = generators[table_key]
                rows = gen.generate_batch(gen_config.batch_size, gen_config.operations)

                if not rows:
                    continue

                parquet_bytes = build_parquet(rows, column_types=column_types.get(table_key))
                total_rows += len(rows)

                if writer:
                    fname = writer.upload_parquet(table.schema_name, table.name, parquet_bytes)
                    click.echo(
                        f"  Batch {batch_num} | {table.name}: {len(rows)} rows → {fname} ({len(parquet_bytes)} bytes)"
                    )
                elif dry_run_dir:
                    table_dir = dry_run_dir / f"{table.schema_name}.schema" / table.name
                    table_dir.mkdir(parents=True, exist_ok=True)
                    import uuid

                    fname = f"{uuid.uuid4()}.parquet"
                    (table_dir / fname).write_bytes(parquet_bytes)
                    click.echo(
                        f"  Batch {batch_num} | {table.name}: {len(rows)} rows → {fname} ({len(parquet_bytes)} bytes)"
                    )

            if gen_config.mode == "batch" and gen_config.total_batches is None:
                # Single batch in batch mode with no total specified
                break

            if gen_config.mode == "continuous":
                time.sleep(gen_config.interval_seconds)

    except KeyboardInterrupt:
        click.echo("\n--- Interrupted ---")

    final_batches = batch_num - 1 if gen_config.total_batches and batch_num > gen_config.total_batches else batch_num
    click.echo(f"Done. {final_batches} batches, {total_rows} total rows generated.")


# ── CLI commands ────────────────────────────────────────────────────


@click.group()
@click.version_option(package_name="open-mirroring-faker")
def cli() -> None:
    """Open Mirroring Faker — generate synthetic data for Fabric Open Mirroring demos."""


@cli.command()
@click.option("--scenario", "-s", required=True, help="Path to scenario YAML or built-in name (e.g. 'retail').")
@click.option("--mode", "-m", type=click.Choice(["continuous", "batch"]), default=None, help="Override generation mode")
@click.option("--batches", "-n", type=int, default=None, help="Override total number of batches.")
@click.option("--batch-size", "-b", type=int, default=None, help="Override rows per batch.")
@click.option("--interval", "-i", type=float, default=None, help="Override interval between batches (seconds).")
@click.option("--dry-run", is_flag=True, default=False, help="Generate Parquet locally without uploading to OneLake.")
@click.option("--schema", "-S", default=None, help="Override schema name for all tables.")
@click.option("--log-level", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR).")
def run(
    scenario: str,
    mode: str | None,
    batches: int | None,
    batch_size: int | None,
    interval: float | None,
    dry_run: bool,
    schema: str | None,
    log_level: str,
) -> None:
    """Run a data generation scenario."""
    load_dotenv()
    _configure_logging(log_level)

    scenario_path = _resolve_scenario(scenario)
    scenario_config = load_scenario(scenario_path)

    # Apply CLI overrides
    if mode is not None:
        scenario_config.generation.mode = mode
    if batches is not None:
        scenario_config.generation.total_batches = batches
    if batch_size is not None:
        scenario_config.generation.batch_size = batch_size
    if interval is not None:
        scenario_config.generation.interval_seconds = interval
    if schema is not None:
        for table in scenario_config.tables:
            table.schema_name = schema

    # Load app config only when not dry-run
    app_config: AppConfig | None = None
    if not dry_run:
        try:
            app_config = load_app_config()
        except ValueError as exc:
            click.echo(f"Error: {exc}", err=True)
            click.echo("Hint: set ONELAKE_WORKSPACE_ID and ONELAKE_MIRRORED_DB_ID, or use --dry-run", err=True)
            sys.exit(1)

    _run_scenario(scenario_config, app_config, dry_run)


@cli.command(name="scenarios")
def list_scenarios() -> None:
    """List built-in demo scenarios."""
    click.echo("Built-in scenarios:\n")
    for name, description in BUILTIN_SCENARIOS.items():
        click.echo(f"  {name:12s}  {description}")
    click.echo("\nUsage: omf run --scenario <name>")


@cli.command()
@click.argument("output", default="my-scenario.yaml")
def init(output: str) -> None:
    """Scaffold a new scenario YAML file."""
    template = """\
scenario:
  name: "My Demo"
  partner_name: "OpenMirroringFaker"
  source_type: "FakeDB"

tables:
  - name: "MyTable"
    schema: "dbo"
    key_columns: ["ID"]
    columns:
      - name: "ID"
        type: "int"
        generator: "sequence"
      - name: "Name"
        type: "string"
        generator: "first_name"
      - name: "Value"
        type: "float"
        generator: "random_float"
        args:
          min: 0.0
          max: 100.0
          precision: 2
      - name: "CreatedAt"
        type: "datetime"
        generator: "now"

generation:
  mode: "continuous"
  batch_size: 50
  interval_seconds: 5
  total_batches: null
  operations:
    insert: 0.7
    update: 0.2
    delete: 0.1
  seed: null
"""
    path = Path(output)
    if path.exists():
        click.echo(f"File already exists: {path}", err=True)
        sys.exit(1)
    path.write_text(template)
    click.echo(f"Created scenario file: {path}")
    click.echo(f"Edit it, then run: omf run --scenario {path}")
