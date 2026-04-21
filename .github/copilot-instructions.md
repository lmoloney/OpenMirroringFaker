# Open Mirroring Faker — Copilot Instructions

Standalone CLI tool that generates synthetic data and writes it to Microsoft Fabric's OneLake Open Mirroring landing zone. Used for demos, workshops, and testing — no Debezium, no EventHub, no source database required.

## Build / Test / Lint Commands

```bash
uv sync                              # Install deps
uv run pytest                        # Run tests
uv run ruff check src/ tests/        # Lint
uv run ruff format src/ tests/       # Format
uv run pyright                       # Type check
uv run omf run --scenario scenarios/retail.yaml  # Run retail demo
uv run omf run --scenario scenarios/retail.yaml --dry-run  # Dry run (no Azure)
```

## Project Structure

```
src/open_mirroring_faker/
├── __init__.py
├── cli.py                 # Click CLI entry point (run/scenarios/init)
├── config.py              # Scenario YAML loading + validation
├── data_generator.py      # Fake data generation with Faker
├── parquet_builder.py     # Build Parquet with __rowMarker__ last column
├── onelake_writer.py      # Upload to OneLake via ADLS Gen2
└── scenarios/
    ├── __init__.py
    └── retail.py          # Built-in retail scenario
```

## Module Responsibilities

- **cli**: Click commands — `run` (execute scenario), `scenarios` (list built-ins), `init` (scaffold YAML).
- **config**: Loads scenario YAML. Validates table schemas, column definitions, generator names. Merges with env vars for OneLake connection. Dataclasses: `AppConfig`, `TableSchema`, `ColumnDef`, `GenerationConfig`.
- **data_generator**: Produces rows from a `TableSchema`. Tracks inserted IDs for update/delete operations. Configurable insert/update/delete ratios. Assigns `__rowMarker__` (0=insert, 4=upsert, 2=delete).
- **parquet_builder**: Converts `list[dict]` → Parquet bytes. `__rowMarker__` MUST be last column. Snappy compression. Type mapping: int→int64, float→float64, string→utf8, datetime→timestamp[ms].
- **onelake_writer**: ADLS Gen2 uploads via `DefaultAzureCredential`. Creates `_partnerEvents.json` and `_metadata.json`. UUID file names. Retry with backoff on 429/500/503.

## Key Conventions

- `__rowMarker__` values: 0=insert, 4=upsert, 2=delete. MUST be last column in Parquet.
- OneLake path: `<db-id>/Files/LandingZone/<Schema>.schema/<Table>/<uuid>.parquet`
- Metadata: `_metadata.json` per table with `keyColumns`, `fileDetectionStrategy`, `isUpsertDefaultRowMarker`.
- Scenarios are YAML files defining tables, columns, generators, and generation config.
- All Azure auth uses `DefaultAzureCredential`.

## Testing

- `pytest` for unit tests
- Mock Azure clients in OneLake writer tests
- Test Parquet output is valid and follows conventions (last column, compression, types)
