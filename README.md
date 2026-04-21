# Open Mirroring Faker

Generate synthetic data for Microsoft Fabric Open Mirroring demos.

A standalone CLI tool that writes Parquet files — with the required `__rowMarker__` column and metadata — directly to OneLake's landing zone, so Fabric automatically ingests them into Delta tables. No external database needed.

## What It Does

```
Scenario YAML → Fake Data Generator → Parquet (with __rowMarker__) → OneLake Landing Zone → Fabric Delta Tables
```

- **Define** tables and columns in a simple YAML scenario file
- **Generate** realistic fake data using [Faker](https://faker.readthedocs.io/) and configurable generators
- **Write** Parquet files with Open Mirroring's `__rowMarker__` column (inserts, updates, deletes)
- **Upload** to OneLake via ADLS Gen2, with metadata files Fabric expects (`_metadata.json`, `_partnerEvents.json`)
- **Fabric auto-ingests** the landing zone into Delta tables — no manual trigger needed

## Quick Start

```bash
# Install uv (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/lmoloney/OpenMirroringFaker.git
cd OpenMirroringFaker
uv sync

# Dry run — no Azure credentials needed
uv run omf run --scenario retail --dry-run --batches 5

# Run against Fabric
cp .env.example .env
# Edit .env with your workspace and mirrored DB IDs
uv run omf run --scenario retail
```

Dry-run mode writes Parquet files to `./omf-output/` so you can inspect the output without any cloud setup.

## CLI Commands

### `omf run` — Run a Data Generation Scenario

```bash
uv run omf run --scenario <name-or-path> [options]
```

| Option | Short | Description |
|---|---|---|
| `--scenario` | `-s` | **Required.** Built-in scenario name (e.g. `retail`, `hr`) or path to a YAML file. |
| `--schema` | `-S` | Override schema name for all tables (e.g. `--schema myschema`). |
| `--mode` | `-m` | Override generation mode: `continuous` or `batch`. |
| `--batches` | `-n` | Override total number of batches. |
| `--batch-size` | `-b` | Override rows per batch. |
| `--interval` | `-i` | Override interval between batches in seconds (continuous mode). |
| `--dry-run` | | Write Parquet files locally instead of uploading to OneLake. |
| `--log-level` | | Set log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default: `INFO`. |

**Examples:**

```bash
# Continuous mode — runs until Ctrl+C
uv run omf run --scenario retail

# HR scenario with custom schema
uv run omf run --scenario hr --schema company_hr --dry-run --batches 3

# 10 batches of 100 rows each, 2s apart
uv run omf run --scenario retail --batches 10 --batch-size 100 --interval 2

# Single batch mode
uv run omf run --scenario retail --mode batch --batch-size 500

# Custom scenario file
uv run omf run --scenario ./my-scenario.yaml --dry-run

# Debug logging
uv run omf run --scenario retail --dry-run --batches 1 --log-level DEBUG
```

### `omf scenarios` — List Built-in Scenarios

```bash
uv run omf scenarios
```

```
Built-in scenarios:

  retail        Retail demo — Customers, Orders, Products
  hr            HR demo — Employees, Departments, TimeOff

Usage: omf run --scenario <name>
```

### `omf init` — Scaffold a New Scenario

```bash
uv run omf init [output-file]
```

Creates a starter YAML file with a sample table definition and sensible defaults. Default output: `my-scenario.yaml`.

```bash
uv run omf init sales-demo.yaml
# Edit sales-demo.yaml to define your tables
uv run omf run --scenario sales-demo.yaml --dry-run
```

## Scenario YAML Format

```yaml
# ── Metadata ──
scenario:
  name: "My Demo"                    # Display name
  partner_name: "OpenMirroringFaker" # Appears in _partnerEvents.json
  source_type: "FakeDB"             # Source system identifier

# ── Table definitions ──
tables:
  - name: "Customers"               # Table name in Fabric
    schema: "dbo"                    # Schema prefix (default: "dbo")
    key_columns: ["CustomerID"]      # Primary key column(s) — used for updates/deletes
    columns:
      - name: "CustomerID"
        type: "int"                  # Column type (see Column Types below)
        generator: "sequence"        # Data generator (see Available Generators below)
      - name: "Email"
        type: "string"
        generator: "email"
      - name: "Score"
        type: "float"
        generator: "random_float"
        args:                        # Generator-specific arguments
          min: 0.0
          max: 100.0
          precision: 2

# ── Generation settings ──
generation:
  mode: "continuous"                 # "continuous" (loop with interval) or "batch" (run once)
  batch_size: 50                     # Rows per batch per table
  interval_seconds: 5               # Seconds between batches (continuous mode)
  total_batches: null                # null = unlimited (continuous) or single (batch)
  operations:                        # Mix of row operations per batch
    insert: 0.7                      # 70% new rows
    update: 0.2                      # 20% updates to previously inserted rows
    delete: 0.1                      # 10% deletes of previously inserted rows
  seed: null                         # Set for reproducible output
```

## Available Generators

| Generator | Description | Args |
|---|---|---|
| `sequence` | Auto-incrementing integer | `start` (default: `1`) |
| `random_int` | Random integer in range | `min` (default: `1`), `max` (default: `1000`) |
| `random_float` | Random float in range | `min` (default: `0.0`), `max` (default: `100.0`), `precision` (default: `2`) |
| `first_name` | Faker first name | — |
| `last_name` | Faker last name | — |
| `email` | Faker email address | — |
| `city` | Faker city name | — |
| `company` | Faker company name | — |
| `address` | Faker street address | — |
| `phone_number` | Faker phone number | — |
| `text` | Faker text paragraph | `max_nb_chars` (default: `200`) |
| `sentence` | Faker sentence | — |
| `now` | Current UTC datetime | — |
| `date_this_year` | Random date in the current year | — |
| `choice` | Random selection from a list | `values` (**required**, list of strings) |
| `uuid` | UUID4 string | — |
| `boolean` | Random boolean | `probability` (default: `0.5`, chance of `true`) |

## Column Types

| Type | PyArrow Type | Example |
|---|---|---|
| `int` | `int64` | `42` |
| `float` | `float64` | `3.14` |
| `string` | `utf8` | `"hello"` |
| `datetime` | `timestamp[ms]` | `2024-01-15T10:30:00` |
| `boolean` | `bool` | `true` |

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ONELAKE_WORKSPACE_ID` | Yes (unless `--dry-run`) | Fabric workspace GUID — from the Power BI URL `/groups/<id>/` |
| `ONELAKE_MIRRORED_DB_ID` | Yes (unless `--dry-run`) | Mirrored database GUID — from the URL `/mirroreddatabases/<id>/` |
| `LOG_LEVEL` | No | Log level override. Default: `INFO` |

Copy `.env.example` to `.env` and fill in your values. The `.env` file is gitignored.

Authentication uses Azure `DefaultAzureCredential` — this works with `az login`, managed identity, environment variables, and other standard Azure auth methods.

## How Open Mirroring Works

Open Mirroring lets external tools write data into Fabric by placing Parquet files in a landing zone on OneLake. Fabric watches this zone and automatically ingests new files into Delta tables.

### Landing zone structure

```
<mirrored-db-id>/Files/LandingZone/
├── _partnerEvents.json              # Partner identity (written once)
├── dbo.schema/
│   ├── Customers/
│   │   ├── _metadata.json           # Key columns + detection strategy
│   │   ├── <uuid>.parquet           # Data files
│   │   └── ...
│   ├── Orders/
│   │   ├── _metadata.json
│   │   └── ...
```

### Metadata files

**`_partnerEvents.json`** — Written at the landing zone root. Identifies the data source:

```json
{
  "partnerName": "OpenMirroringFaker",
  "sourceInfo": { "sourceType": "FakeRetailDB" }
}
```

**`_metadata.json`** — Written per table. Declares key columns and detection strategy:

```json
{
  "keyColumns": ["CustomerID"],
  "fileDetectionStrategy": "LastUpdateTimeFileDetection",
  "isUpsertDefaultRowMarker": true
}
```

### `__rowMarker__` values

Every Parquet file includes a `__rowMarker__` column that tells Fabric which operation to apply:

| Value | Operation | Meaning |
|---|---|---|
| `0` | Insert | New row |
| `4` | Upsert | Insert or update by key |
| `2` | Delete | Remove row by key |

The tool automatically manages these markers based on the `operations` weights in your scenario config.

## Development

```bash
# Install all dependencies (including dev)
uv sync

# Run tests
uv run pytest

# Lint
uv run ruff check src/ tests/

# Type check
uv run pyright
```

Requires Python ≥ 3.12.

## License

[MIT](LICENSE)
