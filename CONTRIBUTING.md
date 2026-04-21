# Contributing to Open Mirroring Faker

Thanks for your interest in contributing! This tool generates synthetic data for Microsoft Fabric Open Mirroring demos.

## Getting Started

```bash
# Clone the repo
git clone https://github.com/lmoloney/OpenMirroringFaker.git
cd OpenMirroringFaker

# Install uv (if needed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies (including dev)
uv sync

# Run the test suite
uv run pytest

# Lint and type check
uv run ruff check src/ tests/
uv run pyright
```

## How to Contribute

### Reporting Bugs

Open an [issue](https://github.com/lmoloney/OpenMirroringFaker/issues) with:
- What you expected vs. what happened
- Steps to reproduce
- The scenario YAML you were using (if applicable)

### Suggesting Features

Open an issue describing the use case. Common contributions:
- New data generators
- New built-in scenarios
- Additional CLI options

### Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b my-feature`)
3. Make your changes
4. Ensure all checks pass:
   ```bash
   uv run ruff check src/ tests/
   uv run ruff format src/ tests/
   uv run pyright
   uv run pytest
   ```
5. Commit with a clear message
6. Open a pull request against `main`

### Adding a New Generator

1. Add the generator name to `VALID_GENERATORS` in `src/open_mirroring_faker/config.py`
2. Add a `case` branch in `DataGenerator._generate_value()` in `src/open_mirroring_faker/data_generator.py`
3. Add tests in `tests/test_data_generator.py`
4. Document the generator in `README.md` under "Available Generators"

### Adding a New Built-in Scenario

1. Create `scenarios/<name>.yaml` with your table definitions
2. Create `src/open_mirroring_faker/scenarios/<name>.py` with a `get_scenario_path()` function
3. Add the entry to `BUILTIN_SCENARIOS` in `src/open_mirroring_faker/scenarios/__init__.py`
4. Add the path to `_BUILTIN_PATHS` in `src/open_mirroring_faker/cli.py`
5. Document in `README.md`

## Code Style

- **Formatter/Linter:** [Ruff](https://docs.astral.sh/ruff/) (configured in `pyproject.toml`)
- **Type checking:** [Pyright](https://github.com/microsoft/pyright) in basic mode
- **Line length:** 120 characters
- **Python version:** 3.12+

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
