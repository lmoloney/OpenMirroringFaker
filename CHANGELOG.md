# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Click CLI with `run`, `scenarios`, and `init` commands
- YAML-driven scenario definitions with 17 data generators (Faker-backed)
- Built-in scenarios: **retail** (Customers/Orders/Products) and **hr** (Employees/Departments/TimeOff)
- Parquet builder with `__rowMarker__` as last column, Snappy compression
- OneLake writer with `_metadata.json`, `_partnerEvents.json`, retry logic
- `--dry-run` mode for local testing without Azure credentials
- `--schema` CLI override to change schema without editing YAML
- Continuous (streaming simulator) and batch generation modes
- Configurable insert/update/delete operation mix
- 26 unit tests
