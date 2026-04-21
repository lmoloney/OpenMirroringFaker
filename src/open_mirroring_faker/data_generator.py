from __future__ import annotations

import logging
import random
import uuid
from datetime import UTC, datetime
from typing import Any

from faker import Faker

from .config import ColumnDef, TableSchema

logger = logging.getLogger(__name__)

# Row marker constants matching Open Mirroring spec
_MARKER_INSERT = 0
_MARKER_DELETE = 2
_MARKER_UPDATE = 4


class DataGenerator:
    """Generates fake rows based on a table schema definition."""

    def __init__(self, table: TableSchema, seed: int | None = None) -> None:
        self.table = table
        self.fake = Faker()
        self._rng = random.Random()
        self._sequence_counters: dict[str, int] = {}
        self._inserted_rows: list[dict[str, Any]] = []

        if seed is not None:
            self.fake.seed_instance(seed)
            self._rng.seed(seed)

    # ── public API ──────────────────────────────────────────────

    def generate_batch(
        self,
        count: int,
        operations: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
        """Generate a batch of rows with mixed operations.

        Args:
            count: Number of rows to generate.
            operations: Weight dict e.g. {"insert": 0.7, "update": 0.2, "delete": 0.1}.
                       If None, all rows are inserts.

        Returns:
            List of row dicts, each including ``__rowMarker__``.
        """
        if operations is None:
            return [self._generate_insert() for _ in range(count)]

        op_names = list(operations.keys())
        op_weights = [operations[k] for k in op_names]

        rows: list[dict[str, Any]] = []
        for _ in range(count):
            (op,) = self._rng.choices(op_names, weights=op_weights, k=1)

            if op == "insert":
                rows.append(self._generate_insert())
            elif op == "update":
                rows.append(self._generate_update())
            elif op == "delete":
                rows.append(self._generate_delete())
            else:
                msg = f"Unknown operation: {op}"
                raise ValueError(msg)

        return rows

    # ── operation helpers ───────────────────────────────────────

    def _generate_insert(self) -> dict[str, Any]:
        row = {col.name: self._generate_value(col) for col in self.table.columns}
        row["__rowMarker__"] = _MARKER_INSERT
        self._inserted_rows.append(row.copy())
        logger.debug("INSERT row with keys %s", {k: row[k] for k in self.table.key_columns})
        return row

    def _generate_update(self) -> dict[str, Any]:
        if not self._inserted_rows:
            logger.debug("No rows to update — falling back to insert")
            return self._generate_insert()

        idx = self._rng.randrange(len(self._inserted_rows))
        existing = self._inserted_rows[idx]

        row: dict[str, Any] = {}
        for col in self.table.columns:
            if col.name in self.table.key_columns:
                row[col.name] = existing[col.name]
            else:
                row[col.name] = self._generate_value(col)
        row["__rowMarker__"] = _MARKER_UPDATE

        # Update stored row to reflect new values
        self._inserted_rows[idx] = {k: v for k, v in row.items() if k != "__rowMarker__"}
        logger.debug("UPDATE row with keys %s", {k: row[k] for k in self.table.key_columns})
        return row

    def _generate_delete(self) -> dict[str, Any]:
        if not self._inserted_rows:
            logger.debug("No rows to delete — falling back to insert")
            return self._generate_insert()

        idx = self._rng.randrange(len(self._inserted_rows))
        existing = self._inserted_rows.pop(idx)

        row = {k: existing[k] for k in self.table.key_columns}
        row["__rowMarker__"] = _MARKER_DELETE
        logger.debug("DELETE row with keys %s", {k: row[k] for k in self.table.key_columns})
        return row

    # ── value generation ────────────────────────────────────────

    def _generate_value(self, column: ColumnDef) -> Any:
        """Dispatch to the appropriate generator for *column*."""
        gen = column.generator
        args = column.args

        match gen:
            case "sequence":
                return self._gen_sequence(column.name, args)
            case "random_int":
                return self._gen_random_int(args)
            case "random_float":
                return self._gen_random_float(args)
            case "first_name":
                return self.fake.first_name()
            case "last_name":
                return self.fake.last_name()
            case "email":
                return self.fake.email()
            case "city":
                return self.fake.city()
            case "company":
                return self.fake.company()
            case "address":
                return self.fake.street_address()
            case "phone_number":
                return self.fake.phone_number()
            case "text":
                return self.fake.text(max_nb_chars=args.get("max_nb_chars", 200))
            case "sentence":
                return self.fake.sentence()
            case "now":
                return datetime.now(UTC)
            case "date_this_year":
                d = self.fake.date_this_year()
                return datetime(d.year, d.month, d.day, tzinfo=UTC)
            case "choice":
                return self._gen_choice(args)
            case "uuid":
                return str(uuid.uuid4())
            case "boolean":
                return self._gen_boolean(args)
            case _:
                msg = f"Unknown generator: {gen!r} for column {column.name!r}"
                raise ValueError(msg)

    # ── individual generators ───────────────────────────────────

    def _gen_sequence(self, col_name: str, args: dict[str, Any]) -> int:
        start: int = args.get("start", 1)
        if col_name not in self._sequence_counters:
            self._sequence_counters[col_name] = start
        counter = self._sequence_counters[col_name]
        self._sequence_counters[col_name] = counter + 1
        return counter

    def _gen_random_int(self, args: dict[str, Any]) -> int:
        lo = args.get("min", 1)
        hi = args.get("max", 1000)
        return self._rng.randint(lo, hi)

    def _gen_random_float(self, args: dict[str, Any]) -> float:
        lo = args.get("min", 0.0)
        hi = args.get("max", 100.0)
        precision = args.get("precision", 2)
        return round(self._rng.uniform(lo, hi), precision)

    def _gen_choice(self, args: dict[str, Any]) -> str:
        values = args.get("values")
        if not values:
            msg = "Generator 'choice' requires a non-empty 'values' list in args"
            raise ValueError(msg)
        return self._rng.choice(values)

    def _gen_boolean(self, args: dict[str, Any]) -> bool:
        probability = args.get("probability", 0.5)
        return self._rng.random() < probability
