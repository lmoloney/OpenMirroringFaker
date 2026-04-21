"""Convert row dicts into Parquet bytes for Microsoft Fabric Open Mirroring."""

from __future__ import annotations

import io
import logging
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

_ROW_MARKER = "__rowMarker__"

_TYPE_MAP: dict[str, pa.DataType] = {
    "int": pa.int64(),
    "float": pa.float64(),
    "string": pa.utf8(),
    "datetime": pa.timestamp("ms"),
    "boolean": pa.bool_(),
}

_PYTHON_COERCE: dict[str, type] = {
    "int": int,
    "float": float,
    "string": str,
    "boolean": bool,
}


def _ordered_columns(rows: list[dict[str, Any]]) -> list[str]:
    """Return stable column order with __rowMarker__ guaranteed last."""
    seen: dict[str, None] = {}
    for row in rows:
        for key in row:
            if key not in seen:
                seen[key] = None

    cols = [c for c in seen if c != _ROW_MARKER]
    cols.append(_ROW_MARKER)
    return cols


def build_parquet(
    rows: list[dict[str, Any]],
    column_types: dict[str, str] | None = None,
) -> bytes:
    """Build an in-memory Parquet file from row dicts.

    __rowMarker__ is always the last column.
    Uses Snappy compression.

    Args:
        rows: List of row dicts. Each must have __rowMarker__ key.
        column_types: Optional mapping of column name → type hint
            ("int", "float", "string", "datetime", "boolean").
            Used for explicit type casting. If None, PyArrow infers types.

    Returns:
        Parquet file bytes.

    Raises:
        ValueError: If rows is empty.
    """
    if not rows:
        raise ValueError("rows must not be empty")

    columns = _ordered_columns(rows)
    logger.debug("Column order: %s", columns)

    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []

    for col in columns:
        values = [row.get(col) for row in rows]

        if column_types and col in column_types:
            type_key = column_types[col]
            arrow_type = _TYPE_MAP[type_key]
            coerce = _PYTHON_COERCE.get(type_key)
            if coerce:
                values = [coerce(v) if v is not None else None for v in values]
            arr = pa.array(values, type=arrow_type, from_pandas=True)
        else:
            arr = pa.array(values, from_pandas=True)

        arrays.append(arr)
        fields.append(pa.field(col, arr.type))

    schema = pa.schema(fields)
    table = pa.table({col: arr for col, arr in zip(columns, arrays, strict=True)}, schema=schema)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    logger.debug("Wrote Parquet: %d rows, %d bytes", len(rows), buf.tell())
    return buf.getvalue()
