"""Tests for open_mirroring_faker.parquet_builder."""

from __future__ import annotations

import io

import pyarrow.parquet as pq
import pytest

from open_mirroring_faker.parquet_builder import build_parquet


def _sample_rows(n: int = 3) -> list[dict]:
    return [
        {"id": i, "name": f"user_{i}", "score": float(i * 10), "__rowMarker__": 0}
        for i in range(1, n + 1)
    ]


class TestBasicBuild:
    def test_basic_build(self) -> None:
        data = build_parquet(_sample_rows(5))

        assert isinstance(data, bytes)
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 5


class TestRowMarkerLastColumn:
    def test_row_marker_last_column(self) -> None:
        data = build_parquet(_sample_rows())
        table = pq.read_table(io.BytesIO(data))

        assert table.schema.names[-1] == "__rowMarker__"


class TestSnappyCompression:
    def test_snappy_compression(self) -> None:
        data = build_parquet(_sample_rows())
        pf = pq.ParquetFile(io.BytesIO(data))

        rg = pf.metadata.row_group(0)
        for i in range(rg.num_columns):
            assert rg.column(i).compression == "SNAPPY"


class TestEmptyRowsRaises:
    def test_empty_rows_raises(self) -> None:
        with pytest.raises(ValueError, match="rows must not be empty"):
            build_parquet([])


class TestWithColumnTypes:
    def test_with_column_types(self) -> None:
        rows = [{"id": 1, "val": 3.14, "label": "a", "flag": True, "__rowMarker__": 0}]
        types = {"id": "int", "val": "float", "label": "string", "flag": "boolean"}
        data = build_parquet(rows, column_types=types)

        table = pq.read_table(io.BytesIO(data))
        schema = table.schema
        assert str(schema.field("id").type) == "int64"
        assert str(schema.field("val").type) == "double"
        assert str(schema.field("label").type) == "string"
        assert str(schema.field("flag").type) == "bool"


class TestNullValuesHandled:
    def test_null_values_handled(self) -> None:
        rows = [
            {"id": 1, "name": None, "score": None, "__rowMarker__": 0},
            {"id": 2, "name": "alice", "score": 42.0, "__rowMarker__": 0},
        ]
        data = build_parquet(rows)

        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 2
        assert table.column("name")[0].as_py() is None
