"""Tests for open_mirroring_faker.data_generator."""

from __future__ import annotations

from open_mirroring_faker.config import ColumnDef, TableSchema
from open_mirroring_faker.data_generator import DataGenerator


def _make_table() -> TableSchema:
    return TableSchema(
        name="TestTable",
        schema_name="dbo",
        key_columns=["ID"],
        columns=[
            ColumnDef(name="ID", type="int", generator="sequence"),
            ColumnDef(name="Name", type="string", generator="first_name"),
            ColumnDef(
                name="Value", type="float", generator="random_float", args={"min": 0.0, "max": 100.0, "precision": 2}
            ),
            ColumnDef(name="Active", type="boolean", generator="boolean"),
        ],
    )


class TestGenerateInsertBatch:
    def test_generate_insert_batch(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        rows = gen.generate_batch(5)

        assert len(rows) == 5
        for row in rows:
            assert row["__rowMarker__"] == 0
            assert set(row.keys()) == {"ID", "Name", "Value", "Active", "__rowMarker__"}


class TestSequenceGenerator:
    def test_sequence_values_increment(self) -> None:
        gen = DataGenerator(_make_table(), seed=1)
        rows = gen.generate_batch(3)

        assert [r["ID"] for r in rows] == [1, 2, 3]


class TestRandomIntRange:
    def test_random_int_range(self) -> None:
        table = TableSchema(
            name="T",
            schema_name="dbo",
            key_columns=["ID"],
            columns=[
                ColumnDef(name="ID", type="int", generator="sequence"),
                ColumnDef(name="Score", type="int", generator="random_int", args={"min": 10, "max": 20}),
            ],
        )
        gen = DataGenerator(table, seed=42)
        rows = gen.generate_batch(50)

        for row in rows:
            assert 10 <= row["Score"] <= 20


class TestRandomFloatPrecision:
    def test_random_float_precision(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        rows = gen.generate_batch(20)

        for row in rows:
            assert 0.0 <= row["Value"] <= 100.0
            text = str(row["Value"])
            if "." in text:
                assert len(text.split(".")[1]) <= 2


class TestChoiceGenerator:
    def test_choice_values_from_list(self) -> None:
        options = ["red", "green", "blue"]
        table = TableSchema(
            name="T",
            schema_name="dbo",
            key_columns=["ID"],
            columns=[
                ColumnDef(name="ID", type="int", generator="sequence"),
                ColumnDef(name="Colour", type="string", generator="choice", args={"values": options}),
            ],
        )
        gen = DataGenerator(table, seed=42)
        rows = gen.generate_batch(30)

        for row in rows:
            assert row["Colour"] in options


class TestBooleanGenerator:
    def test_produces_bools(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        rows = gen.generate_batch(20)

        for row in rows:
            assert isinstance(row["Active"], bool)


class TestOperationsMix:
    def test_operations_mix(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        ops = {"insert": 0.5, "update": 0.3, "delete": 0.2}
        # Pre-populate with inserts so updates/deletes have rows to act on
        gen.generate_batch(20)

        rows = gen.generate_batch(100, operations=ops)
        markers = {r["__rowMarker__"] for r in rows}
        # With seed and 100 rows we expect all three marker types
        assert 0 in markers, "Expected at least one insert (marker 0)"
        assert 4 in markers, "Expected at least one update (marker 4)"
        assert 2 in markers, "Expected at least one delete (marker 2)"


class TestUpdateFallback:
    def test_update_falls_back_to_insert(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        assert len(gen._inserted_rows) == 0
        rows = gen.generate_batch(1, operations={"update": 1.0})
        assert rows[0]["__rowMarker__"] == 0  # fell back to insert


class TestDeleteFallback:
    def test_delete_falls_back_to_insert(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        assert len(gen._inserted_rows) == 0
        rows = gen.generate_batch(1, operations={"delete": 1.0})
        assert rows[0]["__rowMarker__"] == 0  # fell back to insert


class TestDeleteRemovesFromState:
    def test_delete_removes_from_state(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        gen.generate_batch(5)
        assert len(gen._inserted_rows) == 5

        gen.generate_batch(1, operations={"delete": 1.0})
        assert len(gen._inserted_rows) == 4


class TestUpdatePreservesKeyColumns:
    def test_update_preserves_key_columns(self) -> None:
        gen = DataGenerator(_make_table(), seed=42)
        inserts = gen.generate_batch(3)
        original_ids = {r["ID"] for r in inserts}

        updates = gen.generate_batch(5, operations={"update": 1.0})
        for row in updates:
            assert row["__rowMarker__"] == 4
            assert row["ID"] in original_ids


class TestSeededReproducibility:
    def test_seeded_reproducibility(self) -> None:
        a = DataGenerator(_make_table(), seed=99)
        b = DataGenerator(_make_table(), seed=99)

        rows_a = a.generate_batch(10)
        rows_b = b.generate_batch(10)

        assert rows_a == rows_b
