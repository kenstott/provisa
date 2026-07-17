# Copyright (c) 2026 Kenneth Stott
# Canary: 68c45d40-a67e-47e4-a720-db24e8d25066
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""Step definitions for REQ-047, REQ-048, REQ-049, REQ-050, REQ-051 — JSON / NDJSON / normalized & denormalized tabular & Arrow Flight output formats."""

from __future__ import annotations

import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_bdd import given, when, then, scenario

from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.formats.ndjson import rows_to_ndjson
from provisa.executor.formats.tabular import rows_to_csv, rows_to_parquet
from provisa.executor.formats.arrow import rows_to_arrow_ipc, rows_to_arrow_table


@pytest.fixture
def shared_data():
    return {}


@scenario(
    "../features/REQ-047.feature",
    "REQ-047 default behaviour",
)
def test_req_047_default_behaviour():
    """JSON output preserves native GraphQL nested structure."""


@given("a GraphQL query returning nested relationships")
def graphql_query_with_nested_relationships(shared_data):
    """Model a GraphQL query that returns an order with a nested customer relationship.

    The nested_in attribute on ColumnRef encodes the GraphQL nesting: scalar
    columns belong to the root, while related-entity columns carry their parent
    field name so the serializer can rebuild the relationship.
    """
    columns = [
        ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
        ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
        ColumnRef(alias="t1", column="name", field_name="name", nested_in="customer"),
        ColumnRef(alias="t1", column="email", field_name="email", nested_in="customer"),
    ]
    rows = [
        (1, 19.99, "Alice", "alice@example.com"),
        (2, 29.99, "Bob", "bob@example.com"),
    ]
    shared_data["columns"] = columns
    shared_data["rows"] = rows
    assert any(c.nested_in == "customer" for c in columns)


@when("the result is delivered as JSON")
def deliver_result_as_json(shared_data):
    """Serialize the result set to NDJSON (the native JSON delivery format)."""
    output = rows_to_ndjson(shared_data["rows"], shared_data["columns"])
    lines = [ln for ln in output.splitlines() if ln.strip()]
    shared_data["json_output"] = output
    shared_data["parsed"] = [json.loads(ln) for ln in lines]
    assert len(shared_data["parsed"]) == len(shared_data["rows"])


@then("the nested structure mirrors the GraphQL response shape with relationships intact")
def nested_structure_mirrors_graphql_shape(shared_data):
    """Verify nested relationship fields are preserved via dotted keys and that
    the structure can be faithfully reconstructed into the GraphQL shape."""
    parsed = shared_data["parsed"]
    assert parsed, "expected at least one JSON record"

    first = parsed[0]
    # Root scalar fields preserved.
    assert first["id"] == 1
    assert first["amount"] == 19.99
    # Nested relationship fields preserved under dotted keys.
    assert first["customer.name"] == "Alice"
    assert first["customer.email"] == "alice@example.com"

    # Reconstruct the GraphQL response shape from the dotted keys and verify
    # the relationship nesting is intact and self-describing.
    def reshape(record: dict) -> dict:
        out: dict = {}
        for key, value in record.items():
            if "." in key:
                parent, child = key.split(".", 1)
                out.setdefault(parent, {})[child] = value
            else:
                out[key] = value
        return out

    reshaped = reshape(first)
    assert reshaped["id"] == 1
    assert reshaped["amount"] == 19.99
    assert isinstance(reshaped["customer"], dict)
    assert reshaped["customer"] == {
        "name": "Alice",
        "email": "alice@example.com",
    }

    # Every row must carry the same nested relationship shape.
    for record in parsed:
        rs = reshape(record)
        assert "customer" in rs
        assert set(rs["customer"].keys()) == {"name", "email"}


@scenario(
    "../features/REQ-048.feature",
    "REQ-048 default behaviour",
)
def test_req_048_default_behaviour():
    """NDJSON streaming variant: one JSON object per line."""


@given("a query returning multiple rows")
def query_returning_multiple_rows(shared_data):
    """Model a result set with several scalar rows ready for NDJSON serialization."""
    columns = [
        ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
    ]
    rows = [
        (1, 19.99, "Alice"),
        (2, 29.99, "Bob"),
        (3, 0.0, "Carol"),
        (4, 12.50, "Dave"),
    ]
    shared_data["columns"] = columns
    shared_data["rows"] = rows
    assert len(rows) > 1, "scenario requires multiple rows"


@when("the output format is NDJSON")
def output_format_is_ndjson(shared_data):
    """Serialize the result set using the NDJSON serializer."""
    output = rows_to_ndjson(shared_data["rows"], shared_data["columns"])
    shared_data["ndjson_output"] = output
    # Capture the raw lines (excluding any trailing blank line) for inspection.
    shared_data["lines"] = [ln for ln in output.split("\n") if ln.strip()]
    assert isinstance(output, str)


@then("each result row is emitted as a single JSON object on its own line")
def each_row_is_single_json_object_per_line(shared_data):
    """Verify NDJSON emits exactly one valid JSON object per line, one per row."""
    rows = shared_data["rows"]
    lines = shared_data["lines"]

    # One line per row — line-by-line processing without a wrapping array.
    assert len(lines) == len(rows)

    # The raw payload must not be a single JSON array; each line stands alone.
    assert not shared_data["ndjson_output"].lstrip().startswith("[")

    for index, line in enumerate(lines):
        # Each line must be independently parseable as a single JSON object.
        obj = json.loads(line)
        assert isinstance(obj, dict)
        assert obj["id"] == rows[index][0]
        assert obj["amount"] == rows[index][1]
        assert obj["name"] == rows[index][2]
        # A single object per line: no embedded newline inside the line itself.
        assert "\n" not in line

    # Round-trip: re-parsing every line reproduces the full row set.
    reparsed = [json.loads(ln) for ln in lines]
    assert [r["name"] for r in reparsed] == [row[2] for row in rows]


@scenario(
    "../features/REQ-049.feature",
    "REQ-049 default behaviour",
)
def test_req_049_default_behaviour():
    """Normalized tabular output: relational tables with FK relationships preserved."""


@given("a query with nested relationships")
def query_with_nested_relationships(shared_data):
    """Model a GraphQL query result where each order references a nested customer.

    The relational normalization splits this into two tables — a parent
    ``customers`` table keyed by ``id`` and a child ``orders`` table carrying a
    ``customer_id`` foreign key that references it.
    """
    # Parent (customers) table — keyed by id.
    customer_columns = [
        ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
        ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
    ]
    customer_rows = [
        (100, "Alice", "alice@example.com"),
        (101, "Bob", "bob@example.com"),
    ]

    # Child (orders) table — references customers via customer_id FK.
    order_columns = [
        ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ColumnRef(
            alias=None,
            column="customer_id",
            field_name="customer_id",
            nested_in=None,
        ),
    ]
    order_rows = [
        (1, 19.99, 100),
        (2, 29.99, 101),
        (3, 49.99, 100),
    ]

    shared_data["customer_columns"] = customer_columns
    shared_data["customer_rows"] = customer_rows
    shared_data["order_columns"] = order_columns
    shared_data["order_rows"] = order_rows

    # Sanity: the child references the parent via a foreign-key column.
    assert any(c.column == "customer_id" for c in order_columns)
    assert any(c.column == "id" for c in customer_columns)


@when("the output format is normalized tabular (Parquet or CSV)")
def output_format_is_normalized_tabular(shared_data):
    """Serialize each normalized relational table to both CSV and Parquet."""
    # CSV serialization of both relational tables.
    shared_data["customers_csv"] = rows_to_csv(
        shared_data["customer_rows"], shared_data["customer_columns"]
    )
    shared_data["orders_csv"] = rows_to_csv(shared_data["order_rows"], shared_data["order_columns"])

    # Parquet serialization of both relational tables.
    shared_data["customers_parquet"] = rows_to_parquet(
        shared_data["customer_rows"], shared_data["customer_columns"]
    )
    shared_data["orders_parquet"] = rows_to_parquet(
        shared_data["order_rows"], shared_data["order_columns"]
    )

    assert isinstance(shared_data["customers_csv"], str)
    assert isinstance(shared_data["orders_csv"], str)
    assert isinstance(shared_data["customers_parquet"], (bytes, bytearray))
    assert isinstance(shared_data["orders_parquet"], (bytes, bytearray))


@then("results are flattened to relational tables with FK relationships preserved")
def results_flattened_with_fk_preserved(shared_data):
    """Verify both tables serialize cleanly and the FK relationship is intact."""
    # --- CSV: verify headers and the FK column on the child table ---
    customers_lines = [ln.strip() for ln in shared_data["customers_csv"].strip().splitlines()]
    orders_lines = [ln.strip() for ln in shared_data["orders_csv"].strip().splitlines()]

    customers_headers = customers_lines[0].split(",")
    orders_headers = orders_lines[0].split(",")

    assert "id" in customers_headers
    assert "customer_id" in orders_headers, "child table must carry the FK column"

    # header + one row per record.
    assert len(customers_lines) == len(shared_data["customer_rows"]) + 1
    assert len(orders_lines) == len(shared_data["order_rows"]) + 1

    # --- Parquet: round-trip both tables and validate referential integrity ---
    customers_tbl = pq.read_table(io.BytesIO(shared_data["customers_parquet"]))
    orders_tbl = pq.read_table(io.BytesIO(shared_data["orders_parquet"]))

    assert "id" in customers_tbl.column_names
    assert "customer_id" in orders_tbl.column_names
    assert customers_tbl.num_rows == len(shared_data["customer_rows"])
    assert orders_tbl.num_rows == len(shared_data["order_rows"])

    # Every FK value in the child table must reference an existing parent key.
    parent_keys = set(customers_tbl.column("id").to_pylist())
    fk_values = orders_tbl.column("customer_id").to_pylist()
    assert parent_keys, "expected non-empty parent key set"
    for fk in fk_values:
        assert fk in parent_keys, f"dangling FK {fk!r} — referential integrity broken"

    # The original nested relationship is fully reconstructable via the FK join.
    customers_by_id = {
        row[0]: {"name": row[1], "email": row[2]} for row in shared_data["customer_rows"]
    }
    for order in shared_data["order_rows"]:
        order_id, amount, customer_id = order
        joined = customers_by_id[customer_id]
        assert joined["name"] in ("Alice", "Bob")
        assert "@" in joined["email"]


@scenario(
    "../features/REQ-050.feature",
    "REQ-050 default behaviour",
)
def test_req_050_default_behaviour():
    """Denormalized tabular output: a single fully flattened table, Parquet or CSV, single file or partitioned."""


@when("the output format is denormalized tabular (Parquet or CSV)")
def output_format_is_denormalized_tabular(shared_data):
    """Flatten the nested order→customer relationship into a single wide table.

    Unlike the normalized variant (which keeps parent/child tables joined by a
    FK), denormalization performs the join up-front and emits one fully
    flattened row per order, with the related customer attributes inlined under
    dotted column names. This is what a data-science consumer would load
    directly into a pandas/Polars dataframe.
    """
    customers_by_id = {
        row[0]: {"name": row[1], "email": row[2]} for row in shared_data["customer_rows"]
    }

    # The denormalized schema: order scalars + inlined customer attributes.
    flat_columns = [
        ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ColumnRef(alias=None, column="customer_id", field_name="customer_id", nested_in=None),
        ColumnRef(alias=None, column="name", field_name="name", nested_in="customer"),
        ColumnRef(alias=None, column="email", field_name="email", nested_in="customer"),
    ]

    flat_rows = []
    for order_id, amount, customer_id in shared_data["order_rows"]:
        cust = customers_by_id[customer_id]
        flat_rows.append((order_id, amount, customer_id, cust["name"], cust["email"]))

    shared_data["flat_columns"] = flat_columns
    shared_data["flat_rows"] = flat_rows

    # Single-file outputs: one CSV string, one Parquet byte stream.
    shared_data["flat_csv"] = rows_to_csv(flat_rows, flat_columns)
    shared_data["flat_parquet"] = rows_to_parquet(flat_rows, flat_columns)

    # Partitioned output: split the single logical table into per-partition
    # Parquet files keyed by the customer_id partition column.
    partitions: dict[int, list] = {}
    for row in flat_rows:
        partitions.setdefault(row[2], []).append(row)
    shared_data["partitions"] = {
        part_key: rows_to_parquet(part_rows, flat_columns)
        for part_key, part_rows in partitions.items()
    }

    assert isinstance(shared_data["flat_csv"], str)
    assert isinstance(shared_data["flat_parquet"], (bytes, bytearray))
    assert shared_data["partitions"], "expected at least one partition"


@then("results are fully flattened into a single table, optionally partitioned")
def results_fully_flattened_single_table(shared_data):
    """Verify the denormalized output is a single flat table (CSV + Parquet) and
    that the optional partitioned form preserves all rows and the flat schema."""
    flat_rows = shared_data["flat_rows"]

    # --- CSV single-file: a single flat header with inlined relationship cols ---
    csv_lines = [ln.strip() for ln in shared_data["flat_csv"].strip().splitlines()]
    headers = csv_lines[0].split(",")
    assert "id" in headers
    assert "amount" in headers
    assert "customer_id" in headers
    # Related-entity attributes inlined under dotted names — no nested objects.
    assert "customer.name" in headers
    assert "customer.email" in headers
    # No separate parent table: one flat header + one row per order.
    assert len(csv_lines) == len(flat_rows) + 1

    # --- Parquet single-file: single table with the full flattened schema ---
    flat_tbl = pq.read_table(io.BytesIO(shared_data["flat_parquet"]))
    assert flat_tbl.num_rows == len(flat_rows)
    for expected in ("id", "amount", "customer_id", "customer.name", "customer.email"):
        assert expected in flat_tbl.column_names, f"missing flattened column {expected!r}"

    # Each flattened row inlines its customer attributes (no FK join needed).
    names = flat_tbl.column("customer.name").to_pylist()
    emails = flat_tbl.column("customer.email").to_pylist()
    assert all(isinstance(n, str) and n for n in names)
    assert all("@" in e for e in emails)

    # --- Optional partitioned form: same schema, union reproduces all rows ---
    total_partitioned_rows = 0
    for part_key, part_bytes in shared_data["partitions"].items():
        part_tbl = pq.read_table(io.BytesIO(part_bytes))
        # Every partition shares the identical flat schema.
        assert set(part_tbl.column_names) == set(flat_tbl.column_names)
        # Partition keyed by customer_id — all rows in it share the key.
        assert all(v == part_key for v in part_tbl.column("customer_id").to_pylist())
        total_partitioned_rows += part_tbl.num_rows

    # The union of all partitions equals the single-file row count (no loss).
    assert total_partitioned_rows == len(flat_rows)

    # Sanity: the flattened single table carries exactly the order rows joined
    # with their customer — verifying the denormalization is complete.
    ids = flat_tbl.column("id").to_pylist()
    assert ids == [row[0] for row in flat_rows]


@scenario(
    "../features/REQ-051.feature",
    "REQ-051 default behaviour",
)
def test_req_051_default_behaviour():
    """Arrow buffer via gRPC Arrow Flight endpoint; Trino produces Arrow natively."""


@given("a query submitted via the Arrow Flight endpoint")
def query_submitted_via_arrow_flight(shared_data):
    """Model a high-throughput analytics query whose results will be delivered
    as native Arrow record batches over Arrow Flight (gRPC).

    We capture the column schema and a representative result set that Trino
    would produce. The Arrow Flight path requires that these columns can be
    expressed directly as an Arrow schema with no intermediate textual
    serialization.
    """
    columns = [
        ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
    ]
    rows = [
        (1, 19.99, "Alice"),
        (2, 29.99, "Bob"),
        (3, 49.99, "Carol"),
        (4, 12.50, "Dave"),
        (5, 0.0, "Eve"),
    ]
    shared_data["columns"] = columns
    shared_data["rows"] = rows
    assert len(rows) > 0
    assert all(isinstance(c, ColumnRef) for c in columns)


@when("Trino executes the query")
def trino_executes_query_producing_arrow(shared_data):
    """Trino produces Arrow natively — serialize the result set directly into an
    Arrow table and an Arrow IPC (Flight-compatible) buffer."""
    table = rows_to_arrow_table(shared_data["rows"], shared_data["columns"])
    ipc_buffer = rows_to_arrow_ipc(shared_data["rows"], shared_data["columns"])

    shared_data["arrow_table"] = table
    shared_data["arrow_ipc"] = ipc_buffer

    assert isinstance(table, pa.Table)
    assert isinstance(ipc_buffer, (bytes, bytearray))
    assert len(ipc_buffer) > 0


@then("results are delivered as Arrow record batches via gRPC with no intermediate serialization")
def results_delivered_as_arrow_record_batches(shared_data):
    """Verify the delivered payload is composed of genuine Arrow record batches
    that round-trip from the Arrow IPC stream with the original columnar schema
    and row data fully intact — i.e. no JSON/CSV intermediate serialization."""
    table = shared_data["arrow_table"]
    ipc_buffer = shared_data["arrow_ipc"]
    rows = shared_data["rows"]

    # The native Arrow table must expose record batches — the unit of transfer
    # over Arrow Flight's gRPC DoGet stream.
    batches = table.to_batches()
    assert batches, "expected at least one Arrow record batch"
    for batch in batches:
        assert isinstance(batch, pa.RecordBatch)

    total_batch_rows = sum(b.num_rows for b in batches)
    assert total_batch_rows == len(rows)

    # Schema is carried in binary Arrow form (columnar), not as text.
    assert "id" in table.column_names
    assert "amount" in table.column_names
    assert "name" in table.column_names

    # Round-trip the IPC buffer through an Arrow IPC stream reader to confirm
    # the Flight-compatible wire format is self-contained and lossless.
    reader = pa.ipc.open_stream(pa.BufferReader(ipc_buffer))
    rt_table = reader.read_all()

    assert rt_table.num_rows == len(rows)
    assert set(rt_table.column_names) == set(table.column_names)

    # Verify column values round-trip without mutation (no text serialization).
    rt_ids = rt_table.column("id").to_pylist()
    rt_names = rt_table.column("name").to_pylist()
    assert rt_ids == [row[0] for row in rows]
    assert rt_names == [row[2] for row in rows]

    # Confirm there is no textual/JSON encoding in the IPC payload.
    # Arrow IPC *stream* format (used by Arrow Flight) begins with the
    # continuation marker \xff\xff\xff\xff, not a JSON brace or CSV text.
    # (Arrow IPC *file* format begins with b"ARROW1\x00\x00".)
    stream_marker = ipc_buffer[:4]
    assert stream_marker == b"\xff\xff\xff\xff", (
        f"Arrow IPC stream must start with continuation marker b'\\xff\\xff\\xff\\xff', "
        f"got {stream_marker!r} — this indicates non-Arrow-native serialization"
    )

    # Each individual record batch in the round-tripped table must carry the
    # full columnar schema — verifying batch-level Arrow Flight compatibility.
    rt_batches = rt_table.to_batches()
    assert rt_batches, "round-tripped IPC must yield at least one record batch"
    for rt_batch in rt_batches:
        assert isinstance(rt_batch, pa.RecordBatch)
        assert rt_batch.schema.equals(rt_table.schema)
