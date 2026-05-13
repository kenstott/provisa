# Copyright (c) 2026 Kenneth Stott
# Canary: b1e4fa9b-266b-456d-8c07-8b148e402946
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for serialize_rows — JOIN rows → nested GraphQL JSON."""

from decimal import Decimal

from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.serialize import serialize_rows, serialize_aggregate


def _make_columns(specs: list[tuple[str, str | None]]) -> list[ColumnRef]:
    """Build ColumnRef list from (field_name, nested_in) tuples."""
    return [
        ColumnRef(alias=None, column=name, field_name=name, nested_in=nested)
        for name, nested in specs
    ]


class TestSerializeFlatRows:
    def test_simple_flat(self):
        columns = _make_columns([("id", None), ("amount", None), ("status", None)])
        rows = [
            (1, Decimal("19.99"), "completed"),
            (2, Decimal("99.98"), "pending"),
        ]
        result = serialize_rows(rows, columns, "orders")
        assert result == {
            "data": {
                "orders": [
                    {"id": 1, "amount": 19.99, "status": "completed"},
                    {"id": 2, "amount": 99.98, "status": "pending"},
                ]
            }
        }

    def test_empty_rows(self):
        columns = _make_columns([("id", None)])
        result = serialize_rows([], columns, "orders")
        assert result == {"data": {"orders": []}}

    def test_integer_decimal(self):
        """Decimal with no fractional part converts to int."""
        columns = _make_columns([("count", None)])
        rows = [(Decimal("42"),)]
        result = serialize_rows(rows, columns, "stats")
        assert result["data"]["stats"][0]["count"] == 42
        assert isinstance(result["data"]["stats"][0]["count"], int)


class TestSerializeNestedRows:
    def test_many_to_one_nested(self):
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
            ColumnRef(alias="t1", column="email", field_name="email", nested_in="customers"),
        ]
        rows = [
            (1, Decimal("19.99"), "Alice", "alice@example.com"),
            (2, Decimal("29.99"), "Bob", "bob@example.com"),
        ]
        result = serialize_rows(rows, columns, "orders")
        assert result["data"]["orders"][0]["customers"] == {
            "name": "Alice",
            "email": "alice@example.com",
        }

    def test_null_relationship(self):
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
        ]
        rows = [
            (1, "Alice"),
            (2, None),
        ]
        result = serialize_rows(rows, columns, "orders")
        assert result["data"]["orders"][0]["customers"] == {"name": "Alice"}
        assert result["data"]["orders"][1]["customers"] is None

    def test_multiple_nested_groups(self):
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customer"),
            ColumnRef(alias="t2", column="name", field_name="name", nested_in="product"),
        ]
        rows = [(1, "Alice", "Widget")]
        result = serialize_rows(rows, columns, "orders")
        row = result["data"]["orders"][0]
        assert row["customer"] == {"name": "Alice"}
        assert row["product"] == {"name": "Widget"}


class TestSerializeOneToMany:
    def test_one_to_many_groups_rows(self):
        """One-to-many relationship: multiple child rows collapsed under one parent."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t0", column="name", field_name="name", nested_in=None),
            ColumnRef(
                alias="t1",
                column="id",
                field_name="id",
                nested_in="orders",
                cardinality="one-to-many",
            ),
            ColumnRef(
                alias="t1",
                column="amount",
                field_name="amount",
                nested_in="orders",
                cardinality="one-to-many",
            ),
        ]
        rows = [
            (1, "Alice", 10, 100),
            (1, "Alice", 11, 200),
            (1, "Alice", 12, 300),
        ]
        result = serialize_rows(rows, columns, "customers")
        data = result["data"]["customers"]
        assert len(data) == 1
        assert data[0]["id"] == 1
        assert data[0]["name"] == "Alice"
        assert len(data[0]["orders"]) == 3
        assert {"id": 10, "amount": 100} in data[0]["orders"]
        assert {"id": 11, "amount": 200} in data[0]["orders"]
        assert {"id": 12, "amount": 300} in data[0]["orders"]

    def test_one_to_many_no_children_returns_empty_list(self):
        """LEFT JOIN with no matching child rows returns [] not null."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(
                alias="t1",
                column="id",
                field_name="id",
                nested_in="orders",
                cardinality="one-to-many",
            ),
        ]
        rows = [(1, None)]
        result = serialize_rows(rows, columns, "customers")
        data = result["data"]["customers"]
        assert len(data) == 1
        assert data[0]["orders"] == []

    def test_one_to_many_multiple_parents(self):
        """Multiple parents each with their own children."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(
                alias="t1",
                column="oid",
                field_name="oid",
                nested_in="orders",
                cardinality="one-to-many",
            ),
        ]
        rows = [
            (1, 10),
            (1, 11),
            (2, None),
            (3, 20),
        ]
        result = serialize_rows(rows, columns, "customers")
        data = result["data"]["customers"]
        assert len(data) == 3
        c1 = next(r for r in data if r["id"] == 1)
        assert c1["orders"] == [{"oid": 10}, {"oid": 11}]
        c2 = next(r for r in data if r["id"] == 2)
        assert c2["orders"] == []
        c3 = next(r for r in data if r["id"] == 3)
        assert c3["orders"] == [{"oid": 20}]


class TestSerializeMixedNesting:
    def test_many_to_one_with_nested_one_to_many_and_sibling_one_to_many(self):
        """_meta (many-to-one) with _meta.tableColumns (one-to-many) alongside _o__traces (one-to-many).

        Bug: _meta was incorrectly treated as one-to-many because its child
        _meta.tableColumns has cardinality=one-to-many, causing _meta to be [] not {}.
        """
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None, cardinality=None),
            ColumnRef(
                alias="t0", column="name", field_name="name", nested_in=None, cardinality=None
            ),
            ColumnRef(
                alias="t1",
                column="alias",
                field_name="alias",
                nested_in="_meta",
                cardinality="many-to-one",
            ),
            ColumnRef(
                alias="t1",
                column="table_name",
                field_name="tableName",
                nested_in="_meta",
                cardinality="many-to-one",
            ),
            ColumnRef(
                alias="t2",
                column="column_name",
                field_name="columnName",
                nested_in="_meta.tableColumns",
                cardinality="one-to-many",
            ),
            ColumnRef(
                alias="t2",
                column="data_type",
                field_name="dataType",
                nested_in="_meta.tableColumns",
                cardinality="one-to-many",
            ),
            ColumnRef(
                alias="t3",
                column="trace_name",
                field_name="traceName",
                nested_in="_o__traces",
                cardinality="one-to-many",
            ),
        ]
        # SQL cross-product: 1 root × 2 tableColumns × 2 traces = 4 rows
        rows = [
            (1, "buddy", "ps__pets", "pets", "id", "int4", "trace1"),
            (1, "buddy", "ps__pets", "pets", "name", "varchar", "trace1"),
            (1, "buddy", "ps__pets", "pets", "id", "int4", "trace2"),
            (1, "buddy", "ps__pets", "pets", "name", "varchar", "trace2"),
        ]
        result = serialize_rows(rows, columns, "pets")
        data = result["data"]["pets"]
        assert len(data) == 1
        row = data[0]
        assert row["id"] == 1
        assert row["name"] == "buddy"
        # _meta must be a dict (many-to-one), not a list
        assert isinstance(row["_meta"], dict), f"_meta should be dict, got {type(row['_meta'])}"
        assert row["_meta"]["alias"] == "ps__pets"
        assert row["_meta"]["tableName"] == "pets"
        # tableColumns inside _meta must be a list with 2 unique entries
        tc = row["_meta"]["tableColumns"]
        assert isinstance(tc, list), f"tableColumns should be list, got {type(tc)}"
        assert len(tc) == 2
        assert {"columnName": "id", "dataType": "int4"} in tc
        assert {"columnName": "name", "dataType": "varchar"} in tc
        # _o__traces must be a list with 2 unique entries
        traces = row["_o__traces"]
        assert isinstance(traces, list), f"_o__traces should be list, got {type(traces)}"
        assert len(traces) == 2
        assert {"traceName": "trace1"} in traces
        assert {"traceName": "trace2"} in traces

    def test_many_to_one_with_nested_one_to_many_no_sibling(self):
        """_meta (many-to-one) with _meta.tableColumns (one-to-many), no other joins."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None, cardinality=None),
            ColumnRef(
                alias="t1",
                column="alias",
                field_name="alias",
                nested_in="_meta",
                cardinality="many-to-one",
            ),
            ColumnRef(
                alias="t2",
                column="column_name",
                field_name="columnName",
                nested_in="_meta.tableColumns",
                cardinality="one-to-many",
            ),
        ]
        rows = [
            (1, "ps__pets", "id"),
            (1, "ps__pets", "name"),
        ]
        result = serialize_rows(rows, columns, "pets")
        data = result["data"]["pets"]
        assert len(data) == 1
        row = data[0]
        assert isinstance(row["_meta"], dict)
        assert row["_meta"]["alias"] == "ps__pets"
        tc = row["_meta"]["tableColumns"]
        assert isinstance(tc, list)
        assert len(tc) == 2


class TestManyToOneDeduplication:
    def test_duplicate_root_rows_deduplicated(self):
        """Flat path: duplicate root keys are deduplicated; only first row kept."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t0", column="name", field_name="name", nested_in=None),
            ColumnRef(alias="t1", column="last_name", field_name="lastName", nested_in="employee"),
        ]
        # pet id=1 appears twice because multiple assignments matched breed_name
        rows = [
            (1, "Buddy", "Smith"),
            (1, "Buddy", "Jones"),
            (2, "Rex", "Smith"),
        ]
        result = serialize_rows(rows, columns, "pets")
        data = result["data"]["pets"]
        assert len(data) == 2
        buddy = next(r for r in data if r["id"] == 1)
        assert buddy["employee"] == {"lastName": "Smith"}

    def test_duplicate_root_rows_emits_warning(self):
        """When rows are deduplicated, extensions.warnings is populated."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="last_name", field_name="lastName", nested_in="employee"),
        ]
        rows = [
            (1, "Smith"),
            (1, "Jones"),
        ]
        result = serialize_rows(rows, columns, "pets")
        warnings = result.get("extensions", {}).get("warnings", [])
        assert len(warnings) == 1
        assert "employee" in warnings[0]["message"]
        assert "many-to-one" in warnings[0]["message"]
        assert warnings[0]["path"] == "employee"

    def test_no_warning_when_no_duplicates(self):
        """No extensions.warnings key when every root key is unique."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customer"),
        ]
        rows = [(1, "Alice"), (2, "Bob")]
        result = serialize_rows(rows, columns, "orders")
        assert "extensions" not in result

    def test_no_warning_when_nested_values_identical(self):
        """Duplicate root rows with identical nested values emit no warning."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customer"),
        ]
        rows = [(1, "Alice"), (1, "Alice")]
        result = serialize_rows(rows, columns, "orders")
        data = result["data"]["orders"]
        assert len(data) == 1
        assert "extensions" not in result


class TestDeepManyToMany:
    """Deep one-to-many → one-to-many nesting (many-to-many via intermediate table).

    SQL structure: outer join is flat (one row per level-1 child); inner join
    uses ARRAY_AGG so level-2 values arrive as arrays on each level-1 row.

    Columns:
      id            (nested_in=None)
      orders.id     (nested_in="orders",       cardinality="one-to-many")
      items.id      (nested_in="orders.items", cardinality="one-to-many")  ← array values
      items.price   (nested_in="orders.items", cardinality="one-to-many")  ← array values
    """

    @staticmethod
    def _columns():
        return [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None, cardinality=None),
            ColumnRef(
                alias="t1",
                column="oid",
                field_name="id",
                nested_in="orders",
                cardinality="one-to-many",
            ),
            ColumnRef(
                alias="t2",
                column="iid",
                field_name="id",
                nested_in="orders.items",
                cardinality="one-to-many",
            ),
            ColumnRef(
                alias="t2",
                column="price",
                field_name="price",
                nested_in="orders.items",
                cardinality="one-to-many",
            ),
        ]

    def test_deep_nesting_produces_nested_arrays(self):
        """orders.items must be a list nested inside each order, not a flat list at root."""
        columns = self._columns()
        rows = [
            # (customer_id, order_id, item_ids_array,  item_prices_array)
            (1, 10, [100, 101], [9.99, 5.00]),
            (1, 11, [102], [15.00]),
        ]
        result = serialize_rows(rows, columns, "customers")
        data = result["data"]["customers"]
        assert len(data) == 1, f"Expected 1 customer, got {len(data)}"

        customer = data[0]
        assert customer["id"] == 1
        orders = customer["orders"]
        assert isinstance(orders, list), f"orders should be list, got {type(orders)}"
        assert len(orders) == 2, f"Expected 2 orders, got {len(orders)}"

        order10 = next(o for o in orders if o["id"] == 10)
        assert isinstance(order10["items"], list)
        assert {"id": 100, "price": 9.99} in order10["items"]
        assert {"id": 101, "price": 5.00} in order10["items"]

        order11 = next(o for o in orders if o["id"] == 11)
        assert isinstance(order11["items"], list)
        assert {"id": 102, "price": 15.00} in order11["items"]

    def test_deep_nesting_null_inner_produces_empty_list(self):
        """When no items match an order, items must be [] not null."""
        columns = self._columns()
        rows = [(2, 12, None, None)]
        result = serialize_rows(rows, columns, "customers")
        data = result["data"]["customers"]
        assert len(data) == 1
        order = data[0]["orders"][0]
        assert order["id"] == 12
        assert order["items"] == [], f"Expected empty list, got {order['items']!r}"

    def test_deep_nesting_multiple_customers(self):
        """Each customer's items are scoped to their own orders only."""
        columns = self._columns()
        rows = [
            (1, 10, [100], [9.99]),
            (2, 20, [200, 201], [1.00, 2.00]),
            (2, 21, None, None),
        ]
        result = serialize_rows(rows, columns, "customers")
        data = result["data"]["customers"]
        assert len(data) == 2

        c1 = next(c for c in data if c["id"] == 1)
        assert len(c1["orders"]) == 1
        assert c1["orders"][0]["items"] == [{"id": 100, "price": 9.99}]

        c2 = next(c for c in data if c["id"] == 2)
        assert len(c2["orders"]) == 2
        o20 = next(o for o in c2["orders"] if o["id"] == 20)
        assert len(o20["items"]) == 2
        o21 = next(o for o in c2["orders"] if o["id"] == 21)
        assert o21["items"] == []

    def test_items_not_duplicated_across_orders(self):
        """Items from order 10 must not appear in order 11 and vice versa."""
        columns = self._columns()
        rows = [
            (1, 10, [100], [9.99]),
            (1, 11, [200], [5.00]),
        ]
        result = serialize_rows(rows, columns, "customers")
        orders = result["data"]["customers"][0]["orders"]
        o10 = next(o for o in orders if o["id"] == 10)
        o11 = next(o for o in orders if o["id"] == 11)
        assert [i["id"] for i in o10["items"]] == [100]
        assert [i["id"] for i in o11["items"]] == [200]


class TestOneToManyWithAbsorbedManyToOne:
    """One-to-many (ARRAY_AGG) where a nested many-to-one relationship is also ARRAY_AGG'd
    at the parent level (e.g. ps__pets.assignment.employee).

    SQL emits two parallel ARRAY_AGG correlated subqueries per pet row:
      assignment__breedName            → is_agg=True, cardinality="one-to-many"
      assignment__employee__lastName   → is_agg=True, cardinality="many-to-one"

    Each index j in breedName corresponds to index j in lastName.
    shape_transform collapses employee from [{...}] to {...} afterward.
    """

    @staticmethod
    def _columns():
        return [
            ColumnRef(alias="t0", column="name", field_name="name", nested_in=None),
            ColumnRef(
                alias="t0",
                column="breed_name",
                field_name="breedName",
                nested_in=None,
            ),
            ColumnRef(
                alias="t1",
                column="breedName",
                field_name="breedName",
                nested_in="assignment",
                cardinality="one-to-many",
                is_agg=True,
            ),
            ColumnRef(
                alias="t2",
                column="lastName",
                field_name="lastName",
                nested_in="assignment.employee",
                cardinality="many-to-one",
                is_agg=True,
            ),
        ]

    def test_employee_zipped_into_assignment_element(self):
        """Each assignment element must contain its paired employee object."""
        from provisa.executor.serialize import shape_transform

        columns = self._columns()
        rows = [("Luna", "Siamese", ["Siamese"], ["Smith"])]
        result = serialize_rows(rows, columns, "ps__pets")
        result = shape_transform(result, columns)
        data = result["data"]["ps__pets"]
        assert len(data) == 1
        pet = data[0]
        assert pet["name"] == "Luna"
        assignments = pet["assignment"]
        assert isinstance(assignments, list)
        assert len(assignments) == 1
        a = assignments[0]
        assert a["breedName"] == "Siamese"
        assert isinstance(a["employee"], dict), f"employee should be dict, got {a['employee']!r}"
        assert a["employee"]["lastName"] == "Smith"

    def test_multiple_assignments_zipped_correctly(self):
        """Two parallel ARRAY_AGG arrays zip to two assignment elements."""
        from provisa.executor.serialize import shape_transform

        columns = self._columns()
        rows = [("Bella", "Maine Coon", ["Maine Coon", "Maine Coon"], ["Jones", "Adams"])]
        result = serialize_rows(rows, columns, "ps__pets")
        result = shape_transform(result, columns)
        assignments = result["data"]["ps__pets"][0]["assignment"]
        assert len(assignments) == 2
        last_names = {a["employee"]["lastName"] for a in assignments}
        assert last_names == {"Jones", "Adams"}

    def test_null_employee_in_assignment(self):
        """When employee data is None for an assignment, employee field is None."""
        from provisa.executor.serialize import shape_transform

        columns = self._columns()
        rows = [("Rex", "Golden Retriever", ["Golden Retriever"], [None])]
        result = serialize_rows(rows, columns, "ps__pets")
        result = shape_transform(result, columns)
        a = result["data"]["ps__pets"][0]["assignment"][0]
        assert a["employee"] is None

    def test_no_assignments_returns_empty_list(self):
        """When ARRAY_AGG returns NULL (no rows), assignment must be []."""
        from provisa.executor.serialize import shape_transform

        columns = self._columns()
        rows = [("Ghost", "Sphynx", None, None)]
        result = serialize_rows(rows, columns, "ps__pets")
        result = shape_transform(result, columns)
        pet = result["data"]["ps__pets"][0]
        assert pet["assignment"] == []


class TestSerializeNullIntermediatePath:
    def test_null_parent_does_not_crash_deep_path(self):
        """When a.b is None, processing a.b.c must not raise TypeError."""
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="a.b"),
            ColumnRef(alias="t2", column="val", field_name="val", nested_in="a.b.c"),
        ]
        rows = [(1, None, None)]
        result = serialize_rows(rows, columns, "items")
        row = result["data"]["items"][0]
        assert row["id"] == 1
        assert row["a"]["b"] is None


class TestSerializeAggregate:
    def test_aggregate_with_nodes(self):
        """serialize_aggregate merges aggregate row and nodes rows into correct shape."""
        agg_columns = [
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
        ]
        nodes_columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        agg_rows = [(Decimal("3"),)]
        nodes_rows = [(1, Decimal("10.00")), (2, Decimal("20.00")), (3, Decimal("30.00"))]

        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=nodes_rows,
            nodes_columns=nodes_columns,
            root_field="orders_aggregate",
        )
        data = result["data"]["orders_aggregate"]
        assert data["aggregate"]["count"] == 3
        assert len(data["nodes"]) == 3
        assert data["nodes"][0] == {"id": 1, "amount": 10.0}
        assert data["nodes"][2] == {"id": 3, "amount": 30.0}

    def test_aggregate_without_nodes(self):
        """serialize_aggregate with no nodes rows returns empty nodes list."""
        agg_columns = [
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
        ]
        agg_rows = [(Decimal("5"),)]
        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=None,
            nodes_columns=None,
            root_field="orders_aggregate",
        )
        data = result["data"]["orders_aggregate"]
        assert data["aggregate"]["count"] == 5
        assert "nodes" not in data

    def test_aggregate_sum_nested(self):
        """serialize_aggregate handles nested aggregate paths like aggregate.sum."""
        agg_columns = [
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in="aggregate.sum"),
        ]
        nodes_columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ]
        agg_rows = [(Decimal("2"), Decimal("50.00"))]
        nodes_rows = [(1,), (2,)]
        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=nodes_rows,
            nodes_columns=nodes_columns,
            root_field="orders_aggregate",
        )
        data = result["data"]["orders_aggregate"]
        assert data["aggregate"]["count"] == 2
        assert data["aggregate"]["sum"]["amount"] == 50.0
        assert len(data["nodes"]) == 2

    def test_aggregate_aliases_at_all_levels(self):
        """Aliases on aggregate/func/column keys appear in the serialized response."""
        agg_columns = [
            # derived: aggregate / total: sum / tickets: ticket_id
            ColumnRef(
                alias=None, column="ticket_id", field_name="tickets", nested_in="derived.total"
            ),
            ColumnRef(
                alias=None, column="customer_id", field_name="customers", nested_in="derived.total"
            ),
        ]
        agg_rows = [(42, 7)]
        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=None,
            nodes_columns=None,
            root_field="test",
            agg_alias="derived",
        )
        data = result["data"]["test"]
        assert "derived" in data
        assert data["derived"]["total"]["tickets"] == 42
        assert data["derived"]["total"]["customers"] == 7
