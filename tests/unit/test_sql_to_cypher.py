# Copyright (c) 2026 Kenneth Stott
# Canary: eea232ed-8ab8-4723-9703-0485141e62ac
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/sql_to_cypher.py — semantic SQL → Cypher."""

from dataclasses import dataclass, field


from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher


# ---------------------------------------------------------------------------
# Minimal stubs for CompilationContext / TableMeta
# ---------------------------------------------------------------------------

@dataclass
class _TableMeta:
    table_id: int
    field_name: str
    type_name: str
    source_id: str
    catalog_name: str
    schema_name: str
    table_name: str
    domain_id: str = ""
    column_presets: list = field(default_factory=list)
    source_type: str = ""


@dataclass
class _Ctx:
    tables: dict = field(default_factory=dict)
    joins: dict = field(default_factory=dict)
    aggregate_columns: dict = field(default_factory=dict)
    pk_columns: dict = field(default_factory=dict)


def _make_simple_ctx_and_label_map():
    """Single table, no domain prefix in field_name."""
    meta = _TableMeta(
        table_id=1, field_name="persons", type_name="Person",
        source_id="pg-main", catalog_name="postgresql",
        schema_name="public", table_name="persons",
        domain_id="public",
    )
    ctx = _Ctx(
        tables={"persons": meta},
        aggregate_columns={1: [("id", "integer"), ("name", "varchar")]},
    )
    node = NodeMapping(
        label="Person", type_name="Person", domain_label=None,
        table_label="Person", table_id=1, source_id="pg-main",
        id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="public", table_name="persons",
        properties={"id": "id", "name": "name"},
    )
    lm = CypherLabelMap(nodes={"Person": node}, relationships={})
    return ctx, lm


def _make_prefixed_ctx_and_label_map():
    """Single table where field_name has domain prefix (sa__orders style).

    _semantic_table_ref strips the prefix: "sales_analytics"."orders"
    domain_to_label must look up ("sales_analytics", "orders") not
    ("sales_analytics", "sa__orders").
    """
    meta = _TableMeta(
        table_id=2, field_name="sa__orders", type_name="Sa_Orders",
        source_id="pg-main", catalog_name="postgresql",
        schema_name="sales_analytics", table_name="sa_orders",
        domain_id="sales_analytics",
    )
    ctx = _Ctx(
        tables={"sa__orders": meta},
        aggregate_columns={2: [("id", "integer"), ("amount", "float")]},
    )
    node = NodeMapping(
        label="SalesAnalytics:Orders", type_name="Sa_Orders",
        domain_label="SalesAnalytics", table_label="Orders",
        table_id=2, source_id="pg-main", id_column="id", pk_columns=[],
        catalog_name="postgresql", schema_name="sales_analytics",
        table_name="orders",
        properties={"id": "id", "amount": "amount"},
    )
    lm = CypherLabelMap(nodes={"Sa_Orders": node}, relationships={})
    return ctx, lm


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSimpleTable:
    def test_simple_select_produces_match(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "persons"."name" FROM "public"."persons"'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "MATCH" in result
        assert "Person" in result
        assert "RETURN" in result

    def test_returns_none_for_non_select(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        result = semantic_sql_to_cypher("UPDATE persons SET name = 'x'", lm, ctx)
        assert result is None

    def test_where_clause_translated(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "persons"."name" FROM "public"."persons" WHERE "persons"."id" = 1'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "WHERE" in result

    def test_limit_produces_cypher_limit(self):
        """Regression: LIMIT in semantic SQL must emit LIMIT in Cypher (not crash)."""
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "id", "name" FROM "public"."persons" LIMIT 10000'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "LIMIT 10000" in result

    def test_limit_and_offset(self):
        ctx, lm = _make_simple_ctx_and_label_map()
        sql = 'SELECT "id" FROM "public"."persons" LIMIT 50 OFFSET 10'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "LIMIT 50" in result
        assert "SKIP 10" in result

    def test_override_limit_replaces_sql_cap(self):
        """Regression #42: override_limit must replace the SQL safety-cap LIMIT."""
        ctx, lm = _make_simple_ctx_and_label_map()
        # SQL has the default safety cap (10000), but user supplied limit=1
        sql = 'SELECT "id", "name" FROM "public"."persons" LIMIT 10000'
        result = semantic_sql_to_cypher(sql, lm, ctx, override_limit=1)
        assert result is not None
        assert "LIMIT 1" in result
        assert "LIMIT 10000" not in result


class TestDomainPrefixedFieldName:
    """Regression: field_name with __ prefix must still resolve in domain_to_label."""

    def test_prefixed_field_name_resolves(self):
        """semantic_sql_to_cypher must not return None when field_name has domain prefix.

        The semantic SQL for sa__orders uses "sales_analytics"."orders" as the table
        reference (domain prefix stripped by _semantic_table_ref). The domain_to_label
        dict must be keyed on "orders", not "sa__orders".
        """
        ctx, lm = _make_prefixed_ctx_and_label_map()
        # Semantic SQL uses the stripped name ("orders"), not "sa__orders"
        sql = 'SELECT "sa_orders"."amount" FROM "sales_analytics"."orders"'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, (
            "semantic_sql_to_cypher returned None for domain-prefixed field_name — "
            "domain_to_label key mismatch between '__'-prefixed field_name and "
            "stripped semantic SQL table name"
        )
        assert "SalesAnalytics" in result or "Orders" in result

    def test_prefixed_field_name_cypher_has_match(self):
        ctx, lm = _make_prefixed_ctx_and_label_map()
        sql = 'SELECT "sa_orders"."amount" FROM "sales_analytics"."orders"'
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "MATCH" in result
        assert "RETURN" in result


class TestTraversalOnlyNode:
    """Regression: traversal-only nodes (in label_map.nodes but NOT in ctx.tables) must resolve."""

    def _make_ctx_and_label_map(self):
        # ctx only contains the 'pets' table — user has access to it
        pets_meta = _TableMeta(
            table_id=1, field_name="pets", type_name="Pets",
            source_id="pg-main", catalog_name="postgresql",
            schema_name="public", table_name="pets",
            domain_id="public",
        )
        ctx = _Ctx(
            tables={"pets": pets_meta},
            aggregate_columns={1: [("id", "integer"), ("name", "varchar")]},
        )
        pets_node = NodeMapping(
            label="Pets", type_name="Pets", domain_label=None,
            table_label="Pets", table_id=1, source_id="pg-main",
            id_column="id", pk_columns=[],
            catalog_name="postgresql", schema_name="public", table_name="pets",
            properties={"id": "id", "name": "name"},
        )
        # Traversal-only node for ops.spans — not in ctx.tables
        spans_node = NodeMapping(
            label="Ops:Traces", type_name="Ops_Traces", domain_label="Ops",
            table_label="Traces", table_id=99, source_id="ops",
            id_column="span_id", pk_columns=[],
            catalog_name="ops", schema_name="ops", table_name="spans",
            properties={"serviceName": "service_name", "spanId": "span_id"},
            traversal_only=True,
            domain_id="ops",
        )
        rel = RelationshipMapping(
            rel_type="HAS_TRACES",
            source_label="Pets",
            target_label="Ops_Traces",
            join_source_column="id",
            join_target_column="pet_id",
            field_name="_traces",
        )
        lm = CypherLabelMap(
            nodes={"Pets": pets_node, "Ops_Traces": spans_node},
            relationships={"HAS_TRACES::Pets→Ops_Traces": rel},
            nodes_by_table={"Pets": ["Pets"], "Traces": ["Ops_Traces"]},
        )
        return ctx, lm

    def test_lateral_join_to_traversal_node_does_not_return_none(self):
        """LATERAL join to a traversal-only node must not produce None (no crash)."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name", "t3"."span_id" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 10) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, (
            "semantic_sql_to_cypher returned None for LATERAL join to traversal-only node — "
            "traversal-only nodes must be added to domain_to_label even if not in ctx.tables"
        )
        assert "MATCH" in result
        assert "OPTIONAL MATCH" in result
        assert "RETURN" in result

    def test_lateral_join_return_uses_camel_case(self):
        """Properties from traversal-only nodes must use camelCase in RETURN."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 10) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "serviceName" in result
        assert "service_name" not in result

    def test_lateral_join_emits_typed_relationship(self):
        """Regression #41: OPTIONAL MATCH must emit [:HAS_TRACES], not anonymous []."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name", "t3"."span_id" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 10) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "[:HAS_TRACES]" in result, (
            f"Expected [:HAS_TRACES] in Cypher but got anonymous []: {result}"
        )
        assert "-[]->" not in result, f"Anonymous relationship emitted: {result}"

    def test_lateral_join_with_limit_emits_call_subquery(self):
        """Regression #43: LATERAL join with LIMIT N must emit CALL {} with inner LIMIT N."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 2) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, f"Got None: {result}"
        assert "CALL {" in result, f"Expected CALL subquery for per-relationship LIMIT: {result}"
        assert "[..2]" in result, f"Per-relationship LIMIT 2 (as slice) missing: {result}"
        assert "WITH " in result, f"CALL subquery must include WITH clause: {result}"

    def test_lateral_join_parameterized_limit_resolves_call_subquery(self):
        """Regression: LATERAL LIMIT emitted as $N placeholder must resolve via params list."""
        ctx, lm = self._make_ctx_and_label_map()
        # sql_gen emits LIMIT $N (parameterized), not LIMIT 3 (literal)
        sql = (
            'SELECT "pets"."name", "t3"."service_name" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT $1) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx, params=[3])
        assert result is not None, f"Got None: {result}"
        assert "CALL {" in result, f"Expected CALL subquery: {result}"
        assert "[..3]" in result, f"Expected resolved limit [..3]: {result}"

    def test_lateral_join_collect_avoids_cartesian_product(self):
        """Regression #44: multiple LATERAL joins must use collect() + list comprehension, not flat OPTIONAL MATCH."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 5) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "collect(" in result, f"Expected collect() in CALL subquery to avoid cartesian product: {result}"
        # Per-property collect: list comprehensions replaced by direct list var references (issue #49)
        assert "_list AS " in result, f"Expected per-property list var in RETURN: {result}"
        assert "OPTIONAL MATCH" not in result.split("CALL")[0].lstrip("MATCH"), (
            f"OPTIONAL MATCH outside CALL block would cause cartesian product: {result}"
        )

    def test_collect_uses_property_not_node_alias(self):
        """Regression #49: collect() must target a property (e.g. b.serviceName), never a bare
        node alias (e.g. collect(b)), which Trino rejects as ARRAY_AGG(table_alias)."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "t3"."service_name" '
            'FROM "public"."pets" '
            'LEFT JOIN LATERAL (SELECT * FROM "ops"."spans" WHERE "ops"."spans"."pet_id" = "pets"."id" LIMIT 2) "t3" ON TRUE'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        # collect(b) would be ARRAY_AGG(table_alias) — invalid in Trino
        import re
        bare_collect = re.search(r'\bcollect\s*\(\s*[a-z]\s*\)', result)
        assert bare_collect is None, (
            f"Regression #49: collect() must not use bare node alias: {result}"
        )
        # collect(b.prop) is the correct form
        assert re.search(r'\bcollect\s*\(\s*[a-z]\s*\.\s*\w+\s*\)', result), (
            f"Regression #49: collect() must reference a property: {result}"
        )


class TestOneManyNonLateralJoin:
    """Regression #50: non-LATERAL one-to-many optional JOIN must use CALL+collect, not flat OPTIONAL MATCH."""

    def _make_ctx_and_label_map(self):
        pets_meta = _TableMeta(
            table_id=1, field_name="pets", type_name="Pets",
            source_id="pg-main", catalog_name="postgresql",
            schema_name="public", table_name="pets",
            domain_id="public",
        )
        reg_tables_meta = _TableMeta(
            table_id=2, field_name="registered_tables", type_name="RegisteredTables",
            source_id="pg-main", catalog_name="postgresql",
            schema_name="public", table_name="registered_tables",
            domain_id="public",
        )
        table_cols_meta = _TableMeta(
            table_id=3, field_name="table_columns", type_name="TableColumns",
            source_id="pg-main", catalog_name="postgresql",
            schema_name="public", table_name="table_columns",
            domain_id="public",
        )
        ctx = _Ctx(
            tables={"pets": pets_meta, "registered_tables": reg_tables_meta, "table_columns": table_cols_meta},
            aggregate_columns={
                1: [("id", "integer"), ("name", "varchar")],
                2: [("id", "integer"), ("alias", "varchar"), ("schema_name", "varchar")],
                3: [("id", "integer"), ("column_name", "varchar"), ("is_foreign_key", "boolean")],
            },
        )
        pets_node = NodeMapping(
            label="Pets", type_name="Pets", domain_label=None, table_label="Pets",
            table_id=1, source_id="pg-main", id_column="id", pk_columns=[],
            catalog_name="postgresql", schema_name="public", table_name="pets",
            properties={"id": "id", "name": "name"},
        )
        reg_node = NodeMapping(
            label="RegisteredTables", type_name="RegisteredTables", domain_label=None,
            table_label="RegisteredTables", table_id=2, source_id="pg-main",
            id_column="id", pk_columns=[],
            catalog_name="postgresql", schema_name="public", table_name="registered_tables",
            properties={"id": "id", "alias": "alias", "schemaName": "schema_name"},
        )
        col_node = NodeMapping(
            label="TableColumns", type_name="TableColumns", domain_label=None,
            table_label="TableColumns", table_id=3, source_id="pg-main",
            id_column="id", pk_columns=[],
            catalog_name="postgresql", schema_name="public", table_name="table_columns",
            properties={"id": "id", "columnName": "column_name", "isForeignKey": "is_foreign_key"},
        )
        has_table_rel = RelationshipMapping(
            rel_type="HAS_TABLE",
            source_label="Pets",
            target_label="RegisteredTables",
            join_source_column="id",
            join_target_column="table_id",
            field_name="_table",
            many=False,
        )
        has_cols_rel = RelationshipMapping(
            rel_type="HAS_TABLE_COLUMNS",
            source_label="RegisteredTables",
            target_label="TableColumns",
            join_source_column="id",
            join_target_column="table_id",
            field_name="tableColumns",
            many=True,
        )
        lm = CypherLabelMap(
            nodes={"Pets": pets_node, "RegisteredTables": reg_node, "TableColumns": col_node},
            relationships={
                "HAS_TABLE::Pets→RegisteredTables": has_table_rel,
                "HAS_TABLE_COLUMNS::RegisteredTables→TableColumns": has_cols_rel,
            },
        )
        return ctx, lm

    def test_one_to_many_join_emits_call_subquery(self):
        """Regression #50: one-to-many non-LATERAL JOIN must use CALL+collect, not flat OPTIONAL MATCH."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "registered_tables"."alias", "table_columns"."column_name" '
            'FROM "public"."pets" '
            'LEFT JOIN "public"."registered_tables" ON "registered_tables"."table_id" = "pets"."id" '
            'LEFT JOIN "public"."table_columns" ON "table_columns"."table_id" = "registered_tables"."id" '
            'LIMIT 1'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "CALL {" in result, f"Expected CALL subquery for one-to-many join: {result}"
        assert "collect(" in result, f"Expected collect() for TableColumns: {result}"
        assert "HAS_TABLE_COLUMNS" in result

    def test_many_to_one_join_stays_flat(self):
        """Regression #50: many-to-one JOIN (HAS_TABLE) must NOT use CALL+collect."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "registered_tables"."alias" '
            'FROM "public"."pets" '
            'LEFT JOIN "public"."registered_tables" ON "registered_tables"."table_id" = "pets"."id" '
            'LIMIT 1'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "OPTIONAL MATCH" in result, f"Expected flat OPTIONAL MATCH for many-to-one: {result}"
        assert "collect(" not in result, f"Unexpected collect() for many-to-one join: {result}"

    def test_one_to_many_return_uses_list_var(self):
        """Regression #50: RETURN for one-to-many must reference per-property list var, not scalar."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT "pets"."name", "table_columns"."column_name" '
            'FROM "public"."pets" '
            'LEFT JOIN "public"."registered_tables" ON "registered_tables"."table_id" = "pets"."id" '
            'LEFT JOIN "public"."table_columns" ON "table_columns"."table_id" = "registered_tables"."id" '
            'LIMIT 1'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert "_list AS " in result, f"Expected list var in RETURN: {result}"


class TestArrayAggChainedJoin:
    """Regression: ARRAY_AGG subquery with inner JOIN (col from JOIN target, not FROM table).

    Query shape:
      ps__pets { assignment { breedName employee { lastName } } name }
    Generates SQL like:
      (SELECT ARRAY_AGG(t1.breedName) FROM shelter__assignments AS t1 WHERE t1.petId = t0.id)
      (SELECT ARRAY_AGG(t2.lastName) FROM shelter__assignments AS t1
         JOIN shelter__employees AS t2 ON t2.id = t1.employeeId WHERE t1.petId = t0.id)

    Expected Cypher must include OPTIONAL MATCHes for both assignments and employees.
    """

    def _make_ctx_and_label_map(self):
        pets_meta = _TableMeta(
            table_id=1, field_name="shelter__pets", type_name="Pets",
            source_id="shelter-db", catalog_name="shelter",
            schema_name="shelter", table_name="pets",
            domain_id="shelter",
        )
        assignments_meta = _TableMeta(
            table_id=2, field_name="shelter__assignments", type_name="Assignments",
            source_id="shelter-db", catalog_name="shelter",
            schema_name="shelter", table_name="assignments",
            domain_id="shelter",
        )
        employees_meta = _TableMeta(
            table_id=3, field_name="shelter__employees", type_name="Employees",
            source_id="shelter-db", catalog_name="shelter",
            schema_name="shelter", table_name="employees",
            domain_id="shelter",
        )
        ctx = _Ctx(
            tables={
                "shelter__pets": pets_meta,
                "shelter__assignments": assignments_meta,
                "shelter__employees": employees_meta,
            },
            aggregate_columns={
                1: [("id", "integer"), ("name", "varchar"), ("breed_name", "varchar")],
                2: [("id", "integer"), ("pet_id", "integer"), ("employee_id", "integer"), ("breed_name", "varchar")],
                3: [("id", "integer"), ("last_name", "varchar")],
            },
        )
        pets_node = NodeMapping(
            label="Pets", type_name="Pets", domain_label=None, table_label="Pets",
            table_id=1, source_id="shelter-db", id_column="id", pk_columns=[],
            catalog_name="shelter", schema_name="shelter", table_name="pets",
            properties={"id": "id", "name": "name", "breedName": "breed_name"},
        )
        assignments_node = NodeMapping(
            label="Assignments", type_name="Assignments", domain_label=None,
            table_label="Assignments", table_id=2, source_id="shelter-db",
            id_column="id", pk_columns=[],
            catalog_name="shelter", schema_name="shelter", table_name="assignments",
            properties={"id": "id", "breedName": "breed_name"},
        )
        employees_node = NodeMapping(
            label="Employees", type_name="Employees", domain_label=None,
            table_label="Employees", table_id=3, source_id="shelter-db",
            id_column="id", pk_columns=[],
            catalog_name="shelter", schema_name="shelter", table_name="employees",
            properties={"id": "id", "lastName": "last_name"},
        )
        is_assignment_rel = RelationshipMapping(
            rel_type="IS_ASSIGNMENT",
            source_label="Pets",
            target_label="Assignments",
            join_source_column="id",
            join_target_column="pet_id",
            field_name="assignment",
            many=True,
        )
        has_employee_rel = RelationshipMapping(
            rel_type="HAS_EMPLOYEE",
            source_label="Assignments",
            target_label="Employees",
            join_source_column="employee_id",
            join_target_column="id",
            field_name="employee",
            many=False,
        )
        lm = CypherLabelMap(
            nodes={"Pets": pets_node, "Assignments": assignments_node, "Employees": employees_node},
            relationships={
                "IS_ASSIGNMENT::Pets→Assignments": is_assignment_rel,
                "HAS_EMPLOYEE::Assignments→Employees": has_employee_rel,
            },
        )
        return ctx, lm

    def test_chained_array_agg_emits_two_optional_matches(self):
        """ARRAY_AGG(t2.col) FROM t1 JOIN t2 must emit OPTIONAL MATCH for t1 and t2."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT t0.name, '
            '(SELECT ARRAY_AGG(t1.breed_name) FROM "shelter"."assignments" AS t1 WHERE t1.pet_id = t0.id) AS "assignment__breedName", '
            '(SELECT ARRAY_AGG(t2.last_name) FROM "shelter"."assignments" AS t1 JOIN "shelter"."employees" AS t2 ON t2.id = t1.employee_id WHERE t1.pet_id = t0.id) AS "assignment__employee__lastName" '
            'FROM "shelter"."pets" AS t0 LIMIT 100'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, f"Got None — chained ARRAY_AGG subquery not handled: {result}"
        assert result.count("OPTIONAL MATCH") == 2, (
            f"Expected 2 OPTIONAL MATCHes (Assignments, Employees), got:\n{result}"
        )
        assert "Assignments" in result
        assert "Employees" in result

    def test_chained_array_agg_emits_relationship_types(self):
        """Both [:IS_ASSIGNMENT] and [:HAS_EMPLOYEE] must appear in the Cypher."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT t0.name, '
            '(SELECT ARRAY_AGG(t2.last_name) FROM "shelter"."assignments" AS t1 JOIN "shelter"."employees" AS t2 ON t2.id = t1.employee_id WHERE t1.pet_id = t0.id) AS "assignment__employee__lastName" '
            'FROM "shelter"."pets" AS t0 LIMIT 100'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None, f"Got None: {result}"
        assert "[:IS_ASSIGNMENT]" in result, f"Missing [:IS_ASSIGNMENT]: {result}"
        assert "[:HAS_EMPLOYEE]" in result, f"Missing [:HAS_EMPLOYEE]: {result}"

    def test_assignments_optional_match_emitted_once(self):
        """When both breedName and employee.lastName appear, Assignments OPTIONAL MATCH must appear once only."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT t0.name, '
            '(SELECT ARRAY_AGG(t1.breed_name) FROM "shelter"."assignments" AS t1 WHERE t1.pet_id = t0.id) AS "assignment__breedName", '
            '(SELECT ARRAY_AGG(t2.last_name) FROM "shelter"."assignments" AS t1 JOIN "shelter"."employees" AS t2 ON t2.id = t1.employee_id WHERE t1.pet_id = t0.id) AS "assignment__employee__lastName" '
            'FROM "shelter"."pets" AS t0 LIMIT 100'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx)
        assert result is not None
        assert result.count("Assignments") == 2, (
            f"Assignments must appear in exactly 2 OPTIONAL MATCHes, got:\n{result}"
        )

    def test_node_only_mode_includes_chained_aliases(self):
        """node_only=True must include the chained JOIN alias in RETURN."""
        ctx, lm = self._make_ctx_and_label_map()
        sql = (
            'SELECT t0.name, '
            '(SELECT ARRAY_AGG(t2.last_name) FROM "shelter"."assignments" AS t1 JOIN "shelter"."employees" AS t2 ON t2.id = t1.employee_id WHERE t1.pet_id = t0.id) AS "assignment__employee__lastName" '
            'FROM "shelter"."pets" AS t0 LIMIT 100'
        )
        result = semantic_sql_to_cypher(sql, lm, ctx, node_only=True)
        assert result is not None
        # node_only RETURN must include the employees alias (c or similar)
        lines = result.splitlines()
        return_line = next((l for l in lines if l.startswith("RETURN")), "")
        # Should have at least 3 aliases: base (a), assignments (b), employees (c)
        aliases_in_return = [p.strip() for p in return_line.replace("RETURN", "").split(",")]
        assert len(aliases_in_return) >= 3, (
            f"Expected 3 aliases in RETURN for pets+assignments+employees: {return_line}"
        )


class TestJsonAggChainedSubquery:
    """Regression: json_agg(json_object(...)) with nested json_object subquery.

    Query shape:
      ps__pets { assignment { breedName employee { lastName } } name }
    _build_rel_json_expr generates:
      (SELECT json_agg(_t) FROM
        (SELECT json_object(KEY 'breedName' VALUE t1."breed_name",
                            KEY 'employee' VALUE
                              (SELECT json_object(KEY 'lastName' VALUE t2."last_name")
                               FROM "shelter"."employees" AS t2
                               WHERE t2."id" = t1."employee_id" LIMIT 1))
         AS _t FROM "shelter"."assignments" AS t1
         WHERE t1."pet_id" = t0."id" LIMIT 10000) _sub)
    Expected: OPTIONAL MATCH (a:Pets)->[...]->(b:Assignments)
              OPTIONAL MATCH (b:Assignments)->[...]->(c:Employees)
    Bug (before fix): source node for Employees was (a:Pets) because t1 was not in alias_map.
    """

    def _make_ctx_and_label_map(self):
        pets_meta = _TableMeta(
            table_id=1, field_name="shelter__pets", type_name="Pets",
            source_id="shelter-db", catalog_name="shelter",
            schema_name="shelter", table_name="pets",
            domain_id="shelter",
        )
        assignments_meta = _TableMeta(
            table_id=2, field_name="shelter__assignments", type_name="Assignments",
            source_id="shelter-db", catalog_name="shelter",
            schema_name="shelter", table_name="assignments",
            domain_id="shelter",
        )
        employees_meta = _TableMeta(
            table_id=3, field_name="shelter__employees", type_name="Employees",
            source_id="shelter-db", catalog_name="shelter",
            schema_name="shelter", table_name="employees",
            domain_id="shelter",
        )
        ctx = _Ctx(
            tables={
                "shelter__pets": pets_meta,
                "shelter__assignments": assignments_meta,
                "shelter__employees": employees_meta,
            },
            aggregate_columns={
                1: [("id", "integer"), ("name", "varchar")],
                2: [("id", "integer"), ("pet_id", "integer"), ("employee_id", "integer"), ("breed_name", "varchar")],
                3: [("id", "integer"), ("last_name", "varchar")],
            },
        )
        pets_node = NodeMapping(
            label="Pets", type_name="Pets", domain_label=None, table_label="Pets",
            table_id=1, source_id="shelter-db", id_column="id", pk_columns=[],
            catalog_name="shelter", schema_name="shelter", table_name="pets",
            properties={"id": "id", "name": "name"},
        )
        assignments_node = NodeMapping(
            label="Assignments", type_name="Assignments", domain_label=None,
            table_label="Assignments", table_id=2, source_id="shelter-db",
            id_column="id", pk_columns=[],
            catalog_name="shelter", schema_name="shelter", table_name="assignments",
            properties={"id": "id", "breedName": "breed_name"},
        )
        employees_node = NodeMapping(
            label="Employees", type_name="Employees", domain_label=None,
            table_label="Employees", table_id=3, source_id="shelter-db",
            id_column="id", pk_columns=[],
            catalog_name="shelter", schema_name="shelter", table_name="employees",
            properties={"id": "id", "lastName": "last_name"},
        )
        is_assignment_rel = RelationshipMapping(
            rel_type="IS_ASSIGNMENT",
            source_label="Pets",
            target_label="Assignments",
            join_source_column="id",
            join_target_column="pet_id",
            field_name="assignment",
            many=True,
        )
        has_employee_rel = RelationshipMapping(
            rel_type="HAS_EMPLOYEE",
            source_label="Assignments",
            target_label="Employees",
            join_source_column="employee_id",
            join_target_column="id",
            field_name="employee",
            many=False,
        )
        lm = CypherLabelMap(
            nodes={"Pets": pets_node, "Assignments": assignments_node, "Employees": employees_node},
            relationships={
                "IS_ASSIGNMENT::Pets→Assignments": is_assignment_rel,
                "HAS_EMPLOYEE::Assignments→Employees": has_employee_rel,
            },
        )
        return ctx, lm

    def _sql(self) -> str:
        return (
            'SELECT t0."name",'
            ' (SELECT json_agg(_t) FROM'
            '  (SELECT json_object(KEY \'breedName\' VALUE t1."breed_name",'
            '                      KEY \'employee\' VALUE'
            '                        (SELECT json_object(KEY \'lastName\' VALUE t2."last_name")'
            '                         FROM "shelter"."employees" AS t2'
            '                         WHERE t2."id" = t1."employee_id" LIMIT 1))'
            '   AS _t FROM "shelter"."assignments" AS t1'
            '   WHERE t1."pet_id" = t0."id" LIMIT 10000) _sub) AS assignment'
            ' FROM "shelter"."pets" AS t0 LIMIT 10000'
        )

    def test_emits_two_optional_matches(self):
        """json_agg(json_object) with nested json_object must emit OPTIONAL MATCH for assignments and employees."""
        ctx, lm = self._make_ctx_and_label_map()
        result = semantic_sql_to_cypher(self._sql(), lm, ctx)
        assert result is not None, "semantic_sql_to_cypher returned None"
        assert result.count("OPTIONAL MATCH") == 2, (
            f"Expected 2 OPTIONAL MATCHes (Assignments, Employees), got:\n{result}"
        )
        assert "Assignments" in result
        assert "Employees" in result

    def test_employees_source_is_assignments_not_pets(self):
        """Bug: OPTIONAL MATCH for Employees must chain from Assignments, not Pets."""
        ctx, lm = self._make_ctx_and_label_map()
        result = semantic_sql_to_cypher(self._sql(), lm, ctx)
        assert result is not None, "semantic_sql_to_cypher returned None"
        lines = [l.strip() for l in result.splitlines() if "Employees" in l]
        assert lines, f"No line with Employees in:\n{result}"
        employees_line = lines[0]
        # The source node for Employees must be Assignments, not Pets
        assert "Assignments" in employees_line, (
            f"Employees OPTIONAL MATCH must chain from Assignments, not Pets. Got:\n{employees_line}"
        )
        assert employees_line.index("Assignments") < employees_line.index("Employees"), (
            f"Assignments must appear before Employees as source: {employees_line}"
        )

    def test_relationship_types_present(self):
        """Both IS_ASSIGNMENT and HAS_EMPLOYEE must appear in the generated Cypher."""
        ctx, lm = self._make_ctx_and_label_map()
        result = semantic_sql_to_cypher(self._sql(), lm, ctx)
        assert result is not None
        assert "IS_ASSIGNMENT" in result, f"Missing IS_ASSIGNMENT:\n{result}"
        assert "HAS_EMPLOYEE" in result, f"Missing HAS_EMPLOYEE:\n{result}"
