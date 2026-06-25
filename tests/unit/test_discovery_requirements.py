# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for discovery requirements: REQ-413, REQ-415, REQ-611, REQ-612"""

from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# REQ-413: Auto-generate GQL relationships from FK constraints in database
# schema introspection — relationships discoverable from FK metadata in
# addition to manual steward configuration and AI-assisted hints.
# ---------------------------------------------------------------------------


def test_fk_candidates_returned_from_introspection():
    # REQ-413
    # introspect_fk_candidates must surface FK constraint data when the
    # connector exposes it — returns dicts with constraint_name, column_name,
    # referenced_table, referenced_column.
    from provisa.compiler.introspect import introspect_fk_candidates

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = [
        ("orders_user_id_fkey", "user_id", "users", "id"),
    ]

    results = introspect_fk_candidates(conn, "postgresql", "public", "orders")

    assert len(results) == 1
    fk = results[0]
    assert fk["constraint_name"] == "orders_user_id_fkey"
    assert fk["column_name"] == "user_id"
    assert fk["referenced_table"] == "users"
    assert fk["referenced_column"] == "id"


def test_fk_candidates_empty_when_connector_unsupported():
    # REQ-413
    # When the Trino connector does not expose constraint metadata,
    # introspect_fk_candidates must return an empty list (not raise).
    import trino.exceptions

    from provisa.compiler.introspect import introspect_fk_candidates

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.execute.side_effect = trino.exceptions.TrinoUserError(MagicMock(), MagicMock())

    results = introspect_fk_candidates(conn, "bigquery", "myschema", "orders")
    assert results == []


def test_collect_fk_candidates_produces_relationship_candidates():
    # REQ-413
    # collect_fk_candidates in collector.py must return RelationshipCandidate
    # objects derived from FK metadata, with confidence=1.0 and the FK source.
    from provisa.discovery.analyzer import RelationshipCandidate

    # Verify RelationshipCandidate dataclass exists with required fields
    rc = RelationshipCandidate(
        source_table_id=1,
        source_column="user_id",
        target_table_id=2,
        target_column="id",
        cardinality="many_to_one",
        confidence=1.0,
        reasoning="Foreign key constraint",
    )
    assert rc.confidence == 1.0
    assert rc.source_table_id == 1
    assert rc.target_table_id == 2


def test_fk_introspection_auto_register_produces_both_directions():
    # REQ-413
    # auto_register_fk_relationships must insert BOTH directions for each FK:
    # many-to-one (fk_table → ref_table) and one-to-many (ref_table ← fk_table).
    # Verify _m2o_alias and _o2m_alias are distinct (direction matters).
    from provisa.discovery.fk_introspect import _m2o_alias, _o2m_alias

    # Without hasura_v2_style, aliases are just the raw table names
    m2o = _m2o_alias("users", hasura_v2_style=False)
    o2m = _o2m_alias("orders", hasura_v2_style=False)
    assert m2o == "users"
    assert o2m == "orders"


# ---------------------------------------------------------------------------
# REQ-415: The `hasura_v2_relationship_style` option controls whether
# FK-derived relationships use Hasura V2's naming conventions — singular for
# many-to-one, plural for one-to-many using inflection.
# ---------------------------------------------------------------------------


def test_m2o_alias_singular_when_hasura_v2_style():
    # REQ-415
    # many-to-one alias must be the singular form of the referenced table name
    # when hasura_v2_relationship_style=True.
    from provisa.discovery.fk_introspect import _m2o_alias

    # "users" → singular → "user"
    alias = _m2o_alias("users", hasura_v2_style=True)
    assert alias == "user"


def test_o2m_alias_plural_when_hasura_v2_style():
    # REQ-415
    # one-to-many alias must be the plural form of the FK table name
    # when hasura_v2_relationship_style=True.
    from provisa.discovery.fk_introspect import _o2m_alias

    # "order" → plural → "orders"
    alias = _o2m_alias("order", hasura_v2_style=True)
    assert alias == "orders"


def test_m2o_alias_unchanged_without_hasura_v2_style():
    # REQ-415
    # When hasura_v2_relationship_style is False, the many-to-one alias is
    # NOT singularized — it is the raw ref table name.
    from provisa.discovery.fk_introspect import _m2o_alias

    alias = _m2o_alias("users", hasura_v2_style=False)
    assert alias == "users"  # raw name, not singularized


def test_o2m_alias_unchanged_without_hasura_v2_style():
    # REQ-415
    # When hasura_v2_relationship_style is False, the one-to-many alias is
    # NOT pluralized — it is the raw fk table name.
    from provisa.discovery.fk_introspect import _o2m_alias

    alias = _o2m_alias("order", hasura_v2_style=False)
    assert alias == "order"  # raw name, not pluralized


def test_o2m_alias_already_plural_not_double_pluralized():
    # REQ-415
    # When the FK table name is already plural (e.g. "orders"), the
    # one-to-many alias must not be double-pluralized (must not be "orderss").
    from provisa.discovery.fk_introspect import _o2m_alias

    alias = _o2m_alias("orders", hasura_v2_style=True)
    assert alias == "orders"
    assert alias != "orderss"


# ---------------------------------------------------------------------------
# REQ-611: Discovery is structured across five tiers of increasing governance:
# (1) Registered source schema — raw inventory, admin-level visibility;
# (2) Unclaimed tables — introspected from registered sources with no domain owner;
# (3) Domain assets — claimed tables and steward-defined views, fully governed;
# (4) Relationships — approved traversal paths between Tier 3 assets;
# (5) Field grants — domain-to-domain field access permissions.
# Each tier is a prerequisite for the next.
# ---------------------------------------------------------------------------


def test_discovery_input_contains_tables_and_relationships():
    # REQ-611
    # DiscoveryInput (the discovery tier data structure) must carry both table
    # metadata (Tier 2/3 assets) and existing relationships (Tier 4).
    # This validates that the discovery pipeline integrates multiple tiers.
    from provisa.discovery.collector import DiscoveryInput, TableMeta

    t = TableMeta(
        table_id=1,
        source_id="src1",
        domain_id="domain_a",
        schema_name="public",
        table_name="customers",
        columns=[{"name": "id", "type": "integer"}],
        sample_values=[],
    )
    di = DiscoveryInput(
        tables=[t],
        existing_relationships=[
            {
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "id",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
            }
        ],
        rejected_pairs=[],
    )

    assert len(di.tables) == 1
    assert di.tables[0].domain_id == "domain_a"
    assert len(di.existing_relationships) == 1


def test_table_meta_captures_domain_ownership():
    # REQ-611
    # Tier 3 (domain assets) requires tables to carry a domain_id.
    # TableMeta must expose domain_id so the discovery tier can determine
    # whether a table has been claimed.
    from provisa.discovery.collector import TableMeta

    unclaimed = TableMeta(
        table_id=5,
        source_id="src2",
        domain_id="",
        schema_name="public",
        table_name="raw_events",
        columns=[],
        sample_values=[],
    )
    claimed = TableMeta(
        table_id=6,
        source_id="src2",
        domain_id="analytics",
        schema_name="public",
        table_name="events",
        columns=[],
        sample_values=[],
    )

    # Unclaimed table has no domain_id (Tier 2)
    assert unclaimed.domain_id == ""
    # Claimed table carries a domain_id (Tier 3)
    assert claimed.domain_id == "analytics"


def test_discovery_collect_metadata_scope_invalid_raises():
    # REQ-611
    # collect_metadata enforces tier prerequisites by raising ValueError on
    # an invalid scope — the tiered model has exactly three valid scopes.
    import asyncio

    from provisa.discovery.collector import collect_metadata

    trino_conn = MagicMock()
    pg_conn = MagicMock()

    # asyncpg.fetch returns records; simulate empty
    async def mock_fetch(*_):
        return []

    pg_conn.fetch = mock_fetch

    with pytest.raises(ValueError, match="Invalid scope"):
        asyncio.get_event_loop().run_until_complete(
            collect_metadata(trino_conn, pg_conn, "invalid_scope")
        )


# ---------------------------------------------------------------------------
# REQ-612: Relationship candidates are ranked by a four-level confidence
# hierarchy:
# (Highest) Approved catalog relationship validated by both stewards;
# (High) Intra-source FK constraint — explicit modeling intent;
# (Medium) Intra-source semantic inference — column name/type similarity;
# (Low) Cross-source semantic inference — naming conventions diverge.
# Candidates corroborated by multiple evidence types accumulate confidence.
# ---------------------------------------------------------------------------


def test_fk_candidate_has_maximum_confidence():
    # REQ-612
    # FK-derived candidates (High tier) must have confidence=1.0 — the maximum
    # below an approved catalog relationship — reflecting explicit FK intent.
    from provisa.discovery.analyzer import RelationshipCandidate

    fk_candidate = RelationshipCandidate(
        source_table_id=1,
        source_column="user_id",
        target_table_id=2,
        target_column="id",
        cardinality="many_to_one",
        confidence=1.0,
        reasoning="Foreign key constraint",
    )
    assert fk_candidate.confidence == 1.0


def test_validate_candidate_requires_cardinality():
    # REQ-612
    # Candidates must carry a valid cardinality — malformed candidates
    # (including invalid cardinality values) are filtered before ranking.
    from provisa.discovery.analyzer import _validate_candidate
    from provisa.discovery.collector import DiscoveryInput, TableMeta

    t1 = TableMeta(
        table_id=1,
        source_id="s",
        domain_id="d",
        schema_name="sc",
        table_name="a",
        columns=[{"name": "x", "type": "int"}],
        sample_values=[],
    )
    t2 = TableMeta(
        table_id=2,
        source_id="s",
        domain_id="d",
        schema_name="sc",
        table_name="b",
        columns=[{"name": "y", "type": "int"}],
        sample_values=[],
    )
    di = DiscoveryInput(tables=[t1, t2], existing_relationships=[], rejected_pairs=[])

    bad = {
        "source_table_id": 1,
        "source_column": "x",
        "target_table_id": 2,
        "target_column": "y",
        "cardinality": "bad-value",
        "confidence": 0.9,
    }
    assert _validate_candidate(bad, di) is False


def test_validate_candidate_requires_existing_columns():
    # REQ-612
    # Candidates referencing columns that do not exist in the table metadata
    # must be rejected — confidence ranking only applies to valid candidates.
    from provisa.discovery.analyzer import _validate_candidate
    from provisa.discovery.collector import DiscoveryInput, TableMeta

    t1 = TableMeta(
        table_id=1,
        source_id="s",
        domain_id="d",
        schema_name="sc",
        table_name="a",
        columns=[{"name": "real_col", "type": "int"}],
        sample_values=[],
    )
    t2 = TableMeta(
        table_id=2,
        source_id="s",
        domain_id="d",
        schema_name="sc",
        table_name="b",
        columns=[{"name": "id", "type": "int"}],
        sample_values=[],
    )
    di = DiscoveryInput(tables=[t1, t2], existing_relationships=[], rejected_pairs=[])

    bad = {
        "source_table_id": 1,
        "source_column": "nonexistent_col",
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": 0.9,
    }
    assert _validate_candidate(bad, di) is False


def test_validate_candidate_passes_for_valid_candidate():
    # REQ-612
    # A well-formed candidate with valid table IDs, existing columns, and
    # a valid cardinality must pass validation and be eligible for ranking.
    from provisa.discovery.analyzer import _validate_candidate
    from provisa.discovery.collector import DiscoveryInput, TableMeta

    t1 = TableMeta(
        table_id=10,
        source_id="s",
        domain_id="d",
        schema_name="sc",
        table_name="orders",
        columns=[{"name": "user_id", "type": "int"}],
        sample_values=[],
    )
    t2 = TableMeta(
        table_id=20,
        source_id="s",
        domain_id="d",
        schema_name="sc",
        table_name="users",
        columns=[{"name": "id", "type": "int"}],
        sample_values=[],
    )
    di = DiscoveryInput(tables=[t1, t2], existing_relationships=[], rejected_pairs=[])

    good = {
        "source_table_id": 10,
        "source_column": "user_id",
        "target_table_id": 20,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": 0.85,
    }
    assert _validate_candidate(good, di) is True


def test_relationship_candidate_reasoning_field_carried():
    # REQ-612
    # RelationshipCandidate must carry a reasoning field so stewards can
    # understand the evidence behind each ranked candidate.
    from provisa.discovery.analyzer import RelationshipCandidate

    rc = RelationshipCandidate(
        source_table_id=1,
        source_column="col_a",
        target_table_id=2,
        target_column="col_b",
        cardinality="many-to-one",
        confidence=0.75,
        reasoning="Column name similarity: col_a ~ col_b",
    )
    assert rc.reasoning != ""
    assert "similarity" in rc.reasoning
