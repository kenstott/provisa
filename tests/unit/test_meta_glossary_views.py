# Copyright (c) 2026 Kenneth Stott
# Canary: 0f2c6a91-58d4-4b7e-9a13-6d0e2c74b8af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests: the glossary and tag graph as queryable meta-domain metadata.

REQ-1584 registers the glossary views and REQ-1585 the tag graph edges; both bind to
the synthesized ``column_key`` (``table_id || ':' || column_name``) rather than to
``table_columns.id``, which the table upsert replaces wholesale. These run the view SQL
against SQLite so the flags, the key and the traversal are exercised, not just read.
"""

from __future__ import annotations

import re
import sqlite3

import pytest

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS

# The base tables the glossary and tag views read, with exactly the columns they select.
_BASE_DDL = """
CREATE TABLE glossary_terms (
    id TEXT PRIMARY KEY, name TEXT, definition TEXT, is_abstract BOOLEAN,
    deprecated BOOLEAN, retired BOOLEAN, export_excluded BOOLEAN, tenant_id TEXT);
CREATE TABLE glossary_term_refs (
    id TEXT PRIMARY KEY, term_id TEXT, table_id INTEGER, column_name TEXT, tenant_id TEXT);
CREATE TABLE glossary_term_edges (
    id TEXT PRIMARY KEY, from_term_id TEXT, to_term_id TEXT, rel_type TEXT, tenant_id TEXT);
CREATE TABLE glossary_term_experts (
    id TEXT PRIMARY KEY, term_id TEXT, user_id TEXT, kind TEXT, tenant_id TEXT);
CREATE TABLE registered_tables (
    id INTEGER PRIMARY KEY, source_id TEXT, schema_name TEXT, table_name TEXT,
    domain_id TEXT, tenant_id TEXT);
CREATE TABLE table_columns (
    id INTEGER PRIMARY KEY, table_id INTEGER, column_name TEXT, data_type TEXT,
    is_primary_key BOOLEAN, alias TEXT, description TEXT, path TEXT, scope TEXT,
    mask_type TEXT, mask_pattern TEXT, mask_replace TEXT, mask_value TEXT,
    mask_precision TEXT, native_filter_type TEXT, is_foreign_key BOOLEAN,
    is_alternate_key BOOLEAN, object_fields TEXT, visible_to TEXT, unmasked_to TEXT,
    writable_by TEXT, tenant_id TEXT);
CREATE TABLE tag_assignments (
    id TEXT PRIMARY KEY, tag_id TEXT, base_tag_id TEXT, object_type TEXT, source_id TEXT,
    table_id INTEGER, column_name TEXT, relationship_id TEXT, command_name TEXT,
    object_key TEXT, reason TEXT, expires_on TEXT, tenant_id TEXT);
"""

_VIEWS = (
    "glossary_terms",
    "glossary_term_refs",
    "glossary_term_edges",
    "glossary_term_experts",
    "table_columns",
    "tag_assignments",
)


@pytest.fixture
def db() -> sqlite3.Connection:
    """A control plane holding one table, two columns, three terms and four tags."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_BASE_DDL)
    for name in _VIEWS:
        # SQLite has no CREATE OR REPLACE VIEW — startup_seed adapts the same way.
        conn.executescript(
            re.sub(r"CREATE\s+OR\s+REPLACE\s+VIEW", "CREATE VIEW", _META_TABLE_VIEWS[name])
        )

    conn.execute(
        "INSERT INTO registered_tables VALUES (7, 'crm', 'public', 'users', 'sales', 'org1')"
    )
    conn.executemany(
        "INSERT INTO table_columns (id, table_id, column_name, alias, mask_type, tenant_id)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 7, "usr_nm", "user name", None, "org1"),
            (2, 7, "ssn", "social security number", "regex", "org1"),
        ],
    )
    conn.executemany(
        "INSERT INTO glossary_terms VALUES (?, ?, ?, 0, ?, ?, ?, 'org1')",
        [
            # bound to a column and defined -> live
            ("t-user", "user name", "The name a person signs in with.", 0, 0, 0),
            # defined and reachable only over an edge from a bound term -> live, not rooted
            ("t-party", "party", "A person or organisation.", 0, 0, 0),
            # retired, and export-excluded: both are flags, never predicates
            ("t-old", "legacy name", "Superseded.", 0, 1, 1),
        ],
    )
    conn.execute("INSERT INTO glossary_term_refs VALUES ('r1', 't-user', 7, 'usr_nm', 'org1')")
    conn.execute(
        "INSERT INTO glossary_term_edges VALUES ('e1', 't-user', 't-party', 'KIND_OF', 'org1')"
    )
    conn.executemany(
        "INSERT INTO tag_assignments (id, tag_id, object_type, table_id, column_name,"
        " command_name, tenant_id) VALUES (?, ?, ?, ?, ?, ?, 'org1')",
        [
            ("a1", "pii", "column", 7, "usr_nm", None),
            ("a2", "pii", "column", 7, "ssn", None),
            ("a3", "deprecated", "table", 7, None, None),
            ("a4", "reviewed", "command", None, None, "reindex"),
        ],
    )
    conn.commit()
    return conn


class TestGlossaryMetaViews:
    """REQ-1584: the glossary is SQL, and its refs resolve to the column they bind."""

    def test_live_and_rooted_are_computed_by_the_export_admission_rule(
        self, db: sqlite3.Connection
    ) -> None:
        rows = {
            r["id"]: r
            for r in db.execute("SELECT id, ref_count, is_rooted, live FROM glossary_terms_meta")
        }
        assert (rows["t-user"]["ref_count"], rows["t-user"]["is_rooted"]) == (1, 1)
        assert rows["t-user"]["live"] == 1
        # Grounding is transitive over in-service edges: t-party publishes without a ref.
        assert (rows["t-party"]["ref_count"], rows["t-party"]["is_rooted"]) == (0, 0)
        assert rows["t-party"]["live"] == 1

    def test_a_retired_or_excluded_term_is_returned_with_its_flag_not_withheld(
        self, db: sqlite3.Connection
    ) -> None:
        row = db.execute(
            "SELECT retired, export_excluded, live FROM glossary_terms_meta WHERE id = 't-old'"
        ).fetchone()
        assert (row["retired"], row["export_excluded"]) == (1, 1)
        # Retired means out of service, so it grounds nothing — but it is still returned.
        assert row["live"] == 0

    def test_a_ref_resolves_to_its_column_over_the_synthesized_key(
        self, db: sqlite3.Connection
    ) -> None:
        row = db.execute(
            "SELECT r.column_key, c.alias, c.mask_type"
            " FROM glossary_term_refs_meta r"
            " JOIN table_columns_meta c ON c.column_key = r.column_key"
        ).fetchone()
        assert row["column_key"] == "7:usr_nm"
        assert (row["alias"], row["mask_type"]) == ("user name", None)

    def test_the_edge_view_names_both_endpoints_beside_their_ids(
        self, db: sqlite3.Connection
    ) -> None:
        row = db.execute(
            "SELECT from_term, to_term, rel_type FROM glossary_term_edges_meta"
        ).fetchone()
        assert (row["from_term"], row["to_term"], row["rel_type"]) == (
            "user name",
            "party",
            "KIND_OF",
        )


class TestTagGraphMetaJoins:
    """REQ-1585: term -> column -> tag closes in one traversal, and only for columns."""

    def test_a_non_column_assignment_has_a_null_column_key(self, db: sqlite3.Connection) -> None:
        keys = {
            r["id"]: r["column_key"]
            for r in db.execute("SELECT id, column_key FROM tag_assignments_meta")
        }
        assert keys == {"a1": "7:usr_nm", "a2": "7:ssn", "a3": None, "a4": None}

    def test_traversing_from_a_term_reaches_only_the_column_level_tags(
        self, db: sqlite3.Connection
    ) -> None:
        rows = db.execute(
            "SELECT a.id, a.tag_id FROM glossary_terms_meta t"
            " JOIN glossary_term_refs_meta r ON r.term_id = t.id"
            " JOIN tag_assignments_meta a ON a.column_key = r.column_key"
            " WHERE t.id = 't-user'"
        ).fetchall()
        # a1 alone: a2 is another column, a3/a4 carry no column key at all.
        assert [(r["id"], r["tag_id"]) for r in rows] == [("a1", "pii")]

    @pytest.mark.parametrize(
        "rel_id,source,target",
        [
            ("meta:table_columns:glossary_term_refs", "table_columns", "glossary_term_refs"),
            ("meta:glossary_term_refs:table_columns", "glossary_term_refs", "table_columns"),
            ("meta:table_columns:tag_assignments", "table_columns", "tag_assignments"),
            ("meta:tag_assignments:table_columns", "tag_assignments", "table_columns"),
            ("meta:glossary_term_refs:tag_assignments", "glossary_term_refs", "tag_assignments"),
            ("meta:tag_assignments:glossary_term_refs", "tag_assignments", "glossary_term_refs"),
        ],
    )
    def test_the_column_level_edges_are_registered_on_the_shared_key(
        self, rel_id: str, source: str, target: str
    ) -> None:
        rel = next(r for r in META_RELATIONSHIPS if r[0] == rel_id)
        assert (rel[2], rel[7]) == (source, target)
        # Both sides join on column_key, never on the surrogate table_columns.id.
        assert (rel[3], rel[4]) == ("column_key", "column_key")
