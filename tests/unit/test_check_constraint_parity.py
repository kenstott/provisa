# Copyright (c) 2026 Kenneth Stott
# Canary: 45f1ad8d-a4be-4054-9fa8-b82d172c9dd4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every enum-like CHECK (``column IN (...)``) admits the same values in both spellings of the
control plane's schema, and the values the code writes.

schema.sql (PostgreSQL) and schema_org.py (the portable metadata) are written by hand, side by
side, so a value added to one is easily missed in the other -- glossary_term_edges' rel_type
check carried four edge types in schema.sql after the code offered ten, and a config's
DERIVED_FROM edge was refused at boot. A check widened where a column is added (``ADD COLUMN IF
NOT EXISTS ... CHECK``) does not reach a table that already has the column; a refresh block
(DROP CONSTRAINT IF EXISTS, then ADD) does, so every occurrence in schema.sql must agree."""

# Requirements: REQ-1387, REQ-1942, REQ-1586

from __future__ import annotations

import re
from pathlib import Path
from typing import LiteralString, cast

from sqlalchemy import CheckConstraint

from provisa.core.env_classes import BINDINGS, SYNTHETIC
from provisa.core.glossary import TERM_EDGE_TYPES
from provisa.core.models import TAG_FIELD_POLICIES, TAG_OBJECT_TYPES, TAG_PARAM_POLICIES
from provisa.core.schema_org import metadata
from provisa.events.queue import _VALID_EVENT

_SCHEMA_SQL = Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"
_IN = re.compile(r"\b([a-z_]+)\s+IN\s*\(([^()]*)\)", re.I)
_TABLE = re.compile(
    r"\b(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE(?: IF EXISTS)?(?: ONLY)?)\s+\"?([a-z_]+)\"?",
    re.I,
)

#: The values the code writes, where one constant names them.
_CONSTANTS: dict[tuple[str, str], frozenset[str]] = {
    ("sources", "binding"): frozenset(BINDINGS),
    # Only a source is bound to an environment's synthetic store (REQ-1942).
    **{
        (table, "binding"): frozenset(BINDINGS) - {SYNTHETIC}
        for table in ("stores", "kafka_sources", "api_sources")
    },
    ("glossary_term_edges", "rel_type"): frozenset(TERM_EDGE_TYPES),
    ("tag_assignments", "object_type"): frozenset(TAG_OBJECT_TYPES),
    ("tags", "expires_policy"): frozenset(TAG_FIELD_POLICIES),
    ("tags", "reason_policy"): frozenset(TAG_FIELD_POLICIES),
    ("tags", "param_policy"): frozenset(TAG_PARAM_POLICIES),
    ("events", "event_type"): frozenset(_VALID_EVENT),
}


def _values(listed: str) -> frozenset[str]:
    return frozenset(v.strip().strip("'") for v in listed.split(",") if v.strip())


def _portable() -> dict[tuple[str, str], frozenset[str]]:
    """Each (table, column) IN-check of schema_org.py, with the values it admits."""
    found: dict[tuple[str, str], frozenset[str]] = {}
    for table in metadata.tables.values():
        for constraint in table.constraints:
            if isinstance(constraint, CheckConstraint):
                for m in _IN.finditer(str(constraint.sqltext)):
                    key = (table.name, m.group(1).lower())
                    assert key not in found, f"{key} is checked twice in schema_org.py"
                    found[key] = _values(m.group(2))
    return found


def _postgres() -> dict[tuple[str, str], list[tuple[int, frozenset[str]]]]:
    """Each (table, column) IN-check of schema.sql, every occurrence (the CREATE, a column added
    later, a refresh block) with its line and the values it admits."""
    sql = _SCHEMA_SQL.read_text()
    found: dict[tuple[str, str], list[tuple[int, frozenset[str]]]] = {}
    for check in re.finditer(r"\bCHECK\s*\(", sql, re.I):
        end, depth = check.end(), 1
        while depth:
            depth += {"(": 1, ")": -1}.get(sql[end], 0)
            end += 1
        tables = _TABLE.findall(sql, 0, check.start())
        line = sql.count("\n", 0, check.start()) + 1
        for m in _IN.finditer(sql[check.end() : end - 1]):
            found.setdefault((tables[-1], m.group(1).lower()), []).append(
                (line, _values(m.group(2)))
            )
    return found


def test_every_check_admits_the_same_values_in_both_schemas():
    portable, postgres = _portable(), _postgres()
    assert set(postgres) == set(portable), (
        f"only in schema.sql: {sorted(set(postgres) - set(portable))}; "
        f"only in schema_org.py: {sorted(set(portable) - set(postgres))}"
    )
    drift = {
        key: (sorted(portable[key]), [(line, sorted(v)) for line, v in seen if v != portable[key]])
        for key, seen in postgres.items()
        if any(v != portable[key] for _, v in seen)
    }
    assert not drift, f"schema_org.py's values, then schema.sql's that differ (line): {drift}"


def test_every_check_admits_exactly_the_values_the_code_writes():
    portable = _portable()
    drift = {key: (sorted(portable[key]), sorted(want)) for key, want in _CONSTANTS.items()}
    assert {k: v for k, v in drift.items() if v[0] != v[1]} == {}


def test_a_sources_table_that_predates_synthetic_takes_the_wider_check():
    """The one check widened where its column was added: a refresh block replaces it."""
    sql = _SCHEMA_SQL.read_text()
    assert re.search(
        r"DO \$\$ BEGIN\n    ALTER TABLE sources DROP CONSTRAINT IF EXISTS sources_binding_check;\n"
        r"    ALTER TABLE sources ADD CONSTRAINT sources_binding_check\n"
        r"        CHECK \(binding IN \('own', 'copied', 'unbound', 'synthetic'\)\);",
        sql,
    )


def test_postgres_admits_a_synthetic_binding_on_a_sources_table_that_predates_it(
    embedded_postgres,
):
    import psycopg

    sql = _SCHEMA_SQL.read_text()
    refresh = re.search(
        r"DO \$\$ BEGIN\n    ALTER TABLE sources DROP CONSTRAINT IF EXISTS sources_binding_check;"
        r".*?END \$\$;\n",
        sql,
        re.S,
    )
    assert refresh
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS binding_old CASCADE")
        cur.execute("CREATE SCHEMA binding_old")
        cur.execute("SET search_path TO binding_old")
        # The column as schema.sql added it before REQ-1942's synthetic binding.
        cur.execute(
            "CREATE TABLE sources (id TEXT PRIMARY KEY, binding TEXT NOT NULL DEFAULT 'own' "
            "CHECK (binding IN ('own', 'copied', 'unbound')))"
        )
        block = cast("LiteralString", refresh.group(0))  # schema.sql's own text
        cur.execute(block)
        cur.execute(block)  # a reboot re-runs it
        cur.execute("INSERT INTO sources (id, binding) VALUES ('s', 'synthetic')")
        cur.execute("DROP SCHEMA binding_old CASCADE")
