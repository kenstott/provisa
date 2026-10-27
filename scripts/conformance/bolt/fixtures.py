# Copyright (c) 2026 Kenneth Stott
# Canary: 337068e1-ca23-4570-869a-b4adb690ac43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Neo4j Python driver's example graph, declared in the Bolt job's fixture model (REQ-2167,
"setup that creates objects is adapted, not excluded").

Provisa creates no label or relationship type through a query surface, so the labels and types
the driver's examples create as their setup are declared here, in the job's PostgreSQL (schema
``conformance``) and in the model's ``conformance`` domain, writable by ``org_admin``: Person,
Company and Greeting, and KNOWS (Person to Person) and WORKS_FOR (Person to Company), each a
junction table. The examples then write and read their own rows of them. The companies the
examples name as already there (Wayne Enterprises, LexCorp, Neo4j) are seeded. A junction's keys
cascade a deleted person or company, as the examples' DETACH DELETE cleanup does.

What each adapted case's setup became is stated in by_design/bolt.yaml, for the page.

Usage (in the job's .venv, before the instance starts):
  fixtures.py seed --pg <dsn> --tables <tables.yaml> --relationships <relationships.yaml>
"""

# Requirements: REQ-2167

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

SCHEMA = "conformance"
ROLE = "org_admin"

#: Each node table: its columns after its key, ``id``.
NODES: dict[str, list[str]] = {"person": ["name"], "company": ["name"], "greeting": ["message"]}
#: The companies the examples employ people at, as already there.
COMPANIES = ("Wayne Enterprises", "LexCorp", "Neo4j")
#: Each junction table: its relationship type, its source and target node tables.
JUNCTIONS: dict[str, tuple[str, str, str]] = {
    "knows": ("KNOWS", "person", "person"),
    "works_for": ("WORKS_FOR", "person", "company"),
}


def ddl() -> list[str]:
    """The statements creating the example graph's tables in the job's PostgreSQL."""
    statements = [f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"]
    for table in (*JUNCTIONS, *NODES):
        statements.append(f"DROP TABLE IF EXISTS {SCHEMA}.{table}")
    for table, columns in NODES.items():
        cols = ", ".join(f"{c} text" for c in columns)
        statements.append(f"CREATE TABLE {SCHEMA}.{table} (id serial PRIMARY KEY, {cols})")
    for table, (_type, source, target) in JUNCTIONS.items():
        statements.append(
            f"CREATE TABLE {SCHEMA}.{table} (id serial PRIMARY KEY, "
            f"from_id integer NOT NULL REFERENCES {SCHEMA}.{source}(id) ON DELETE CASCADE, "
            f"to_id integer NOT NULL REFERENCES {SCHEMA}.{target}(id) ON DELETE CASCADE)"
        )
    statements.append(
        f"INSERT INTO {SCHEMA}.company (name) VALUES "
        + ", ".join(f"('{name}')" for name in COMPANIES)
    )
    return statements


def _table(table: str, written: list[str]) -> dict:
    """A model table entry: its columns typed as registration types them, read and written by
    ``org_admin``."""
    from pgwire_catalog_types import registered_typing

    columns = [
        {"name": "id", **registered_typing("integer"), "is_primary_key": True, "visible_to": [ROLE]}
    ] + [
        {
            "name": c,
            **registered_typing("integer" if c.endswith("_id") else "text"),
            "visible_to": [ROLE],
        }
        for c in written
    ]
    return {
        "source_id": "conformance-pg",
        "schema": SCHEMA,
        "table": table,
        "alias": table,
        "domain_id": SCHEMA,
        "columns": columns,
        "write_grants": [
            {"role": ROLE, "operation": "insert", "columns": written},
            {"role": ROLE, "operation": "update", "columns": written},
            {"role": ROLE, "operation": "delete"},
        ],
    }


def tables() -> list[dict]:
    """The model's entries for every node and junction table."""
    return [
        *(_table(t, cols) for t, cols in NODES.items()),
        *(_table(t, ["from_id", "to_id"]) for t in JUNCTIONS),
    ]


def relationships() -> list[dict]:
    """Each junction's relationship, its type named by its alias (via_label_source fixed)."""
    return [
        {
            "id": f"{source}-{table}-{target}",
            "alias": rel_type,
            "cardinality": "one-to-many",
            "source_table_id": source,
            "source_column": "id",
            "target_table_id": target,
            "target_column": "id",
            "via_table": table,
            "via_source_column": "from_id",
            "via_target_column": "to_id",
            "via_label_source": "fixed",
        }
        for table, (rel_type, source, target) in JUNCTIONS.items()
    ]


def seed(pg_dsn: str, tables_out: Path, relationships_out: Path) -> None:
    import psycopg
    import yaml

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for statement in ddl():
            conn.execute(statement)
    tables_out.write_text(yaml.safe_dump(tables(), sort_keys=False))
    relationships_out.write_text(yaml.safe_dump(relationships(), sort_keys=False))
    print(f"seeded {SCHEMA}: {', '.join([*NODES, *JUNCTIONS])}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--pg", required=True)
    s.add_argument("--tables", required=True, type=Path)
    s.add_argument("--relationships", required=True, type=Path)
    args = parser.parse_args(argv)
    seed(args.pg, args.tables, args.relationships)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
