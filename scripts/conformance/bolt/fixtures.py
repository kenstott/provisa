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
``conformance``) and in a model domain of their own, ``bolt_examples``: Person, Company and
Greeting, and KNOWS (Person to Person) and WORKS_FOR (Person to Company), each a junction table;
KNOWS carries the ``from`` property the driver's introduction example writes. The tables start
empty: each example writes the rows it reads, after its cleanup has emptied the graph.

The job's driver user acts as a role of its own, ``bolt_examples``, mapped from its username,
whose domain access is that domain alone: the examples' ``MATCH (a) DETACH DELETE a`` cleanup
reaches their own graph and nothing else (not conformance.types_fixture), and their MERGE and
DETACH DELETE run as written. A junction's keys cascade a deleted person or company.

What each adapted case's setup became is stated in by_design/bolt.yaml, for the page.

Usage (in the job's .venv, before the instance starts):
  fixtures.py seed --pg <dsn> --user <the driver's user> --tables <tables.yaml>
      --relationships <relationships.yaml> --access <access.yaml>
"""

# Requirements: REQ-2167

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

SCHEMA = "conformance"
#: The examples' model domain, and the role the driver's user acts as: it reaches that domain alone.
DOMAIN = "bolt_examples"
ROLE = "bolt_examples"

#: Each node table: its columns after its key, ``id``.
NODES: dict[str, list[str]] = {"person": ["name"], "company": ["name"], "greeting": ["message"]}
#: Each junction table: its relationship type, its source and target node tables.
JUNCTIONS: dict[str, tuple[str, str, str]] = {
    "knows": ("KNOWS", "person", "person"),
    "works_for": ("WORKS_FOR", "person", "company"),
}
#: Each junction table's attribute columns: the relationship's properties the examples write.
ATTRIBUTES: dict[str, list[str]] = {"knows": ["from"], "works_for": []}


def ddl() -> list[str]:
    """The statements creating the example graph's tables in the job's PostgreSQL."""
    statements = [f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"]
    for table in (*JUNCTIONS, *NODES):
        statements.append(f"DROP TABLE IF EXISTS {SCHEMA}.{table}")
    for table, columns in NODES.items():
        cols = ", ".join(f"{c} text" for c in columns)
        statements.append(f"CREATE TABLE {SCHEMA}.{table} (id serial PRIMARY KEY, {cols})")
    for table, (_type, source, target) in JUNCTIONS.items():
        # An attribute's name may be a SQL keyword (``from``): quoted.
        attrs = "".join(f', "{c}" text' for c in ATTRIBUTES[table])
        statements.append(
            f"CREATE TABLE {SCHEMA}.{table} (id serial PRIMARY KEY, "
            f"from_id integer NOT NULL REFERENCES {SCHEMA}.{source}(id) ON DELETE CASCADE, "
            f"to_id integer NOT NULL REFERENCES {SCHEMA}.{target}(id) ON DELETE CASCADE{attrs})"
        )
    return statements


def _table(table: str, written: list[str]) -> dict:
    """A model table entry in the examples' domain: its columns typed as registration types them,
    read and written by the examples' role."""
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
        "domain_id": DOMAIN,
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
        *(_table(t, ["from_id", "to_id", *ATTRIBUTES[t]]) for t in JUNCTIONS),
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


def access(user: str) -> dict:
    """The examples' domain, their role (reaching that domain alone, writing as the examples do)
    and the rule mapping the driver's user to it, by username."""
    return {
        "domains": [{"id": DOMAIN, "description": "The Neo4j driver examples' graph"}],
        "roles": [
            {
                "id": ROLE,
                "description": "The Neo4j driver examples' user: reads and writes their graph alone",
                "capabilities": ["query_development", "full_results", "write"],
                "domain_access": [DOMAIN],
            }
        ],
        "role_mapping": [{"claim": "username", "type": "exact", "value": user, "role": ROLE}],
    }


def seed(
    pg_dsn: str, user: str, tables_out: Path, relationships_out: Path, access_out: Path
) -> None:
    import psycopg
    import yaml

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for statement in ddl():
            conn.execute(statement)
    tables_out.write_text(yaml.safe_dump(tables(), sort_keys=False))
    relationships_out.write_text(yaml.safe_dump(relationships(), sort_keys=False))
    access_out.write_text(yaml.safe_dump(access(user), sort_keys=False))
    print(f"seeded {SCHEMA}: {', '.join([*NODES, *JUNCTIONS])}; {user} acts as {ROLE}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--pg", required=True)
    s.add_argument("--user", required=True, help="the user the driver signs in as")
    s.add_argument("--tables", required=True, type=Path)
    s.add_argument("--relationships", required=True, type=Path)
    s.add_argument("--access", required=True, type=Path)
    args = parser.parse_args(argv)
    seed(args.pg, args.user, args.tables, args.relationships, args.access)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
