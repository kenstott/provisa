# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e91a4-5d20-4b8f-a6e3-0f9b2d7c4e18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables the BI tools' schema browsers are pointed at (REQ-2204), beside the pgwire job's own
fixture (pgwire_catalog_types.py ``seed``: ``conformance.types_fixture`` and the function probe's
enum, sequence and identity table).

Two tables in schema ``conformance`` give a browser keys and an index to list: ``bi_customer``
(primary key ``id``) and ``bi_order`` (primary key ``id``, ``customer_id`` a foreign key to
``bi_customer`` with an index of its own). The same tables are declared in the conformance
instance's model (domain ``conformance``, visible to org_admin, ``customer_id`` a many-to-one
relationship), so a statement captured against PostgreSQL names the same relations on Provisa.

Usage (in the job's .venv, before the instance starts):
  fixtures.py seed --pg <dsn> --tables <tables.yaml> --relationships <relationships.yaml>
"""

# Requirements: REQ-2204

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

SCHEMA = "conformance"

#: Each table: its columns after ``id`` (its key), as (column, PostgreSQL type).
TABLES: dict[str, list[tuple[str, str]]] = {
    "bi_customer": [("name", "text"), ("region", "character varying(20)")],
    "bi_order": [
        ("customer_id", "integer"),
        ("total", "numeric(10,2)"),
        ("placed", "date"),
    ],
}
#: (table, column, referenced table): the foreign keys, each with an index on its column.
FOREIGN_KEYS: list[tuple[str, str, str]] = [("bi_order", "customer_id", "bi_customer")]

ROWS: dict[str, list[str]] = {
    "bi_customer": ["(1, 'Ada', 'north')", "(2, 'Grace', 'south')"],
    "bi_order": ["(1, 1, 12.50, '2026-01-02')", "(2, 2, 40.00, '2026-02-03')"],
}


def ddl() -> list[str]:
    """The statements creating and filling the tables in the job's PostgreSQL."""
    statements = [f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"]
    statements += [f"DROP TABLE IF EXISTS {SCHEMA}.{t}" for t in reversed(TABLES)]
    for table, columns in TABLES.items():
        cols = ", ".join(f"{c} {typ}" for c, typ in columns)
        statements.append(f"CREATE TABLE {SCHEMA}.{table} (id integer PRIMARY KEY, {cols})")
    for table, column, target in FOREIGN_KEYS:
        statements.append(
            f"ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT {table}_{column}_fkey "
            f"FOREIGN KEY ({column}) REFERENCES {SCHEMA}.{target}(id)"
        )
        statements.append(f"CREATE INDEX {table}_{column}_idx ON {SCHEMA}.{table} ({column})")
    for table, rows in ROWS.items():
        statements.append(f"INSERT INTO {SCHEMA}.{table} VALUES {', '.join(rows)}")
    return statements


def tables() -> list[dict]:
    """The model's entries: each column typed as registration types it, visible to org_admin."""
    from pgwire_catalog_types import registered_typing

    return [
        {
            "source_id": "conformance-pg",
            "schema": SCHEMA,
            "table": table,
            "alias": table,
            "domain_id": SCHEMA,
            "columns": [
                {
                    "name": "id",
                    **registered_typing("integer"),
                    "is_primary_key": True,
                    "visible_to": ["org_admin"],
                },
                *(
                    {"name": c, **registered_typing(typ), "visible_to": ["org_admin"]}
                    for c, typ in columns
                ),
            ],
        }
        for table, columns in TABLES.items()
    ]


def relationships() -> list[dict]:
    """Each foreign key as a many-to-one relationship."""
    return [
        {
            "id": f"{table}__{column}",
            "alias": f"{table}_{target}".upper(),
            "cardinality": "many-to-one",
            "source_table_id": table,
            "source_column": column,
            "target_table_id": target,
            "target_column": "id",
        }
        for table, column, target in FOREIGN_KEYS
    ]


def seed(pg_dsn: str, tables_out: Path, relationships_out: Path) -> None:
    import psycopg
    import yaml

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for statement in ddl():
            conn.execute(statement)
    tables_out.write_text(yaml.safe_dump(tables(), sort_keys=False))
    relationships_out.write_text(yaml.safe_dump(relationships(), sort_keys=False))
    print(f"seeded {SCHEMA}: {', '.join(TABLES)}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
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
