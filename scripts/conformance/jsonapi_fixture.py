# Copyright (c) 2026 Kenneth Stott
# Canary: e0b778ae-33b1-47e2-8103-137bedce4b7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JSON:API job's fixture table (REQ-2167): the resource type jsonapi_spec.py reads.

The instance's own tables do not serve the read-only cases: ``probe_identity`` is seeded empty
(the function probe inserts into it) and ``types_fixture`` has no key, so no single-resource
lookup. This table has a key (``id``), two attributes to narrow with a sparse fieldset, and rows.
It is seeded into the job's PostgreSQL (schema ``conformance``) and registered in the model's
``conformance`` domain, read by ``org_admin``, through the instance's ``extra-tables``.

Usage (in the job's .venv, before the instance starts):
  jsonapi_fixture.py seed --pg <dsn> --out <tables.yaml>
"""

# Requirements: REQ-2167

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

SCHEMA = "conformance"
#: The resource type the cases read: its domain is the instance's ``conformance``.
TABLE = "jsonapi_resource"
#: Its attribute columns after its key, ``id``, with their PostgreSQL types.
ATTRIBUTES: dict[str, str] = {"name": "text", "quantity": "integer"}
#: Its rows: (name, quantity).
ROWS: list[tuple[str, int]] = [("first", 10), ("second", 20), ("third", 30)]


def ddl() -> list[str]:
    """The statements creating and filling the table in the job's PostgreSQL."""
    cols = ", ".join(f"{c} {t}" for c, t in ATTRIBUTES.items())
    values = ", ".join(f"('{name}', {quantity})" for name, quantity in ROWS)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}",
        f"CREATE TABLE {SCHEMA}.{TABLE} (id serial PRIMARY KEY, {cols})",
        f"INSERT INTO {SCHEMA}.{TABLE} ({', '.join(ATTRIBUTES)}) VALUES {values}",
    ]


def tables() -> list[dict]:
    """The model's entry for the table: its columns typed as registration types them, read by
    org_admin."""
    from pgwire_catalog_types import registered_typing

    columns = [
        {
            "name": "id",
            **registered_typing("integer"),
            "is_primary_key": True,
            "visible_to": ["org_admin"],
        },
        *(
            {"name": c, **registered_typing(t), "visible_to": ["org_admin"]}
            for c, t in ATTRIBUTES.items()
        ),
    ]
    return [
        {
            "source_id": "conformance-pg",
            "schema": SCHEMA,
            "table": TABLE,
            "alias": TABLE,
            "domain_id": SCHEMA,
            "columns": columns,
        }
    ]


def seed(pg_dsn: str, tables_out: Path) -> None:
    import psycopg
    import yaml

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for statement in ddl():
            conn.execute(statement)
    tables_out.write_text(yaml.safe_dump(tables(), sort_keys=False))
    print(f"seeded {SCHEMA}.{TABLE}: {len(ROWS)} rows")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--pg", required=True)
    s.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    seed(args.pg, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
