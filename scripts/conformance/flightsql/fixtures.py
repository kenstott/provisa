# Copyright (c) 2026 Kenneth Stott
# Canary: 90b1e14a-4487-4b81-ba6d-ede883a01122
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ADBC validation suite's fixture tables, declared in the Flight SQL job's fixture model
(REQ-2167, "setup that creates objects is adapted, not excluded").

Provisa creates no table through a query surface, so a case that builds its table in its setup
is adapted: before the instance starts, its setup_query runs in the job's PostgreSQL (schema
``conformance``) and the table is registered in the model's ``conformance`` domain, where the
case's own query reads it. Where cases share a table name with different contents (the ten
``test_timestamp`` precisions), each case's table is named for the case (``test_<case>``) and the
case's query and metadata are overridden to read and name it (``queries/type/select/<case>.sql``
and ``.toml``, checked in and checked here). Where a setup is not PostgreSQL (``BLOB``, ``X'..'``), it is written in PostgreSQL's
equivalent. The get_objects tests' table, which they would ingest, is ``getobjects_fixture``; the
statement tests' tables, which they would create and write, are seeded empty and writable
(``WRITTEN_TABLES``).

What each adapted case's setup became is stated in by_design/flight-sql.yaml, for the page.

Usage (in the job's .venv, before the instance starts):
  fixtures.py seed --validation <suite checkout> --pg <dsn> --out <tables.yaml>
"""

# Requirements: REQ-2167

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

SCHEMA = "conformance"
SELECT_CASES = Path("adbc_drivers_validation/queries/type/select")
OVERRIDES = HERE / "queries" / "type" / "select"

#: The get_objects tests' table: the columns and rows their fixture would ingest.
GET_OBJECTS_TABLE = "getobjects_fixture"
GET_OBJECTS_SETUP = f"""CREATE TABLE {GET_OBJECTS_TABLE} (ints INTEGER, strs TEXT);
INSERT INTO {GET_OBJECTS_TABLE} (ints, strs) VALUES (1, NULL), (NULL, 'foo'), (42, 'spam');"""

#: The statement tests' tables, which they would create empty and then write: seeded empty and
#: writable by org_admin. The tests' CREATE TABLE is replaced by emptying the table
#: (tests/provisa.py's query_override), as a rerun starts from the empty table the CREATE made.
WRITTEN_TABLES = {
    "sample_table": "CREATE TABLE sample_table (id INTEGER, value TEXT);",
    "test_rows_affected": "CREATE TABLE test_rows_affected (id INTEGER);",
}

_PART = re.compile(r"^// part: (\w+)\s*$", re.M)
_CREATE = re.compile(r"CREATE TABLE\s+(\w+)", re.I)
_HEX = re.compile(r"X'([0-9a-fA-F]*)'")


def _parts(text: str) -> dict[str, str]:
    """A .txtcase file's parts by name."""
    found = list(_PART.finditer(text))
    return {
        m.group(1): text[m.end() : found[i + 1].start() if i + 1 < len(found) else len(text)]
        for i, m in enumerate(found)
    }


@dataclass(frozen=True)
class Case:
    name: str  # the case's name under type/select (int32, timestamp3tz, ...)
    table: str  # the table its setup creates
    setup: str  # its setup_query
    query: str  # its query

    @property
    def adapted_table(self) -> str:
        """The table it reads in the fixture model: its own where its setup's name is shared."""
        return f"test_{self.name}"

    @property
    def renamed(self) -> bool:
        return self.adapted_table != self.table

    def _rename(self, sql: str) -> str:
        return re.sub(rf"\b{re.escape(self.table)}\b", self.adapted_table, sql)

    @property
    def adapted_setup(self) -> str:
        """Its setup as PostgreSQL runs it, creating its adapted table."""
        sql = re.sub(r"\bBLOB\b", "BYTEA", self._rename(self.setup))
        return _HEX.sub(lambda m: f"'\\x{m.group(1)}'::bytea", sql)

    @property
    def adapted_query(self) -> str:
        return self._rename(self.query).strip()


def cases(validation: Path) -> list[Case]:
    """The suite's type/select cases that create their table in setup."""
    out = []
    for path in sorted((validation / SELECT_CASES).glob("*.txtcase")):
        parts = _parts(path.read_text())
        setup = parts.get("setup_query")
        if not setup:
            continue
        created = _CREATE.search(setup)
        if created is None:
            raise ValueError(f"{path.name}: a setup_query that creates no table")
        out.append(Case(path.stem, created.group(1), setup, parts["query"]))
    return out


def metadata_override(case: Case) -> str:
    """A renamed case's metadata override: the table its setup names, which the suite reads its
    table schema from (``[setup] drop``)."""
    return f'[setup]\ndrop = "{case.adapted_table}"\n'


def check_overrides(found: list[Case]) -> None:
    """Every renamed case's overrides exist: its query reads the case's own table, and its
    metadata names that table."""
    for case in found:
        if not case.renamed:
            continue
        expected = {
            OVERRIDES / f"{case.name}.sql": case.adapted_query,
            OVERRIDES / f"{case.name}.toml": metadata_override(case).strip(),
        }
        for override, text in expected.items():
            if not override.exists():
                raise FileNotFoundError(f"{override}: {case.name} reads {case.adapted_table}")
            if override.read_text().strip() != text:
                raise ValueError(f"{override}: expected {text!r}")


def _columns(conn, table: str) -> list[tuple[str, str]]:
    return conn.execute(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum",
        [SCHEMA, table],
    ).fetchall()


def _model_table(conn, table: str, *, written: bool = False) -> dict:
    """``table`` as the model registers it: every column typed as registration types a
    PostgreSQL column, visible to org_admin; ``written``, writable by org_admin too."""
    from pgwire_catalog_types import registered_typing

    columns = []
    for name, pg_type in _columns(conn, table):
        typing = registered_typing(pg_type)
        if typing["data_type"] == "numeric":
            raise ValueError(f"{table}.{name}: an unsized numeric; size it in the setup")
        columns.append({"name": name, **typing, "visible_to": ["org_admin"]})
    entry = {
        "source_id": "conformance-pg",
        "schema": SCHEMA,
        "table": table,
        "alias": table,
        "domain_id": SCHEMA,
        "columns": columns,
    }
    if written:
        names = [c["name"] for c in columns]
        entry["write_grants"] = [
            {"role": "org_admin", "operation": "insert", "columns": names},
            {"role": "org_admin", "operation": "update", "columns": names},
            {"role": "org_admin", "operation": "delete"},
        ]
    return entry


def seed(validation: Path, pg_dsn: str, out: Path) -> None:
    import psycopg
    import yaml

    found = cases(validation)
    check_overrides(found)
    setups = [(c.adapted_table, c.adapted_setup) for c in found]
    setups.append((GET_OBJECTS_TABLE, GET_OBJECTS_SETUP))
    setups.extend(WRITTEN_TABLES.items())
    tables = []
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        conn.execute(f"SET search_path TO {SCHEMA}")
        for table, setup in setups:
            conn.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{table}")
            conn.execute(setup)
            tables.append(_model_table(conn, table, written=table in WRITTEN_TABLES))
    out.write_text(yaml.safe_dump(tables, sort_keys=False))
    print(f"seeded {len(tables)} fixture tables into {SCHEMA}; wrote {out}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--validation", type=Path, required=True)
    s.add_argument("--pg", required=True)
    s.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    seed(args.validation, args.pg, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
