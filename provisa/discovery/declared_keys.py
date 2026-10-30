# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1f3b82-5a7c-4e90-b4d2-8f0c6e1a3b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The keys a source declares for a table (REQ-2170): its primary key, its unique (alternate)
keys and its foreign keys, read from the source's own catalog over its pooled driver, with the
columns it declares NOT NULL (REQ-2190).

Registration records them into the model (``table_columns.is_primary_key`` and
``is_alternate_key``) and discovery offers the foreign keys as relationship candidates, so every
key-dependent feature reads the source's own declaration. A source that declares no keys records
none; nothing is guessed from data. A type with no reader here, or a source with no driver in the
pool, is not read: :func:`declared_keys` answers None, never an empty declaration.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from provisa.compiler.sql_literals import sql_literal


@dataclass(frozen=True)
class ForeignKey:
    """One column of the table that references a column of another table."""

    column: str
    referenced_table: str
    referenced_column: str


@dataclass(frozen=True)
class UniqueKey:
    """A unique constraint the source declares (not its primary key), its columns in order."""

    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class DeclaredKeys:
    """What a source declares of one table's keys."""

    primary_key: tuple[str, ...] = ()
    unique: tuple[UniqueKey, ...] = ()
    foreign: tuple[ForeignKey, ...] = ()
    # REQ-2190: the columns it declares NOT NULL.
    not_null: frozenset[str] = frozenset()

    @property
    def alternate_columns(self) -> frozenset[str]:
        """The columns that are a unique key on their own: each one identifies a row."""
        return frozenset(key.columns[0] for key in self.unique if len(key.columns) == 1)


Reader = Callable[[Any, str, str, str], Awaitable[DeclaredKeys]]

#: The columns a source declares NOT NULL, from the standard information_schema.
_INFO_SCHEMA_NOT_NULL = """
SELECT column_name FROM information_schema.columns
WHERE table_schema = {schema} AND table_name = {table} AND is_nullable = 'NO'
"""


async def _info_schema_not_null(pool: Any, source_id: str, args: dict[str, str]) -> frozenset[str]:
    rows = (await pool.execute(source_id, _INFO_SCHEMA_NOT_NULL.format(**args), [])).rows
    return frozenset(row[0] for row in rows)


def _grouped(rows: list) -> tuple[UniqueKey, ...]:
    """``(constraint name, column)`` rows in key order, as one key per constraint."""
    keys: dict[str, list[str]] = {}
    for name, column in rows:
        keys.setdefault(name, []).append(column)
    return tuple(UniqueKey(name, tuple(columns)) for name, columns in keys.items())


# --- PostgreSQL and the systems that speak its catalog -------------------------------------------

_INFO_SCHEMA_KEYS = """
SELECT tc.constraint_type, tc.constraint_name, kcu.column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.table_schema = {schema} AND tc.table_name = {table}
  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY tc.constraint_name, kcu.ordinal_position
"""

# A foreign key's columns with the columns they reference, paired by the referenced key's own
# position ({paired}: PostgreSQL's position_in_unique_constraint; SQL Server, which has none,
# lists a foreign key's columns in the order of the key it references).
_INFO_SCHEMA_FOREIGN = """
SELECT kcu.column_name, pk.table_name, pk.column_name
FROM information_schema.referential_constraints AS rc
JOIN information_schema.key_column_usage AS kcu
  ON kcu.constraint_name = rc.constraint_name
 AND kcu.constraint_schema = rc.constraint_schema
JOIN information_schema.key_column_usage AS pk
  ON pk.constraint_name = rc.unique_constraint_name
 AND pk.constraint_schema = rc.unique_constraint_schema
 AND pk.ordinal_position = kcu.{paired}
WHERE kcu.table_schema = {schema} AND kcu.table_name = {table}
ORDER BY kcu.constraint_name, kcu.ordinal_position
"""


def _info_schema_reader(dialect: str, paired: str) -> Reader:
    """The keys from the standard ``information_schema`` (CockroachDB, SQL Server)."""

    async def read(pool: Any, source_id: str, schema: str, table: str) -> DeclaredKeys:
        args = {"schema": sql_literal(schema, dialect), "table": sql_literal(table, dialect)}
        keys = await pool.execute(source_id, _INFO_SCHEMA_KEYS.format(**args), [])
        primary = [(name, col) for kind, name, col in keys.rows if kind == "PRIMARY KEY"]
        unique = [(name, col) for kind, name, col in keys.rows if kind == "UNIQUE"]
        foreign = await pool.execute(
            source_id, _INFO_SCHEMA_FOREIGN.format(paired=paired, **args), []
        )
        return DeclaredKeys(
            primary_key=tuple(col for _name, col in primary),
            unique=_grouped(unique),
            foreign=tuple(ForeignKey(*row) for row in foreign.rows),
            not_null=await _info_schema_not_null(pool, source_id, args),
        )

    return read


# PostgreSQL's own catalog, found by the schema's and table's names. Its information_schema views
# compute every constraint in the database before the table's name narrows them: on a database
# holding tens of thousands of tables (a control plane with an org schema per organization, which
# a PostgreSQL source may be) one foreign-key read took seconds, and a boot that read the keys of
# each registered table never answered (REQ-2170). The catalog also names the keys of a table its
# reader may only SELECT, which information_schema hides from it.
_PG_CATALOG_KEYS = """
SELECT CASE c.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END, c.conname, a.attname
FROM pg_catalog.pg_constraint AS c
JOIN pg_catalog.pg_class AS t ON t.oid = c.conrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.relnamespace
CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, pos)
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
WHERE n.nspname = {schema} AND t.relname = {table} AND c.contype IN ('p', 'u')
ORDER BY c.conname, k.pos
"""

# conkey and confkey list a foreign key's columns and the columns they reference pair by pair.
_PG_CATALOG_FOREIGN = """
SELECT a.attname, rt.relname, ra.attname
FROM pg_catalog.pg_constraint AS c
JOIN pg_catalog.pg_class AS t ON t.oid = c.conrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.relnamespace
JOIN pg_catalog.pg_class AS rt ON rt.oid = c.confrelid
CROSS JOIN LATERAL unnest(c.conkey, c.confkey) WITH ORDINALITY AS k(attnum, refnum, pos)
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
JOIN pg_catalog.pg_attribute AS ra ON ra.attrelid = c.confrelid AND ra.attnum = k.refnum
WHERE n.nspname = {schema} AND t.relname = {table} AND c.contype = 'f'
ORDER BY c.conname, k.pos
"""


async def _pg_catalog(pool: Any, source_id: str, schema: str, table: str) -> DeclaredKeys:
    args = {"schema": sql_literal(schema, "postgres"), "table": sql_literal(table, "postgres")}
    keys = await pool.execute(source_id, _PG_CATALOG_KEYS.format(**args), [])
    primary = [(name, col) for kind, name, col in keys.rows if kind == "PRIMARY KEY"]
    unique = [(name, col) for kind, name, col in keys.rows if kind == "UNIQUE"]
    foreign = await pool.execute(source_id, _PG_CATALOG_FOREIGN.format(**args), [])
    return DeclaredKeys(
        primary_key=tuple(col for _name, col in primary),
        unique=_grouped(unique),
        foreign=tuple(ForeignKey(*row) for row in foreign.rows),
        not_null=await _info_schema_not_null(pool, source_id, args),
    )


# --- MySQL and the systems that speak its catalog ------------------------------------------------

# MySQL names every primary key PRIMARY and records a foreign key's target on the column itself.
_MYSQL_KEYS = """
SELECT tc.constraint_type, tc.constraint_name, kcu.column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.table_schema = {schema} AND tc.table_name = {table}
  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY tc.constraint_name, kcu.ordinal_position
"""

_MYSQL_FOREIGN = """
SELECT column_name, referenced_table_name, referenced_column_name
FROM information_schema.key_column_usage
WHERE table_schema = {schema} AND table_name = {table}
  AND referenced_table_name IS NOT NULL
ORDER BY constraint_name, ordinal_position
"""


async def _mysql(pool: Any, source_id: str, schema: str, table: str) -> DeclaredKeys:
    args = {"schema": sql_literal(schema, "mysql"), "table": sql_literal(table, "mysql")}
    keys = await pool.execute(source_id, _MYSQL_KEYS.format(**args), [])
    primary = [(name, col) for kind, name, col in keys.rows if kind == "PRIMARY KEY"]
    unique = [(name, col) for kind, name, col in keys.rows if kind == "UNIQUE"]
    foreign = await pool.execute(source_id, _MYSQL_FOREIGN.format(**args), [])
    return DeclaredKeys(
        primary_key=tuple(col for _name, col in primary),
        unique=_grouped(unique),
        foreign=tuple(ForeignKey(*row) for row in foreign.rows),
        not_null=await _info_schema_not_null(pool, source_id, args),
    )


# --- SQLite: its PRAGMAs ---------------------------------------------------------------------------


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def _sqlite(pool: Any, source_id: str, _schema: str, table: str) -> DeclaredKeys:
    # table_info: cid, name, type, notnull, dflt_value, pk (its position in the key, 0 if none).
    info = await pool.execute(source_id, f"PRAGMA table_info({_quoted(table)})", [])
    primary = [name for _pos, name in sorted((row[5], row[1]) for row in info.rows if row[5])]
    # index_list: seq, name, unique, origin ('u' a UNIQUE constraint), partial.
    indexes = await pool.execute(source_id, f"PRAGMA index_list({_quoted(table)})", [])
    unique: list[UniqueKey] = []
    for row in indexes.rows:
        if row[2] and row[3] == "u":
            columns = await pool.execute(source_id, f"PRAGMA index_info({_quoted(row[1])})", [])
            unique.append(UniqueKey(row[1], tuple(r[2] for r in columns.rows)))
    # foreign_key_list: id, seq, table, from, to, ... ; ``to`` empty means the target's key.
    foreign_rows = await pool.execute(source_id, f"PRAGMA foreign_key_list({_quoted(table)})", [])
    foreign = []
    for row in foreign_rows.rows:
        target = row[4]
        if not target:
            target_info = await pool.execute(source_id, f"PRAGMA table_info({_quoted(row[2])})", [])
            keyed = sorted((r[5], r[1]) for r in target_info.rows if r[5])
            target = keyed[row[1]][1]
        foreign.append(ForeignKey(row[3], row[2], target))
    return DeclaredKeys(
        primary_key=tuple(primary),
        unique=tuple(unique),
        foreign=tuple(foreign),
        not_null=frozenset(row[1] for row in info.rows if row[3]),
    )


# --- DuckDB: its constraints table function -------------------------------------------------------

_DUCKDB_CONSTRAINTS = """
SELECT constraint_type, constraint_name, constraint_column_names, referenced_table,
       referenced_column_names
FROM duckdb_constraints()
WHERE schema_name = {schema} AND table_name = {table}
  AND constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
ORDER BY constraint_index
"""


async def _duckdb(pool: Any, source_id: str, schema: str, table: str) -> DeclaredKeys:
    args = {"schema": sql_literal(schema, "duckdb"), "table": sql_literal(table, "duckdb")}
    rows = await pool.execute(source_id, _DUCKDB_CONSTRAINTS.format(**args), [])
    primary: tuple[str, ...] = ()
    unique: list[UniqueKey] = []
    foreign: list[ForeignKey] = []
    for kind, name, columns, referenced_table, referenced_columns in rows.rows:
        if kind == "PRIMARY KEY":
            primary = tuple(columns)
        elif kind == "UNIQUE":
            unique.append(UniqueKey(name, tuple(columns)))
        else:
            foreign.extend(
                ForeignKey(column, referenced_table, target)
                for column, target in zip(columns, referenced_columns, strict=True)
            )
    return DeclaredKeys(
        primary_key=primary,
        unique=tuple(unique),
        foreign=tuple(foreign),
        not_null=await _info_schema_not_null(pool, source_id, args),
    )


#: The source kinds whose catalog is read here, by the reader that reads it.
READERS: dict[str, Reader] = {
    **{kind: _pg_catalog for kind in ("postgresql", "postgres", "yugabytedb", "greenplum")},
    # CockroachDB's information_schema is its own virtual table, not PostgreSQL's view.
    "cockroachdb": _info_schema_reader("postgres", "position_in_unique_constraint"),
    "sqlserver": _info_schema_reader("tsql", "ordinal_position"),
    **{kind: _mysql for kind in ("mysql", "mariadb", "singlestore", "tidb")},
    "sqlite": _sqlite,
    "duckdb": _duckdb,
}


async def declared_keys(
    pool: Any, source_type: str, source_id: str, schema: str, table: str
) -> DeclaredKeys | None:
    """What ``source_id`` declares of ``schema.table``'s keys, read over its pooled driver.
    None when it is not read here: a source kind with no reader, or no driver for it in the pool.
    A source that declares nothing answers an empty :class:`DeclaredKeys`."""
    reader = READERS.get(source_type.lower())
    if reader is None or not pool.has(source_id):
        return None
    return await reader(pool, source_id, schema, table)


__all__ = ["READERS", "DeclaredKeys", "ForeignKey", "UniqueKey", "declared_keys"]
