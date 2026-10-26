# Copyright (c) 2026 Kenneth Stott
# Canary: badbdb7d-6a17-48cf-9135-36cfaaaa9441
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgwire catalog and type conformance: Provisa against a real PostgreSQL (REQ-2167).

Run in the pgwire conformance job (.github/workflows/conformance-pgwire.yml), with psycopg against
both servers. Three parts, each written as data -- a difference is a finding, never an exit status:

1. Catalog shape. For every pg_catalog and information_schema relation real PostgreSQL has that
   Provisa serves, the columns Provisa's RowDescription gives for ``SELECT * FROM <rel> LIMIT 0``
   against pg_attribute (name, order, type OID, typmod). For every pg_catalog function (each
   overload whose arguments are not pseudo-types), the call ``fn(NULL::arg, ...)`` through Provisa:
   whether it is served, and its result type against pg_proc.prorettype.
2. Coverage. The relations and functions real PostgreSQL has that Provisa does not serve, and the
   catalog relations Provisa serves that PostgreSQL does not have.
3. Type conversions. For each column of the fixture table (one value of each type, read through
   the source Provisa registers over it) and for the casts tools commonly issue, the type OID,
   typmod and the text and binary encodings Provisa sends, against what PostgreSQL sends for the
   same statement.

The fixture lives in the real PostgreSQL at ``conformance.types_fixture``; Provisa's model presents
it under exactly that name (domain ``conformance`` as the schema, the table under its own name), so
the same statement runs unchanged on both servers.

Usage:
  pgwire_catalog_types.py seed   --pg <dsn>
  pgwire_catalog_types.py config --out <provisa.yaml> --pg-port <port>
  pgwire_catalog_types.py probe  --pg <dsn> --provisa <dsn> --out-json <f> --out-md <f>
"""

# Requirements: REQ-2167

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import pq

FIXTURE_SCHEMA = "conformance"
FIXTURE_TABLE = "types_fixture"

# (column, PostgreSQL type, literal) -- one value of each type a source column can carry.
FIXTURE: list[tuple[str, str, str]] = [
    ("c_bool", "boolean", "true"),
    ("c_int2", "smallint", "32767"),
    ("c_int4", "integer", "2147483647"),
    ("c_int8", "bigint", "9223372036854775807"),
    ("c_float4", "real", "3.14"),
    ("c_float8", "double precision", "2.718281828459045"),
    ("c_numeric", "numeric", "12345.6789"),
    ("c_numeric_10_2", "numeric(10,2)", "12345.67"),
    ("c_text", "text", "'hello pgwire'"),
    ("c_varchar", "character varying", "'varchar value'"),
    ("c_varchar_20", "character varying(20)", "'bounded'"),
    ("c_bpchar_5", "character(5)", "'ab'"),
    ("c_bytea", "bytea", "'\\xdeadbeef'"),
    ("c_date", "date", "'2024-06-15'"),
    ("c_time", "time without time zone", "'12:30:45.123456'"),
    ("c_timetz", "time with time zone", "'12:30:45+02'"),
    ("c_timestamp", "timestamp without time zone", "'2024-06-15 12:30:45.123456'"),
    ("c_timestamptz", "timestamp with time zone", "'2024-06-15 12:30:45.123456+00'"),
    ("c_interval", "interval", "'1 day 02:03:04'"),
    ("c_uuid", "uuid", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"),
    ("c_json", "json", '\'{"key": "value", "n": 1}\''),
    ("c_jsonb", "jsonb", '\'{"key": "value", "n": 1}\''),
    ("c_inet", "inet", "'192.168.0.1/24'"),
    ("c_cidr", "cidr", "'10.0.0.0/8'"),
    ("c_macaddr", "macaddr", "'08:00:2b:01:02:03'"),
    ("c_int4_array", "integer[]", "'{1,2,3}'"),
    ("c_text_array", "text[]", "'{a,b,c}'"),
    ("c_float8_array", "double precision[]", "'{1.5,2.5}'"),
    ("c_oid", "oid", "12345"),
    ("c_money", "money", "'12.34'"),
    ("c_xml", "xml", "'<a>1</a>'"),
    ("c_point", "point", "'(1,2)'"),
]

# Expressions tools issue as they connect and introspect (JDBC, DBeaver, psql, BI tools), each
# run as ``SELECT <expr> AS v`` on both servers.
CASTS: list[str] = [
    "1::int2",
    "1::int4",
    "1::int8",
    "1.5::float4",
    "1.5::float8",
    "'12345.6789'::numeric",
    "'12345.6789'::numeric(10,2)",
    "'x'::text",
    "'x'::varchar",
    "'x'::varchar(10)",
    "'x'::char(3)",
    "'x'::bpchar",
    "'x'::name",
    "true",
    "'t'::boolean",
    "'2024-06-15'::date",
    "'12:30:45'::time",
    "'2024-06-15 12:30:45'::timestamp",
    "'2024-06-15 12:30:45+00'::timestamptz",
    "'1 day'::interval",
    "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
    "'{\"a\": 1}'::json",
    "'{\"a\": 1}'::jsonb",
    "'\\xdeadbeef'::bytea",
    "ARRAY[1,2,3]",
    "ARRAY['a','b']",
    "'{1,2}'::int4[]",
    "'{a,b}'::text[]",
    "1::oid",
    "23::regtype",
    "'int4'::regtype",
    "23::regtype::text",
    "'pg_catalog.pg_class'::regclass",
    "'pg_catalog.pg_class'::regclass::oid",
    "'pg_catalog'::regnamespace",
    "1::text",
    "1::varchar",
    "1::bigint::text",
    "'1'::int4",
    "NULL",
    "NULL::int4",
    "NULL::text",
    "NULL::timestamptz",
    "'192.168.0.1'::inet",
    "current_schema()",
    "current_database()",
    "current_user",
    "session_user",
    "pg_catalog.format_type(23, NULL)",
    "pg_catalog.format_type(1043, 24)",
    "pg_catalog.pg_get_userbyid(10)",
    "pg_catalog.has_schema_privilege('pg_catalog', 'USAGE')",
    "pg_catalog.pg_encoding_to_char(6)",
    "pg_catalog.obj_description(0)",
    "pg_catalog.col_description(0, 0)",
    "pg_catalog.pg_table_is_visible(0)",
    "pg_catalog.array_to_string(ARRAY[1,2], ',')",
    "pg_catalog.quote_ident('x')",
    "pg_catalog.set_config('application_name', 'conformance', false)",
    "pg_catalog.current_setting('server_version_num')",
    "pg_catalog.current_setting('standard_conforming_strings')",
]

_REL_KINDS = "('r','v','m','f','p')"


# --- connections ------------------------------------------------------------------------------


class Server:
    """A raw libpq connection that reconnects when the server dropped it."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.conn = psycopg.connect(dsn, autocommit=True)

    def _ensure(self) -> None:
        if self.conn.closed or self.conn.pgconn.status != pq.ConnStatus.OK:
            try:
                self.conn.close()
            finally:
                self.conn = psycopg.connect(self.dsn, autocommit=True)

    def run(self, sql: str, result_format: int = 0) -> dict[str, Any]:
        """One statement over the extended protocol: its fields, first row, or its error."""
        try:
            self._ensure()
            res = self.conn.pgconn.exec_params(sql.encode(), [], result_format=result_format)
        except psycopg.Error as exc:
            return {"ok": False, "error": f"connection: {exc}".strip()}
        if res.status not in (pq.ExecStatus.TUPLES_OK, pq.ExecStatus.COMMAND_OK):
            msg = (res.error_message or b"").decode(errors="replace").strip()
            return {"ok": False, "error": msg[:400]}
        fields = [
            {
                "name": (res.fname(i) or b"").decode(),
                "type_oid": res.ftype(i),
                "typmod": res.fmod(i),
                "format": res.fformat(i),
            }
            for i in range(res.nfields)
        ]
        row = None
        if res.ntuples:
            row = [res.get_value(0, i) for i in range(res.nfields)]
        return {"ok": True, "fields": fields, "row": row}

    def rows(self, sql: str) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


def _encode(value: bytes | None) -> str | None:
    return None if value is None else value.hex()


def _text(value: bytes | None) -> str | None:
    return None if value is None else value.decode(errors="replace")


# --- seed -------------------------------------------------------------------------------------


def seed(pg_dsn: str) -> None:
    cols = ", ".join(f"{name} {typ}" for name, typ, _ in FIXTURE)
    vals = ", ".join(lit for _, _, lit in FIXTURE)
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {FIXTURE_SCHEMA}")
        conn.execute(f"DROP TABLE IF EXISTS {FIXTURE_SCHEMA}.{FIXTURE_TABLE}")
        conn.execute(f"CREATE TABLE {FIXTURE_SCHEMA}.{FIXTURE_TABLE} ({cols})")
        conn.execute(f"INSERT INTO {FIXTURE_SCHEMA}.{FIXTURE_TABLE} VALUES ({vals})")
    print(f"seeded {FIXTURE_SCHEMA}.{FIXTURE_TABLE}: {len(FIXTURE)} columns, one row")


def registered_type(pg_type: str) -> str:
    """The type a PostgreSQL source column of ``pg_type`` registers as, decided as registration
    decides it (provisa/api/admin/introspect.py ``_pg_column_typing``): an array by
    ``column_typing`` (JSON, REQ-1965), a sized decimal keeping its size (REQ-1961), any other
    scalar by ``to_ir``. Raises ValueError for a type registration refuses."""
    from provisa.core.ir_types import column_typing, to_ir
    from provisa.core.numeric_type import numeric_size_of

    typing = column_typing(pg_type, "postgresql")
    if typing.native_type or numeric_size_of(typing.data_type) is not None:
        return typing.data_type
    return to_ir(pg_type)


def registrable_columns() -> tuple[list[tuple[str, str, str]], dict[str, str]]:
    """The fixture's columns as ``[(column, pg type, registered type)]``, and those registration
    refuses, each with why. A refused column stays in PostgreSQL's fixture but not in the model:
    the type report names it rather than the boot failing on it."""
    registered: list[tuple[str, str, str]] = []
    refused: dict[str, str] = {}
    for name, typ, _ in FIXTURE:
        try:
            registered.append((name, typ, registered_type(typ)))
        except ValueError as exc:
            refused[name] = str(exc).split(":")[0]
    return registered, refused


def provisa_config(pg_port: int) -> dict[str, Any]:
    """The model the conformance instance boots with.

    No auth provider, so pgwire is in trust mode and the startup user names the role
    (``org_admin``). One domain, ``conformance``, which pgwire presents as the schema; one
    PostgreSQL source over the job's real PostgreSQL; one table, ``types_fixture``, registered from
    that source's ``conformance`` schema under its own name and alias, every registrable column
    typed as registration types a PostgreSQL column (``registered_type``) and visible to
    ``org_admin``. So ``conformance.types_fixture`` names the same relation on both servers. A
    column whose type registration refuses is left out of the model and named in the report.
    pgspec needs nothing here: every table it touches it creates itself as a session temporary
    table."""
    registered, _refused = registrable_columns()
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": "org_admin"}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": FIXTURE_SCHEMA, "description": "pgwire conformance fixtures"}],
        "roles": [],
        "sources": [
            {
                "id": "conformance-pg",
                "type": "postgresql",
                "host": "${env:CONFORMANCE_PG_HOST}",
                "port": pg_port,
                "database": "${env:CONFORMANCE_PG_DATABASE}",
                "username": "${env:CONFORMANCE_PG_USER}",
                "password": "${env:CONFORMANCE_PG_PASSWORD}",
            }
        ],
        "tables": [
            {
                "source_id": "conformance-pg",
                "schema": FIXTURE_SCHEMA,
                "table": FIXTURE_TABLE,
                "alias": FIXTURE_TABLE,
                "domain_id": FIXTURE_SCHEMA,
                "columns": [
                    {
                        "name": name,
                        # A column is registered with its type (REQ-1426), as registration
                        # types a PostgreSQL source's column.
                        "data_type": ir_type,
                        "visible_to": ["org_admin"],
                    }
                    for name, _typ, ir_type in registered
                ],
            }
        ],
    }


def write_config(path: Path, pg_port: int) -> None:
    import yaml

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(provisa_config(pg_port), sort_keys=False))
    print(f"wrote {path}")


# --- part 1 and 2: catalog shape and coverage -------------------------------------------------


def _type_names(pg: Server) -> dict[int, str]:
    return {oid: name for oid, name in pg.rows("SELECT oid::int8, typname::text FROM pg_type")}


def _pg_relations(pg: Server) -> dict[str, list[dict]]:
    rels = pg.rows(
        "SELECT n.nspname::text, c.relname::text, a.attname::text, a.attnum::int, "
        "a.atttypid::int8, a.atttypmod::int "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
        f"WHERE n.nspname IN ('pg_catalog', 'information_schema') AND c.relkind IN {_REL_KINDS} "
        "ORDER BY 1, 2, 4"
    )
    out: dict[str, list[dict]] = {}
    for nsp, rel, att, _num, typ, mod in rels:
        out.setdefault(f"{nsp}.{rel}", []).append({"name": att, "type_oid": typ, "typmod": mod})
    return out


def _served_only_relations(pg_rels: dict[str, list[dict]]) -> list[str]:
    """Catalog relations Provisa's pgwire catalog declares that PostgreSQL does not have."""
    from provisa.pgwire.catalog_data import _TABLE_MAP

    return sorted(f"{s}.{r}" for s, r in _TABLE_MAP if f"{s}.{r}" not in pg_rels)


def _compare_columns(expected: list[dict], got: list[dict], names: dict[int, str]) -> list[str]:
    diffs: list[str] = []
    exp_names = [c["name"] for c in expected]
    got_names = [c["name"] for c in got]
    missing = [n for n in exp_names if n not in got_names]
    extra = [n for n in got_names if n not in exp_names]
    if missing:
        diffs.append("missing columns: " + ", ".join(missing))
    if extra:
        diffs.append("extra columns: " + ", ".join(extra))
    common_exp = [n for n in exp_names if n in got_names]
    common_got = [n for n in got_names if n in exp_names]
    if common_exp != common_got:
        diffs.append("column order differs")
    by_name = {c["name"]: c for c in got}
    for col in expected:
        g = by_name.get(col["name"])
        if g is None:
            continue
        if g["type_oid"] != col["type_oid"]:
            diffs.append(
                f"{col['name']}: type {names.get(g['type_oid'], g['type_oid'])} "
                f"(oid {g['type_oid']}), PostgreSQL "
                f"{names.get(col['type_oid'], col['type_oid'])} (oid {col['type_oid']})"
            )
        elif g["typmod"] != col["typmod"]:
            diffs.append(f"{col['name']}: typmod {g['typmod']}, PostgreSQL {col['typmod']}")
    return diffs


def catalog_relations(pg: Server, provisa: Server, names: dict[int, str]) -> dict[str, Any]:
    pg_rels = _pg_relations(pg)
    matching: list[str] = []
    differing: dict[str, list[str]] = {}
    not_served: dict[str, str] = {}
    for rel, expected in pg_rels.items():
        got = provisa.run(f"SELECT * FROM {rel} LIMIT 0")
        if not got["ok"]:
            not_served[rel] = got["error"]
            continue
        diffs = _compare_columns(expected, got["fields"], names)
        if diffs:
            differing[rel] = diffs
        else:
            matching.append(rel)
    return {
        "postgres_relations": len(pg_rels),
        "served": len(matching) + len(differing),
        "matching": sorted(matching),
        "differing": dict(sorted(differing.items())),
        "not_served": dict(sorted(not_served.items())),
        "served_not_in_postgres": _served_only_relations(pg_rels),
    }


def _pg_functions(pg: Server) -> list[dict]:
    rows = pg.rows(
        "SELECT p.proname::text, pg_get_function_identity_arguments(p.oid)::text, "
        "p.prokind::text, p.proretset, p.prorettype::int8, "
        "ARRAY(SELECT format_type(t, NULL) FROM unnest(p.proargtypes::oid[]) WITH ORDINALITY u(t, i) "
        "      ORDER BY i)::text[] "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'pg_catalog' AND p.prokind IN ('f', 'a') "
        "AND NOT EXISTS (SELECT 1 FROM unnest(p.proargtypes::oid[]) t JOIN pg_type ty ON ty.oid = t "
        "                WHERE ty.typtype = 'p') "
        "AND p.prorettype NOT IN ('internal'::regtype, 'cstring'::regtype) "
        "ORDER BY 1, 2"
    )
    return [
        {
            "name": name,
            "signature": f"{name}({args})",
            "kind": kind,
            "returns_set": retset,
            "rettype": rettype,
            "argtypes": list(argtypes),
        }
        for name, args, kind, retset, rettype, argtypes in rows
    ]


def catalog_functions(pg: Server, provisa: Server, names: dict[int, str]) -> dict[str, Any]:
    fns = _pg_functions(pg)
    matching: list[str] = []
    differing: dict[str, str] = {}
    not_served: dict[str, str] = {}
    # A call PostgreSQL itself refuses in this form (an ordered-set aggregate needs WITHIN GROUP,
    # a few refuse a NULL of their argument type) says nothing about Provisa: not probed.
    not_probed: dict[str, str] = {}
    for fn in fns:
        args = ", ".join(f"NULL::{t}" for t in fn["argtypes"])
        sql = f'SELECT pg_catalog."{fn["name"]}"({args}) AS v'
        want = pg.run(sql)
        if not want["ok"] or not want["fields"]:
            not_probed[fn["signature"]] = want.get("error") or "no result column"
            continue
        got = provisa.run(sql)
        if not got["ok"]:
            not_served[fn["signature"]] = got["error"]
            continue
        expected = want["fields"][0]["type_oid"]
        oid = got["fields"][0]["type_oid"] if got["fields"] else None
        if oid != expected:
            differing[fn["signature"]] = (
                f"returns {names.get(oid, oid)} (oid {oid}), PostgreSQL "
                f"{names.get(expected, expected)} (oid {expected})"
            )
        else:
            matching.append(fn["signature"])
    return {
        "postgres_functions": len(fns) - len(not_probed),
        "excluded": (
            "overloads taking a pseudo-type argument, type I/O (internal, cstring), and calls "
            "PostgreSQL itself refuses as fn(NULL::arg, ...)"
        ),
        "not_probed": dict(sorted(not_probed.items())),
        "served": len(matching) + len(differing),
        "matching": len(matching),
        "differing": dict(sorted(differing.items())),
        "not_served": dict(sorted(not_served.items())),
    }


# --- part 3: type conversions -----------------------------------------------------------------


def _compare_statement(pg: Server, provisa: Server, sql: str, names: dict[int, str]) -> dict:
    entry: dict[str, Any] = {"statement": sql}
    diffs: list[str] = []
    for fmt, label in ((0, "text"), (1, "binary")):
        want = pg.run(sql, result_format=fmt)
        got = provisa.run(sql, result_format=fmt)
        if not want["ok"]:
            entry[f"postgres_{label}_error"] = want["error"]
            continue
        if not got["ok"]:
            diffs.append(f"{label}: Provisa error: {got['error']}")
            continue
        if not got["fields"]:
            diffs.append(f"{label}: Provisa sent no RowDescription column")
            continue
        w, g = want["fields"][0], got["fields"][0]
        wv = want["row"][0] if want["row"] else None
        gv = got["row"][0] if got["row"] else None
        if fmt == 0:
            entry["postgres_type"] = names.get(w["type_oid"], w["type_oid"])
            entry["provisa_type"] = names.get(g["type_oid"], g["type_oid"])
            if g["type_oid"] != w["type_oid"]:
                diffs.append(
                    f"type oid {g['type_oid']} ({entry['provisa_type']}), PostgreSQL "
                    f"{w['type_oid']} ({entry['postgres_type']})"
                )
            if g["typmod"] != w["typmod"]:
                diffs.append(f"typmod {g['typmod']}, PostgreSQL {w['typmod']}")
            if gv != wv:
                diffs.append(f"text {_text(gv)!r}, PostgreSQL {_text(wv)!r}")
        else:
            if g["format"] != 1:
                diffs.append(f"binary requested, Provisa sent format {g['format']}")
            elif gv != wv:
                diffs.append(f"binary {_encode(gv)}, PostgreSQL {_encode(wv)}")
    entry["differences"] = diffs
    return entry


def type_conversions(pg: Server, provisa: Server, names: dict[int, str]) -> dict[str, Any]:
    for server in (pg, provisa):
        server.run("SET TimeZone TO 'UTC'")
        server.run("SET DateStyle TO 'ISO, MDY'")
        server.run("SET IntervalStyle TO 'postgres'")
        server.run("SET extra_float_digits TO 1")
    registered, refused = registrable_columns()
    model_type = {col: ir for col, _, ir in registered}
    fixture = []
    for col, typ, _ in FIXTURE:
        sql = f"SELECT {col} FROM {FIXTURE_SCHEMA}.{FIXTURE_TABLE}"
        if col in refused:
            fixture.append(
                {
                    "statement": sql,
                    "column": col,
                    "declared": typ,
                    "differences": [f"not registrable as a source column: {refused[col]}"],
                }
            )
            continue
        fixture.append(
            _compare_statement(pg, provisa, sql, names)
            | {"column": col, "declared": typ, "registered_as": model_type[col]}
        )
    casts = [_compare_statement(pg, provisa, f"SELECT {expr} AS v", names) for expr in CASTS]
    rows = fixture + casts
    return {
        "fixture_columns": fixture,
        "casts": casts,
        "compared": len(rows),
        "matching": sum(1 for r in rows if not r["differences"]),
        "differing": sum(1 for r in rows if r["differences"]),
    }


# --- report -----------------------------------------------------------------------------------


def _md_list(items: dict[str, Any] | list[str], limit: int = 400) -> list[str]:
    lines: list[str] = []
    entries = list(items.items()) if isinstance(items, dict) else [(i, None) for i in items]
    for key, val in entries[:limit]:
        if isinstance(val, list):
            lines.append(f"- `{key}`: " + "; ".join(val).replace("|", "\\|"))
        elif val:
            lines.append(f"- `{key}`: " + str(val).splitlines()[0][:200].replace("|", "\\|"))
        else:
            lines.append(f"- `{key}`")
    if len(entries) > limit:
        lines.append(f"- ... {len(entries) - limit} more in conformance-pgwire.json")
    return lines


def _details(title: str, body: list[str]) -> list[str]:
    return ["<details><summary>" + title + "</summary>", "", *body, "", "</details>", ""]


def markdown(report: dict[str, Any]) -> str:
    rel = report["catalog_relations"]
    fn = report["catalog_functions"]
    ty = report["type_conversions"]
    out = [
        f"## pgwire catalog shape (PostgreSQL {report['postgres_version']})",
        "",
        "| | PostgreSQL | served by Provisa | matching | differing |",
        "|---|---:|---:|---:|---:|",
        f"| relations | {rel['postgres_relations']} | {rel['served']} | {len(rel['matching'])} "
        f"| {len(rel['differing'])} |",
        f"| functions | {fn['postgres_functions']} | {fn['served']} | {fn['matching']} "
        f"| {len(fn['differing'])} |",
        "",
        f"Functions compared: every pg_catalog overload except {fn['excluded']} "
        f"({len(fn.get('not_probed', {}))} of those, listed in conformance-pgwire.json).",
        "",
    ]
    out += _details(f"Relations that differ ({len(rel['differing'])})", _md_list(rel["differing"]))
    out += _details(
        f"Functions whose result type differs ({len(fn['differing'])})", _md_list(fn["differing"])
    )
    out += [f"## pgwire catalog coverage (PostgreSQL {report['postgres_version']})", ""]
    out += _details(
        f"Relations PostgreSQL has that Provisa does not serve ({len(rel['not_served'])})",
        _md_list(rel["not_served"]),
    )
    out += _details(
        f"Functions PostgreSQL has that Provisa does not serve ({len(fn['not_served'])})",
        _md_list(list(fn["not_served"]), limit=3000),
    )
    out += _details(
        "Catalog relations Provisa serves that PostgreSQL does not have "
        f"({len(rel['served_not_in_postgres'])})",
        _md_list(rel["served_not_in_postgres"]),
    )
    out += [
        f"## pgwire type conversions (PostgreSQL {report['postgres_version']})",
        "",
        f"{ty['compared']} statements compared (fixture columns and tool casts): "
        f"{ty['matching']} identical in type OID, typmod, text and binary encoding; "
        f"{ty['differing']} differ.",
        "",
        "| statement | PostgreSQL type | Provisa type | differences |",
        "|---|---|---|---|",
    ]
    for row in ty["fixture_columns"] + ty["casts"]:
        if not row["differences"]:
            continue
        diffs = "; ".join(row["differences"]).replace("|", "\\|").replace("\n", " ")[:600]
        out.append(
            f"| `{row['statement']}` | {row.get('postgres_type', '')} "
            f"| {row.get('provisa_type', '')} | {diffs} |"
        )
    out.append("")
    return "\n".join(out)


def probe(pg_dsn: str, provisa_dsn: str, out_json: Path, out_md: Path) -> None:
    pg = Server(pg_dsn)
    provisa = Server(provisa_dsn)
    names = _type_names(pg)
    started = time.monotonic()
    report: dict[str, Any] = {
        "postgres_version": pg.rows("SHOW server_version")[0][0],
        "catalog_relations": catalog_relations(pg, provisa, names),
        "catalog_functions": catalog_functions(pg, provisa, names),
        "type_conversions": type_conversions(pg, provisa, names),
    }
    report["seconds"] = round(time.monotonic() - started, 1)
    out_json.write_text(json.dumps(report, indent=2, default=str) + "\n")
    out_md.write_text(markdown(report) + "\n")
    print(markdown(report))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--pg", required=True)
    c = sub.add_parser("config")
    c.add_argument("--out", required=True, type=Path)
    c.add_argument("--pg-port", required=True, type=int)
    p = sub.add_parser("probe")
    p.add_argument("--pg", required=True)
    p.add_argument("--provisa", required=True)
    p.add_argument("--out-json", required=True, type=Path)
    p.add_argument("--out-md", required=True, type=Path)
    args = parser.parse_args(argv[1:])
    if args.command == "seed":
        seed(args.pg)
    elif args.command == "config":
        write_config(args.out, args.pg_port)
    else:
        probe(args.pg, args.provisa, args.out_json, args.out_md)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
