# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2c6d14-3b7e-4a90-b1d5-6e0c94a7f382
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Warm catalog reads on a large model (REQ-2190, REQ-127): the statements client tools send, timed
over pgwire on a native local instance of the conformance model plus 1,000 tables of 20 columns and
333 relationships.

Usage (each through ~/.provisa-agent/heavy.sh; ports are isolated, never local-dev's):
  pgwire_catalog_timing.py model   --dir <work>
  pgwire_catalog_timing.py start   --dir <work>
  pgwire_catalog_timing.py measure --user conformance --password conformance-password --out <f>
  pgwire_catalog_timing.py stop    --dir <work>

``measure`` runs each statement five times and reports the first run and the median of the rest. A
statement whose table changes on each run (``*_each``) is the warm cost a tool walking a schema
pays, which no cache of a statement's text can answer. Half the tables are in a domain the
restricted role (user ``analyst``) does not reach."""

# Requirements: REQ-2190, REQ-127

from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg
import yaml

sys.path.insert(0, str(Path(__file__).parent / "conformance"))
from pgwire_catalog_types import registered_typing  # noqa: E402

_TABLES = 1000
_TEXT_COLUMNS = 18
_ROLES = ["sales_reader", "org_admin"]
_API_PORT = 18931
_PGWIRE_PORT = 18936
_PORTS = {
    "GRPC_PORT": "18932",
    "FLIGHT_PORT": "18933",
    "PROVISA_BOLT_PORT": "18934",
    "PROVISA_MCP_PORT": "18935",
}
_SUPERUSER = ("conformance-su", "conformance-su-password")
_USERS = (("conformance", "conformance-password"), ("analyst", "analyst-password"))
_REPO = Path(__file__).resolve().parent.parent
_PYTHON = str(_REPO / ".venv" / "bin" / "python")

_GET_TABLES = (
    "SELECT n.nspname, c.relname, d.description FROM pg_catalog.pg_namespace n, "
    "pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid "
    "AND d.objsubid = 0 AND d.classoid = 'pg_class'::regclass) "
    "WHERE c.relnamespace = n.oid AND c.relkind IN ('r','p') ORDER BY 1,2"
)
_GET_COLUMNS = (
    "SELECT n.nspname, c.relname, a.attname, a.atttypid, "
    "pg_catalog.format_type(a.atttypid,a.atttypmod), a.attnotnull "
    "FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) "
    "JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid) "
    "WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relname = '{table}'"
)
_PSQL_DT = (
    "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c "
    "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r','p') AND n.nspname <> 'pg_catalog' "
    "AND n.nspname <> 'information_schema' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2"
)
_GET_INDEX_INFO = (
    "SELECT tmp.TABLE_CAT, tmp.TABLE_SCHEM, tmp.TABLE_NAME, tmp.NON_UNIQUE, tmp.INDEX_QUALIFIER, "
    "tmp.INDEX_NAME, tmp.TYPE, tmp.ORDINAL_POSITION, "
    "trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) "
    "AS COLUMN_NAME, CASE tmp.AM_NAME WHEN 'btree' THEN "
    "CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1::smallint WHEN 1 THEN 'D' ELSE 'A' END "
    "ELSE NULL END AS ASC_OR_DESC, tmp.CARDINALITY, tmp.PAGES, tmp.FILTER_CONDITION FROM "
    "(SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME, "
    "NOT i.indisunique AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, "
    "CASE i.indisclustered WHEN true THEN 1 ELSE CASE am.amname WHEN 'hash' THEN 2 ELSE 3 END END "
    "AS TYPE, (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION, "
    "ci.reltuples AS CARDINALITY, ci.relpages AS PAGES, "
    "pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION, ci.oid AS CI_OID, "
    "i.indoption AS I_INDOPTION, am.amname AS AM_NAME FROM pg_catalog.pg_class ct "
    "JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
    "JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid) "
    "JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) "
    "JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) "
    "WHERE true AND n.nspname = 'sales' AND ct.relname = '{table}') AS tmp "
    "ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION"
)
_FOREIGN_KEYS = (
    "SELECT c.conname, pg_catalog.pg_get_constraintdef(c.oid) FROM pg_catalog.pg_constraint c "
    "WHERE c.contype = 'f'"
)
_ONE_DEFINITION = (
    "SELECT c.conname, pg_catalog.pg_get_constraintdef(c.oid) FROM pg_catalog.pg_constraint c "
    "JOIN pg_catalog.pg_class t ON t.oid = c.conrelid WHERE t.relname = '{table}'"
)
_IS_TABLES = "SELECT table_schema, table_name FROM information_schema.tables"
_IS_COLUMNS = "SELECT table_name, column_name FROM information_schema.columns"

#: Every run names the same statement; a table of the sales domain, which both roles reach.
_STATEMENTS = {
    "getTables": _GET_TABLES,
    "getColumns_one": _GET_COLUMNS.format(table="t0000"),
    "getColumns_each": _GET_COLUMNS,
    "psql_dt": _PSQL_DT,
    "is_tables": _IS_TABLES,
    "is_columns_all": _IS_COLUMNS,
    "getIndexInfo_one": _GET_INDEX_INFO.format(table="t0000"),
    "getIndexInfo_each": _GET_INDEX_INFO,
    "constraintdef_each": _ONE_DEFINITION,
    "fk_defs_all": _FOREIGN_KEYS,
}
_RUNS = 5


def _column(name: str, pg_type: str, roles: list[str], key: bool = False) -> dict:
    column = {"name": name, **registered_typing(pg_type), "visible_to": roles}
    if key:
        column["is_primary_key"] = True
    return column


def _model(work: Path) -> None:
    """The 1,000 tables, as DDL and as the conformance config's extra tables."""
    tables, relationships, ddl = [], [], []
    text_columns = ", ".join(f"c{j:02d} text" for j in range(_TEXT_COLUMNS))
    for i in range(_TABLES):
        name = f"t{i:04d}"
        domain = "sales" if i % 2 == 0 else "hr"
        roles = _ROLES if i % 2 == 0 else ["org_admin"]
        columns = [_column("id", "integer", roles, True), _column("parent_id", "integer", roles)]
        columns += [_column(f"c{j:02d}", "text", roles) for j in range(_TEXT_COLUMNS)]
        tables.append(
            {
                "source_id": "conformance-pg",
                "schema": "conformance",
                "table": name,
                "alias": name,
                "domain_id": domain,
                "columns": columns,
            }
        )
        ddl.append(
            f"CREATE TABLE conformance.{name} (id int primary key, parent_id int, {text_columns})"
        )
        if i > 0 and i % 3 == 0:
            relationships.append(
                {
                    "id": f"r{i}",
                    "alias": f"p{i}",
                    "cardinality": "many-to-one",
                    "source_table_id": name,
                    "source_column": "parent_id",
                    "target_table_id": f"t{i - 1:04d}",
                    "target_column": "id",
                }
            )
    access = {
        "domains": [
            {"id": "sales", "description": "sales"},
            {"id": "hr", "description": "hr"},
        ],
        "roles": [
            {
                "id": "sales_reader",
                "description": "reaches sales alone",
                "parents": ["viewer"],
                "capabilities": ["query_development", "full_results"],
                "domain_access": ["sales"],
            }
        ],
        "role_mapping": [
            {"claim": "username", "type": "exact", "value": "analyst", "role": "sales_reader"}
        ],
    }
    work.mkdir(parents=True, exist_ok=True)
    (work / "tables.yaml").write_text(yaml.safe_dump(tables))
    (work / "relationships.yaml").write_text(yaml.safe_dump(relationships))
    (work / "access.yaml").write_text(yaml.safe_dump(access))
    (work / "model.sql").write_text(";\n".join(ddl))


def _conformance(*args: str, check: bool = True) -> None:
    command = [_PYTHON, str(_REPO / "scripts/conformance/pgwire_catalog_types.py"), *args]
    subprocess.run(command, cwd=_REPO, check=check)


def _wait(url: str, attempts: int, *, pause: float = 2.0) -> None:
    for _ in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=5) as answer:
                if answer.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(pause)
    raise TimeoutError(url)


def _admin(method: str, path: str, body: bytes, content_type: str) -> None:
    credentials = base64.b64encode(":".join(_SUPERUSER).encode()).decode()
    request = urllib.request.Request(
        f"http://127.0.0.1:{_API_PORT}{path}",
        data=body,
        method=method,
        headers={"Authorization": f"Basic {credentials}", "Content-Type": content_type},
    )
    for attempt in range(6):
        try:
            with urllib.request.urlopen(request, timeout=1800):
                return
        except urllib.error.HTTPError:
            raise
        except urllib.error.URLError:
            if attempt == 5:
                raise
            time.sleep(3)


def _control_plane(home: Path) -> dict[str, str]:
    started = subprocess.check_output(
        [
            _PYTHON,
            "-m",
            "provisa.core.control_plane_pg",
            "start",
            str(home / "control-pg"),
            "--init-sql",
            "db/init.sql",
        ],
        cwd=_REPO,
        text=True,
    )
    settings = dict(line.split("=", 1) for line in started.splitlines() if "=" in line)
    (home / "control-plane.json").write_text(json.dumps(settings))
    return settings


def _seed(work: Path, home: Path, plane: dict[str, str]) -> str:
    host, port, password = plane["PG_HOST"], plane["PG_PORT"], plane["PG_PASSWORD"]
    server = {"host": host, "port": port, "user": "provisa", "password": password}
    with psycopg.connect(dbname="provisa", autocommit=True, **server) as conn:
        conn.execute("CREATE DATABASE conformance")
    dsn = f"host={host} port={port} dbname=conformance user=provisa password={password}"
    # The fixture's table is what is timed against; its one row holds an xml value, which the
    # embedded server, built without libxml, refuses after the table exists.
    _conformance("seed", "--pg", dsn, check=False)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS conformance")
        for statement in (work / "model.sql").read_text().split(";\n"):
            conn.execute(statement)
    config = home / "provisa.yaml"
    _conformance(
        "config",
        "--out",
        str(config),
        "--pg-port",
        port,
        "--pg",
        dsn,
        "--extra-tables",
        str(work / "tables.yaml"),
        "--extra-relationships",
        str(work / "relationships.yaml"),
        "--extra-access",
        str(work / "access.yaml"),
    )
    config.write_text(
        "\n".join(
            f"{line[: line.index('host:')]}host: 127.0.0.1" if "host:" in line else line
            for line in config.read_text().splitlines()
        )
    )
    return f"postgresql+psycopg://provisa:{password}@/provisa?host={host}&port={port}"


def _start(work: Path) -> None:
    home = work / "home"
    if home.exists():
        shutil.rmtree(home)
    home.mkdir(parents=True)
    plane = _control_plane(home)
    url = _seed(work, home, plane)
    environment = {
        **os.environ,
        **_PORTS,
        "PROVISA_API_PORT": str(_API_PORT),
        "PROVISA_PGWIRE_PORT": str(_PGWIRE_PORT),
        "PROVISA_DATA_DIR": str(home),
        "PROVISA_TELEMETRY_DIR": str(home / "telemetry"),
        "PROVISA_CONFIG": str(home / "provisa.yaml"),
        "PROVISA_HOME": str(home),
        "PROVISA_LICENSING_SANDBOX_DIR": str(home / "licensing"),
        "PYTHON_KEYRING_BACKEND": "tests._memory_keyring.MemoryKeyring",
        "PROVISA_ENCRYPTION_KEY": base64.b64encode(secrets.token_bytes(32)).decode(),
        "ORG_ID": "conformance",
        "TENANT_DATABASE_URL": url,
        "PLATFORM_DATABASE_URL": url,
        "PROVISA_SUPERUSER_USERNAME": _SUPERUSER[0],
        "PROVISA_SUPERUSER_PASSWORD": _SUPERUSER[1],
        "CONFORMANCE_JWT_SECRET": "conformance-jwt-signing-key-not-a-secret-0000",
        "CONFORMANCE_PG_HOST": plane["PG_HOST"],
        "CONFORMANCE_PG_PORT": plane["PG_PORT"],
        "CONFORMANCE_PG_DATABASE": "conformance",
        "CONFORMANCE_PG_USER": "provisa",
        "CONFORMANCE_PG_PASSWORD": plane["PG_PASSWORD"],
    }
    with open(home / "server.log", "wb") as log:
        server = subprocess.Popen(
            [
                str(_REPO / ".venv/bin/uvicorn"),
                "main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(_API_PORT),
            ],
            cwd=_REPO,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    (home / "server.pid").write_text(str(server.pid))
    _wait(f"http://127.0.0.1:{_API_PORT}/health", 600)
    _admin(
        "PUT",
        "/admin/config",
        (home / "provisa.yaml").read_bytes(),
        "application/yaml",
    )
    _wait(f"http://127.0.0.1:{_API_PORT}/ready", 600)
    for username, password in _USERS:
        body = json.dumps({"username": username, "password": password}).encode()
        _admin("POST", "/admin/users/", body, "application/json")
    print("ready")


def _stop(work: Path) -> None:
    home = work / "home"
    pid_file = home / "server.pid"
    if pid_file.exists():
        subprocess.call(["kill", pid_file.read_text()])
    subprocess.call(
        [_PYTHON, "-m", "provisa.core.control_plane_pg", "stop", str(home / "control-pg")],
        cwd=_REPO,
    )


def _measure(user: str, password: str, out: Path | None) -> None:
    conn = psycopg.connect(
        host="127.0.0.1",
        port=_PGWIRE_PORT,
        dbname="provisa",
        user=user,
        password=password,
        sslmode="disable",
        autocommit=True,
    )
    results = {}
    for name, statement in _STATEMENTS.items():
        timings, rows = [], None
        for run in range(_RUNS):
            table = f"t{2 * run:04d}" if name == "getColumns_each" else f"t{2 * run + 2:04d}"
            started = time.perf_counter()
            rows = len(conn.execute(statement.replace("{table}", table), prepare=False).fetchall())
            timings.append((time.perf_counter() - started) * 1000)
        results[name] = {
            "first_ms": round(timings[0], 1),
            "warm_ms": round(statistics.median(timings[1:]), 1),
            "rows": rows,
        }
    print(user, json.dumps(results))
    if out is not None:
        out.write_text(json.dumps(results, indent=1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("model", "start", "stop"):
        commands.add_parser(name).add_argument("--dir", required=True, type=Path)
    measure = commands.add_parser("measure")
    measure.add_argument("--user", required=True)
    measure.add_argument("--password", required=True)
    measure.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.command == "model":
        _model(args.dir)
    elif args.command == "start":
        _start(args.dir)
    elif args.command == "stop":
        _stop(args.dir)
    else:
        _measure(args.user, args.password, args.out)


if __name__ == "__main__":
    main()
