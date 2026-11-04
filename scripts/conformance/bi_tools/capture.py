# Copyright (c) 2026 Kenneth Stott
# Canary: 61d4b8e2-9a7f-4c05-8e31-b2f6a0d9c7e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Capture the statements BI tools and psql send to browse a schema (REQ-2204).

Each tool, at a pinned version, connects through the recording proxy (proxy.py) to a PostgreSQL 17
seeded with the pgwire conformance job's fixture (pgwire_catalog_types.py ``seed`` and
bi_tools/fixtures.py), signs in as a role of its own (so the recording tells the tools apart), and
browses schema ``conformance`` as its schema browser does:

* psql: the PostgreSQL 17.11 image's own psql runs \\dn, \\d, \\dt, \\dv, \\di, \\df (each also over
  ``conformance.*``) and \\d+ of each fixture table, in one session.
* Metabase: its official image, set up, the database added and synced, re-synced and its schema read
  back through its HTTP API (``/api/setup``, ``/api/database``, ``sync_schema``,
  ``/api/database/:id/metadata``, ``/api/table/:id/query_metadata``). Its sync is its browser.
* Grafana: its official image with the bundled PostgreSQL data source added through its HTTP API,
  checked (``/health``), and the SQL its query editor sends to list tables and a table's columns
  (postgresMetaQuery.ts of the bundled plugin: getVersion, getTimescaleDBVersion, showTables, then
  getSchema of each table showTables lists) run through ``/api/ds/query``, as the editor runs them.
* DBeaver: a desktop application with no headless schema browser, captured once by hand
  (``serve``): the PostgreSQL and the proxy stay up while a person connects DBeaver and browses, as
  docs/conformance/pgwire.md describes.

Each tool's statements are written to ``bi_tools/<tool>.yaml`` with the tool's version, in the order
first sent, deduplicated, each with the bind parameters it was sent with (type OID and value). OIDs
of the fixture's objects become ``{{oid:<catalog>:<name>}}`` tokens, and the database's and the
tool user's names ``{{database}}`` and ``{{user}}``, which the replay resolves on each server
(replay.py), since PostgreSQL and Provisa number and name them differently.

Usage (Docker, the job's .venv; through ~/.provisa-agent/heavy.sh on a shared machine):
  capture.py run [--tools psql metabase grafana]
  capture.py serve --tool dbeaver --version 25.1.4     # by hand: browse, then press Enter
"""

# Requirements: REQ-2204

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

#: The pinned images: PostgreSQL as the pgwire job runs it (its psql is the psql captured).
PG_IMAGE = "postgres:17.11"
METABASE_IMAGE = "metabase/metabase:v0.64.5"
GRAFANA_IMAGE = "grafana/grafana:13.3.0"

PG_PORT = 55491
PROXY_PORT = 55492
METABASE_PORT = 55493
GRAFANA_PORT = 55494
#: The database the tools browse: a name of its own, so a statement naming it is told from one
#: naming schema ``conformance``.
DATABASE = "bitools"
PASSWORD = "capture-only-not-a-secret"
#: Where a container reaches the proxy on the host.
HOST_FROM_CONTAINER = "host.docker.internal"

TOOLS = ("psql", "metabase", "grafana", "dbeaver")
SCHEMA = "conformance"
#: psql's browse: the brief's meta-commands, each over the search path and over the fixture schema,
#: and each fixture table described in full.
PSQL_COMMANDS = [
    "\\dn",
    "\\d",
    "\\dt",
    "\\dt conformance.*",
    "\\dv conformance.*",
    "\\di conformance.*",
    "\\df",
    "\\df conformance.*",
    "\\d+ conformance.types_fixture",
    "\\d+ conformance.bi_customer",
    "\\d+ conformance.bi_order",
]

#: The first OID PostgreSQL gives a user object: only these are the fixture's.
FIRST_USER_OID = 16384


def user_of(tool: str) -> str:
    """The role a tool signs in as."""
    return f"bi_{tool}"


# --- the SQL Grafana's PostgreSQL query editor sends (postgresMetaQuery.ts, bundled plugin) -----


def _grafana_schema_constraint() -> str:
    return """
          quote_ident(table_schema) IN (
          SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
          FROM
            generate_series(
              array_lower(string_to_array(current_setting('search_path'),','),1),
              array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
          )"""


def grafana_version() -> str:
    return "SELECT current_setting('server_version_num')::int/100 as version"


def grafana_timescaledb_version() -> str:
    return "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"


def grafana_show_tables() -> str:
    c = _grafana_schema_constraint()
    return f"""SELECT
    CASE WHEN {c}
      THEN quote_ident(table_name)
      ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
    END AS "table"
    FROM information_schema.tables
    WHERE quote_ident(table_schema) NOT IN ('information_schema',
                             'pg_catalog',
                             '_timescaledb_cache',
                             '_timescaledb_catalog',
                             '_timescaledb_internal',
                             '_timescaledb_config',
                             'timescaledb_information',
                             'timescaledb_experimental')
    ORDER BY CASE WHEN {c} THEN 0 ELSE 1 END, 1"""


def grafana_get_schema(table: str) -> str:
    part = "'" + table.replace("'", "''") + "'"
    return f"""SELECT quote_ident(column_name) AS "column", data_type AS "type"
    FROM information_schema.columns
    WHERE
      CASE WHEN array_length(parse_ident({part}),1) = 2
        THEN quote_ident(table_schema) = (parse_ident({part}))[1]
          AND quote_ident(table_name) = (parse_ident({part}))[2]
        ELSE quote_ident(table_name) = {part}
          AND {_grafana_schema_constraint()}
      END"""


# --- turning a recording into stored statements ----------------------------------------------


def statement_key(entry: dict[str, Any]) -> str:
    """Two recorded statements are one when their SQL and parameters are the same."""
    return json.dumps([entry["sql"], entry.get("params") or []], sort_keys=True)


def dedupe(entries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """The entries, each once, in the order first sent."""
    seen: set[str] = set()
    out = []
    for entry in entries:
        key = statement_key(entry)
        if key not in seen:
            seen.add(key)
            out.append(entry)
    return out


def tokenise(text: str, oids: dict[int, str], names: dict[str, str]) -> str:
    """``text`` with each fixture OID and each of ``names`` (a literal word: the database's name,
    the tool's user) replaced by its token. A number or word is replaced only standing alone."""
    for oid, token in oids.items():
        text = re.sub(rf"(?<![\w.$]){oid}(?![\w.])", token, text)
    for word, token in names.items():
        text = re.sub(rf"(?<![\w$]){re.escape(word)}(?![\w$])", token, text)
    return text


def stored(
    entries: Iterable[dict[str, Any]], oids: dict[int, str], names: dict[str, str]
) -> list[dict[str, Any]]:
    """The recorded entries of one tool as stored statements: tokenised, then deduplicated (the
    same statement for two connections' OIDs is one), each with an id."""
    out = []
    for entry in entries:
        statement: dict[str, Any] = {
            "protocol": entry["protocol"],
            "sql": tokenise(entry["sql"], oids, names),
        }
        params = entry.get("params") or []
        if params:
            statement["params"] = [
                {
                    **p,
                    "value": None if p["value"] is None else tokenise(p["value"], oids, names),
                }
                if p.get("format", 0) == 0
                else p
                for p in params
            ]
        out.append(statement)
    return [{"id": i + 1, **s} for i, s in enumerate(dedupe(out))]


#: The fixture's objects, each with the token naming it (a statement that names an object by OID).
OID_QUERY = """
SELECT c.oid::int8, 'pg_class:' || n.nspname || '.' || c.relname
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %(schema)s
UNION ALL
SELECT t.oid::int8, 'pg_type:' || n.nspname || '.' || t.typname
  FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = %(schema)s
UNION ALL
SELECT k.oid::int8, 'pg_constraint:' || n.nspname || '.' || k.conname
  FROM pg_constraint k JOIN pg_namespace n ON n.oid = k.connamespace WHERE n.nspname = %(schema)s
UNION ALL
SELECT oid::int8, 'pg_namespace:' || nspname FROM pg_namespace WHERE nspname = %(schema)s
UNION ALL
SELECT oid::int8, 'pg_database:' FROM pg_database WHERE datname = current_database()
UNION ALL
SELECT oid::int8, 'pg_authid:' FROM pg_roles WHERE rolname = %(user)s
"""


def fixture_oids(pg_dsn: str, user: str) -> dict[int, str]:
    import psycopg

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        rows = conn.execute(OID_QUERY, {"schema": SCHEMA, "user": user}).fetchall()
    return {oid: "{{oid:" + name + "}}" for oid, name in rows if oid >= FIRST_USER_OID}


def write_tool(
    tool: str,
    recording: Path,
    pg_dsn: str,
    version: str,
    method: str,
    browse: list[str],
    out_dir: Path = HERE,
) -> Path:
    """``bi_tools/<tool>.yaml``: the tool's recorded statements as ``stored`` gives them."""
    import yaml

    user = user_of(tool)
    entries = [
        e
        for e in (json.loads(line) for line in recording.read_text().splitlines() if line)
        if e["user"] == user
    ]
    statements = stored(
        entries, fixture_oids(pg_dsn, user), {DATABASE: "{{database}}", user: "{{user}}"}
    )
    doc = {
        "tool": tool,
        "version": version,
        "captured": dt.date.today().isoformat(),
        "method": method,
        "browse": browse,
        "statements": statements,
    }
    path = out_dir / f"{tool}.yaml"
    path.write_text(yaml.dump(doc, Dumper=block_dumper(), sort_keys=False, width=100))
    print(f"{path}: {len(statements)} statements ({len(entries)} sent)")
    return path


def block_dumper() -> type:
    """yaml's SafeDumper writing a multi-line string as a block, so a stored statement reads as
    its SQL."""
    import yaml

    class Dumper(yaml.SafeDumper):
        pass

    def text(dumper: yaml.SafeDumper, value: str) -> Any:
        style = "|" if "\n" in value else None
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)

    Dumper.add_representer(str, text)
    return Dumper


# --- the environment: PostgreSQL, the proxy and the tools' containers ------------------------


def _docker(*args: str, check: bool = True) -> str:
    done = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and done.returncode:
        raise SystemExit(f"docker {' '.join(args)}: {done.stderr.strip()}")
    return done.stdout.strip()


def _wait(what: str, ready, timeout: float = 600) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready():
            return
        time.sleep(2)
    raise SystemExit(f"{what} was not ready within {timeout:.0f}s")


def pg_dsn(user: str = "postgres", password: str = PASSWORD) -> str:
    return f"host=127.0.0.1 port={PG_PORT} dbname={DATABASE} user={user} password={password}"


def _pg_ready() -> bool:
    import psycopg

    try:
        psycopg.connect(pg_dsn(), connect_timeout=2).close()
        return True
    except psycopg.Error:
        return False


def start_postgres(name: str) -> None:
    """PostgreSQL 17 with the job's fixture in database ``bitools`` and a role per tool."""
    import psycopg

    import pgwire_catalog_types
    from bi_tools import fixtures

    _docker("rm", "-f", name, check=False)
    _docker(
        "run", "-d", "--name", name, "-p", f"{PG_PORT}:5432",
        "-e", f"POSTGRES_PASSWORD={PASSWORD}", "-e", f"POSTGRES_DB={DATABASE}", PG_IMAGE,
    )  # fmt: skip
    _wait("PostgreSQL", _pg_ready, 120)
    pgwire_catalog_types.seed(pg_dsn())
    with psycopg.connect(pg_dsn(), autocommit=True) as conn:
        for statement in fixtures.ddl():
            conn.execute(statement)
        for tool in TOOLS:
            user = user_of(tool)
            conn.execute(f"DROP ROLE IF EXISTS {user}")
            conn.execute(f"CREATE ROLE {user} LOGIN PASSWORD '{PASSWORD}'")
            conn.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {user}")
            conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA} TO {user}")


def start_proxy(recording: Path) -> None:
    """The recording proxy, in a thread of this process, from every interface to PostgreSQL."""
    import asyncio

    from bi_tools import proxy

    recording.write_text("")
    thread = threading.Thread(
        target=lambda: asyncio.run(
            proxy.serve(("0.0.0.0", PROXY_PORT), ("127.0.0.1", PG_PORT), recording)
        ),
        daemon=True,
    )
    thread.start()
    time.sleep(1)


def settle(recording: Path, user: str, quiet: float = 15, timeout: float = 900) -> None:
    """Wait until ``user`` has sent nothing for ``quiet`` seconds."""

    def sent() -> int:
        return sum(1 for line in recording.read_text().splitlines() if f'"user": "{user}"' in line)

    deadline = time.monotonic() + timeout
    last, since = sent(), time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(3)
        now = sent()
        if now != last:
            last, since = now, time.monotonic()
        elif time.monotonic() - since >= quiet:
            return
    raise SystemExit(f"{user} was still sending after {timeout:.0f}s")


def _http(method: str, url: str, body: Any = None, headers: dict[str, str] | None = None) -> Any:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    if data is not None:
        request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read()
    return json.loads(raw) if raw.strip() else None


def _answers(url: str) -> bool:
    try:
        _http("GET", url)
        return True
    except (urllib.error.URLError, ConnectionError, TimeoutError):
        return False


# --- the tools ---------------------------------------------------------------------------------


def capture_psql(recording: Path) -> tuple[str, str, list[str]]:
    version = _docker("run", "--rm", PG_IMAGE, "psql", "--version")
    args = [
        "run", "--rm", "-e", f"PGPASSWORD={PASSWORD}", PG_IMAGE, "psql", "-X",
        "-h", HOST_FROM_CONTAINER, "-p", str(PROXY_PORT), "-U", user_of("psql"), "-d", DATABASE,
    ]  # fmt: skip
    for command in PSQL_COMMANDS:
        args += ["-c", command]
    print(_docker(*args))
    settle(recording, user_of("psql"), quiet=3)
    method = f"Automated: {PG_IMAGE}'s psql (-X), one session, through the recording proxy."
    return re.search(r"\d+\.\d+", version).group(), method, PSQL_COMMANDS


def capture_metabase(recording: Path) -> tuple[str, str, list[str]]:
    name = "j591-bi-metabase"
    base = f"http://127.0.0.1:{METABASE_PORT}"
    _docker("rm", "-f", name, check=False)
    _docker("run", "-d", "--name", name, "-p", f"{METABASE_PORT}:3000", METABASE_IMAGE)
    try:
        _wait("Metabase", lambda: _answers(f"{base}/api/health"), 600)
        token = _http("GET", f"{base}/api/session/properties")["setup-token"]
        session = _http(
            "POST",
            f"{base}/api/setup",
            {
                "token": token,
                "user": {
                    "first_name": "Conformance",
                    "last_name": "Capture",
                    "email": "capture@example.com",
                    "password": "Capture-only-2026!",
                    "password_confirm": "Capture-only-2026!",
                },
                "prefs": {"site_name": "capture", "allow_tracking": False},
            },
        )["id"]
        auth = {"X-Metabase-Session": session}
        database = _http(
            "POST",
            f"{base}/api/database",
            {
                "engine": "postgres",
                "name": "conformance",
                "details": {
                    "host": HOST_FROM_CONTAINER,
                    "port": PROXY_PORT,
                    "dbname": DATABASE,
                    "user": user_of("metabase"),
                    "password": PASSWORD,
                    "ssl": False,
                },
                "is_full_sync": True,
            },
            auth,
        )
        db = database["id"]
        _wait(
            "Metabase's first sync",
            lambda: (
                _http("GET", f"{base}/api/database/{db}", headers=auth).get("initial_sync_status")
                == "complete"
            ),
            900,
        )
        settle(recording, user_of("metabase"))
        _http("POST", f"{base}/api/database/{db}/sync_schema", {}, auth)
        settle(recording, user_of("metabase"))
        metadata = _http("GET", f"{base}/api/database/{db}/metadata", headers=auth)
        for table in metadata["tables"]:
            _http("GET", f"{base}/api/table/{table['id']}/query_metadata", headers=auth)
        settle(recording, user_of("metabase"))
        version = _http("GET", f"{base}/api/session/properties", headers=auth)["version"]["tag"]
    finally:
        _docker("rm", "-f", name, check=False)
    method = (
        f"Automated: {METABASE_IMAGE} through its HTTP API: setup, add the database (first "
        "sync, analysis), sync_schema, then the database's and each table's metadata."
    )
    browse = [
        "POST /api/database (first sync and analysis)",
        "POST /api/database/:id/sync_schema",
        "GET /api/database/:id/metadata",
        "GET /api/table/:id/query_metadata for each table",
    ]
    return version, method, browse


def capture_grafana(recording: Path) -> tuple[str, str, list[str]]:
    name = "j591-bi-grafana"
    base = f"http://127.0.0.1:{GRAFANA_PORT}"
    basic = {"Authorization": "Basic " + base64.b64encode(f"admin:{PASSWORD}".encode()).decode()}
    _docker("rm", "-f", name, check=False)
    _docker(
        "run", "-d", "--name", name, "-p", f"{GRAFANA_PORT}:3000",
        "-e", f"GF_SECURITY_ADMIN_PASSWORD={PASSWORD}", GRAFANA_IMAGE,
    )  # fmt: skip
    try:
        _wait("Grafana", lambda: _answers(f"{base}/api/health"), 300)
        source = _http(
            "POST",
            f"{base}/api/datasources",
            {
                "name": "conformance",
                "type": "grafana-postgresql-datasource",
                "access": "proxy",
                "url": f"{HOST_FROM_CONTAINER}:{PROXY_PORT}",
                "user": user_of("grafana"),
                "jsonData": {
                    "database": DATABASE,
                    "sslmode": "disable",
                    "postgresVersion": 1700,
                    "timescaledb": False,
                },
                "secureJsonData": {"password": PASSWORD},
            },
            basic,
        )["datasource"]
        uid = source["uid"]
        _http("GET", f"{base}/api/datasources/uid/{uid}/health", headers=basic)

        def query(sql: str) -> list[list[Any]]:
            answer = _http(
                "POST",
                f"{base}/api/ds/query",
                {
                    "queries": [
                        {
                            "refId": "A",
                            "datasource": {"uid": uid, "type": "grafana-postgresql-datasource"},
                            "rawSql": sql,
                            "format": "table",
                            "rawQuery": True,
                            "editorMode": "code",
                        }
                    ],
                    "from": "now-1h",
                    "to": "now",
                },
                basic,
            )
            frames = answer["results"]["A"].get("frames") or []
            return frames[0]["data"]["values"] if frames else []

        query(grafana_version())
        query(grafana_timescaledb_version())
        listed = query(grafana_show_tables())
        tables = [t for t in (listed[0] if listed else []) if t.startswith(f"{SCHEMA}.")]
        for table in tables:
            query(grafana_get_schema(table))
        settle(recording, user_of("grafana"), quiet=5)
        version = _http("GET", f"{base}/api/health")["version"]
        plugin = _http(
            "GET", f"{base}/api/plugins/grafana-postgresql-datasource/settings", headers=basic
        )["info"]["version"]
    finally:
        _docker("rm", "-f", name, check=False)
    method = (
        f"Automated: {GRAFANA_IMAGE} (bundled grafana-postgresql-datasource {plugin}) through "
        "its HTTP API: the data source added and checked, then the SQL its query editor sends "
        "(postgresMetaQuery.ts) run through /api/ds/query."
    )
    browse = [
        "POST /api/datasources",
        "GET /api/datasources/uid/:uid/health",
        "getVersion, getTimescaleDBVersion, showTables",
        f"getSchema of each table: {', '.join(tables)}",
    ]
    return f"{version} (plugin {plugin})", method, browse


CAPTURES = {"psql": capture_psql, "metabase": capture_metabase, "grafana": capture_grafana}

DBEAVER_STEPS = [
    "New connection: PostgreSQL, host 127.0.0.1, port 55492, database bitools, user bi_dbeaver",
    "Connect; expand Databases > bitools > Schemas > conformance",
    "Expand Tables, then each table's Columns, Constraints, Foreign Keys and Indexes",
    "Expand Views, Materialized Views, Functions, Sequences and Data types",
    "Open each table's editor (Properties, DDL tabs)",
    "Open Database > Session Manager (Tools > Session Manager); refresh it once",
    "Open the connection's Dashboard and leave it open for 30 seconds",
    "Disconnect",
]


def run(tools: list[str], recording: Path) -> None:
    name = "j591-bi-pg"
    start_postgres(name)
    try:
        start_proxy(recording)
        for tool in tools:
            version, method, browse = CAPTURES[tool](recording)
            write_tool(tool, recording, pg_dsn(), version, method, browse)
    finally:
        _docker("rm", "-f", name, check=False)


def serve(tool: str, version: str, recording: Path) -> None:
    """PostgreSQL and the proxy up while a person browses with ``tool``; Enter writes it."""
    name = "j591-bi-pg"
    start_postgres(name)
    try:
        start_proxy(recording)
        print(f"Connect {tool} to 127.0.0.1:{PROXY_PORT}, database {DATABASE}, "
              f"user {user_of(tool)}, password {PASSWORD}, SSL off. Then:")  # fmt: skip
        for step in DBEAVER_STEPS:
            print(f"  - {step}")
        input("Press Enter when the browse is done: ")
        method = (
            f"By hand: {tool} {version} on macOS, through the recording proxy "
            "(capture.py serve), following the steps in browse."
        )
        write_tool(tool, recording, pg_dsn(), version, method, DBEAVER_STEPS)
    finally:
        _docker("rm", "-f", name, check=False)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--tools", nargs="+", default=list(CAPTURES), choices=list(CAPTURES))
    r.add_argument("--recording", type=Path, default=Path.cwd() / "bi-capture.jsonl")
    s = sub.add_parser("serve")
    s.add_argument("--tool", required=True, choices=TOOLS)
    s.add_argument("--version", required=True)
    s.add_argument("--recording", type=Path, default=Path.cwd() / "bi-capture.jsonl")
    args = parser.parse_args(argv)
    if args.command == "run":
        run(args.tools, args.recording)
    else:
        serve(args.tool, args.version, args.recording)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
