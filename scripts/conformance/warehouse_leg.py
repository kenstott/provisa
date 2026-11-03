# Copyright (c) 2026 Kenneth Stott
# Canary: b9beec06-53be-4079-ae26-026c5997ceae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A warehouse leg of Conformance (functions) (REQ-2167): the live warehouse the integration suite's
warehouse lane holds credentials for (.github/workflows/integration-suite-lanes.yml, warehouse job),
brought up for the leg and left stopped after it with that lane's own helpers (tests/integration).

``up`` fails by name when a secret the leg reads (function_legs.yaml ``secrets``) is not set, as the
lane's "Credentials present" step does; wakes or provisions what the leg uses; and writes to
$GITHUB_ENV the secrets the Provisa instance reads and the engine URL. ``down`` leaves what ``up``
reached stopped, suspended, paused or deleted (REQ-254), whatever happened between, and does nothing
when ``up`` never got past its check.
"""

# Requirements: REQ-2167

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import function_legs  # noqa: E402

#: The lane's Snowflake setting (integration-suite-lanes.yml): suspended after 60 idle seconds, so a
#: leg that dies before ``down`` cannot leave it billing.
SNOWFLAKE_AUTO_SUSPEND_S = 60
#: The MotherDuck engine e2e's scratch names (tests/integration/test_motherduck_engine_store_e2e.py),
#: whose sweep drops one a day old: a leg that dies before ``down`` leaves nothing for good.
MOTHERDUCK_STAMP = "%Y%m%d%H%M%S"


def secrets_of(engine: str, doc: dict | None = None) -> list[str]:
    leg = next(leg for leg in (doc or function_legs.load())["legs"] if leg["engine"] == engine)
    return list(leg.get("secrets", ()))


def missing(names: list[str], env: dict[str, str]) -> list[str]:
    return [name for name in names if not env.get(name)]


def export(env_file: Path, values: dict[str, str]) -> None:
    """Append ``values`` to $GITHUB_ENV, each in the delimited form a value of any text survives."""
    with env_file.open("a") as out:
        for name, value in values.items():
            delimiter = f"EOF_{uuid.uuid4().hex}"
            out.write(f"{name}<<{delimiter}\n{value}\n{delimiter}\n")


def mask(value: str) -> None:
    """A value built from a secret (an encoded password, a URL) is not masked by the secret's own."""
    if value:
        print(f"::add-mask::{value}", flush=True)


# --- up: each returns the engine URL ($PROVISA_ENGINE_URL; empty where the engine reads its
# server from the secrets themselves) and the variables the instance needs beyond the secrets.


def _snowflake_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    from tests.integration.snowflake_warehouse import set_auto_suspend

    wh = env["SNOWFLAKE_WAREHOUSE"]
    print("the Snowflake warehouse suspends after", set_auto_suspend(wh, SNOWFLAKE_AUTO_SUSPEND_S))
    user, pw = quote(env["SNOWFLAKE_USER"], safe=""), quote(env["SNOWFLAKE_PASSWORD"], safe="")
    db = env["SNOWFLAKE_DATABASE"]
    return f"snowflake://{user}:{pw}@{env['SNOWFLAKE_ACCOUNT']}/{db}/public?warehouse={wh}", {}


def _databricks_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    from tests.integration.databricks_warehouse import ensure_warehouse_running

    ensure_warehouse_running()
    url = (
        f"databricks://token:{env['DATABRICKS_TOKEN']}@{env['DATABRICKS_SERVER_HOSTNAME']}"
        f"?http_path={env['DATABRICKS_HTTP_PATH']}"
    )
    return url, {}


def _bigquery_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    # The lane's own handling: the service-account key is a file the client is pointed at.
    key = state / "gcp-key.json"
    key.write_text(env["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    return f"bigquery://{env['GOOGLE_CLOUD_PROJECT']}", {"GOOGLE_APPLICATION_CREDENTIALS": str(key)}


def _fabric_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    from tests.integration.fabric_capacity import ensure_capacity_resumed

    ensure_capacity_resumed()
    # The engine connects from FABRIC_SQL_SERVER / FABRIC_DATABASE, never a URL (engine.py).
    return "", {}


def _az_login(env: dict[str, str]) -> None:
    subprocess.run(
        [
            "az", "login", "--service-principal", "-u", env["AZURE_CLIENT_ID"],
            "-p", env["AZURE_CLIENT_SECRET"], "--tenant", env["AZURE_TENANT_ID"], "--output", "none",
        ],
        check=True,
    )  # fmt: skip


def _synapse_workspace(env: dict[str, str]) -> str:
    return env["SYNAPSE_SQL_SERVER"].removesuffix("-ondemand.sql.azuresynapse.net")


def _synapse_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    # The pinned path of tests/integration/synapse_provision.py synapse_lane: the standing workspace
    # admits only this runner's address, by a rule made here and removed by ``down``.
    from tests.integration.synapse_provision import _open_firewall, _wait_admitted

    _az_login(env)
    rule = "provisa-functions-" + datetime.now(UTC).strftime("%y%m%d%H%M%S")
    (state / "synapse-rule").write_text(rule)
    _open_firewall(_synapse_workspace(env), env["FABRIC_RESOURCE_GROUP"], rule)
    _wait_admitted(env["SYNAPSE_SQL_SERVER"], env["SYNAPSE_DATABASE"])
    return "", {}


def _redshift_state(state: Path) -> dict[str, str]:
    return {**os.environ, "REDSHIFT_E2E_STATE_FILE": str(state / "redshift.json")}


def _redshift_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    # No standing cluster: the warehouse lane provisions one per session (redshift_cluster.py), and
    # scripts/redshift_e2e.py is its bridge for a caller whose up and down are separate processes.
    out = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "redshift_e2e.py"), "up"],
        check=True,
        capture_output=True,
        text=True,
        env=_redshift_state(state),
    )
    sys.stderr.write(out.stderr)
    cluster = json.loads(out.stdout.strip().splitlines()[-1])
    mask(cluster["password"])
    pw = quote(cluster["password"], safe="")
    url = (
        f"redshift+psycopg2://{cluster['user']}:{pw}@{cluster['host']}:{cluster['port']}"
        f"/{cluster['database']}"
    )
    return url, {}


def _motherduck_account(token: str):
    import duckdb

    from provisa.federation.duckdb_extensions import install_and_load

    con = duckdb.connect()
    install_and_load(con, "motherduck", from_community=False)
    con.execute("SET GLOBAL motherduck_token = ?", [token])
    con.execute("ATTACH 'md:'")
    return con


def _motherduck_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    token = env["MOTHERDUCK_API_TOKEN"]
    name = f"provisa_it_{datetime.now(UTC).strftime(MOTHERDUCK_STAMP)}_{uuid.uuid4().hex[:8]}"
    con = _motherduck_account(token)
    try:
        con.execute(f'CREATE DATABASE "{name}"')
    finally:
        con.close()
    (state / "motherduck-database").write_text(name)
    return f"motherduck:///{name}?motherduck_token={token}", {}


def _singlestore_up(env: dict[str, str], state: Path) -> tuple[str, dict[str, str]]:
    # The hosted instance the SingleStore source and bulk-load e2e read, by the URL they build.
    pw = quote(env["SINGLESTORE_PASSWORD"], safe="")
    url = (
        f"singlestoredb://{env['SINGLESTORE_USERNAME']}:{pw}@{env['SINGLESTORE_HOST']}"
        f":{env['SINGLESTORE_PORT']}/{env['SINGLESTORE_DATABASE']}"
    )
    return url, {}


# --- down: what each leaves as the lane does --------------------------------------------------


def _snowflake_down(env: dict[str, str], state: Path) -> None:
    from tests.integration.snowflake_warehouse import suspend_warehouse

    print("the Snowflake warehouse is", suspend_warehouse(env["SNOWFLAKE_WAREHOUSE"]))


def _databricks_down(env: dict[str, str], state: Path) -> None:
    from tests.integration.databricks_warehouse import stop_warehouse, warehouse_state

    stop_warehouse()
    print("the Databricks warehouse is", warehouse_state())


def _fabric_down(env: dict[str, str], state: Path) -> None:
    from tests.integration.fabric_capacity import ensure_capacity_paused

    print("the Fabric capacity is", ensure_capacity_paused())


def _synapse_down(env: dict[str, str], state: Path) -> None:
    from tests.integration.synapse_provision import _close_firewall

    rule = state / "synapse-rule"
    if rule.is_file():
        _close_firewall(_synapse_workspace(env), env["FABRIC_RESOURCE_GROUP"], rule.read_text())
        print("the Synapse firewall rule", rule.read_text(), "is removed")


def _redshift_down(env: dict[str, str], state: Path) -> None:
    subprocess.run(
        [sys.executable, str(REPO / "scripts" / "redshift_e2e.py"), "down"],
        check=True,
        env=_redshift_state(state),
    )


def _motherduck_down(env: dict[str, str], state: Path) -> None:
    made = state / "motherduck-database"
    if made.is_file():
        con = _motherduck_account(env["MOTHERDUCK_API_TOKEN"])
        try:
            con.execute(f'DROP DATABASE IF EXISTS "{made.read_text()}"')
        finally:
            con.close()
        print("the MotherDuck database", made.read_text(), "is dropped")


def _nothing(env: dict[str, str], state: Path) -> None:
    print("nothing to leave stopped: the leg woke and made nothing")


UP: dict[str, Callable[[dict[str, str], Path], tuple[str, dict[str, str]]]] = {
    "snowflake": _snowflake_up,
    "databricks": _databricks_up,
    "bigquery": _bigquery_up,
    "fabric": _fabric_up,
    "synapse": _synapse_up,
    "redshift": _redshift_up,
    "motherduck": _motherduck_up,
    "singlestore": _singlestore_up,
}
DOWN: dict[str, Callable[[dict[str, str], Path], None]] = {
    "snowflake": _snowflake_down,
    "databricks": _databricks_down,
    "bigquery": _nothing,
    "fabric": _fabric_down,
    "synapse": _synapse_down,
    "redshift": _redshift_down,
    "motherduck": _motherduck_down,
    "singlestore": _nothing,
}
#: Secrets the instance is not given: the key's text, which ``up`` writes to a file instead.
NOT_EXPORTED = frozenset({"GOOGLE_APPLICATION_CREDENTIALS_JSON"})


def up(engine: str, env: dict[str, str], env_file: Path, state: Path) -> None:
    names = secrets_of(engine)
    absent = missing(names, env)
    if absent:
        raise SystemExit(
            f"::error::the {engine} leg cannot run; repo secrets not set: {' '.join(absent)}"
        )
    state.mkdir(parents=True, exist_ok=True)
    # From here on there may be something to leave stopped: ``down`` reads the secrets back from
    # $GITHUB_ENV, so they are exported before anything is woken.
    export(env_file, {name: env[name] for name in names if name not in NOT_EXPORTED})
    (state / "started").write_text(engine)
    url, extra = UP[engine](env, state)
    mask(url)
    export(env_file, {**extra, "PROVISA_ENGINE_URL": url})
    print(f"the {engine} leg's warehouse is up")


def down(engine: str, env: dict[str, str], state: Path) -> None:
    if not (state / "started").is_file():
        print(f"the {engine} leg brought nothing up")
        return
    DOWN[engine](env, state)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="A warehouse leg of Conformance (functions).")
    parser.add_argument("command", choices=("up", "down"))
    parser.add_argument("--engine", required=True, choices=sorted(UP))
    parser.add_argument("--state", required=True, type=Path, help="the leg's own directory")
    parser.add_argument("--env-file", type=Path, help="$GITHUB_ENV (up)")
    args = parser.parse_args(argv[1:])
    env = dict(os.environ)
    if args.command == "up":
        if args.env_file is None:
            parser.error("up writes $GITHUB_ENV: --env-file")
        up(args.engine, env, args.env_file, args.state)
    else:
        down(args.engine, env, args.state)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
