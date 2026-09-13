# Copyright (c) 2026 Kenneth Stott
# Canary: 92fdcacd-18a1-46ad-ab60-7e59d2add841
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Firebird demo source: create widgets(id, name) + 3 rows via `isql` inside
the container (there is no pure-Python Firebird wire client in this project's dependencies — the
DuckDB firebird extension itself needs the native libfbclient, which is an app-server-side concern,
not a seeding concern; docker exec avoids adding that dependency here too).

``PROVISA_DEMO_PREFIX`` names the compose project prefix provision.py started this source under
(``up --prefix <P>`` -> project ``<P>-firebird``) — required so this script finds ITS OWN container
by label rather than any other Firebird that happens to be running (e.g. a maintainer's local-dev
stack), matching this project's three-instance-isolation rule. Defaults to provision.py's own
default prefix, ``provisa-demo``.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

_PREFIX = os.environ.get("PROVISA_DEMO_PREFIX", "provisa-demo")
_PROJECT = f"{_PREFIX}-firebird"
_USER = "provisa"
_PASSWORD = "provisa"
_DB_PATH_IN_CONTAINER = "/firebird/data/test.fdb"  # FIREBIRD_DATABASE=test.fdb (compose env)
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def _container_id() -> str:
    out = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={_PROJECT}",
            "--filter",
            "label=com.docker.compose.service=firebird",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"no running firebird container for compose project {_PROJECT!r}")
    return ids[0]


def _isql(container_id: str, script: str) -> None:
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container_id,
            "/usr/local/firebird/bin/isql",
            "-user",
            _USER,
            "-password",
            _PASSWORD,
            _DB_PATH_IN_CONTAINER,
        ],  # fmt: skip
        input=script,
        capture_output=True,
        text=True,
        check=True,
    )


def main() -> int:
    container_id = _container_id()
    # Quoted lower-case identifiers: unquoted Firebird DDL folds to UPPER CASE (verified live —
    # information_schema reports table "WIDGETS", columns "ID"/"NAME"), which the generic
    # registerTable mutation would persist verbatim (no apply_sql_name normalization on that path,
    # unlike graphql_remote_router's registration). Quoting avoids that mismatch outright rather
    # than depending on the engine's own identifier case-folding at query time.
    ddl = (
        'CREATE TABLE "widgets" ("id" INTEGER, "name" VARCHAR(64));\n'
        "COMMIT;\n"
        + "\n".join(f"INSERT INTO \"widgets\" VALUES ({wid}, '{name}');" for wid, name in _WIDGETS)
        + "\nCOMMIT;\n"
    )
    deadline = time.monotonic() + 60
    last_err: subprocess.CalledProcessError | None = None
    while time.monotonic() < deadline:
        try:
            _isql(container_id, ddl)
            print(f"firebird demo source primed: {len(_WIDGETS)} widgets in {_PROJECT}")
            return 0
        except subprocess.CalledProcessError as e:
            last_err = e
            time.sleep(3)
    detail = last_err.stderr if last_err is not None else "<no seed attempt>"
    print(f"firebird widgets seed never succeeded: {last_err!r}: {detail!r}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
