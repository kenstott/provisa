# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1e1a1c-3b3a-4a52-9f4b-1d1b0c9d9d7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Exasol demo source: create PROVISA.WIDGETS(id, name) + 3 rows via `exaplus`
inside the container (no pyexasol dependency — see tests/integration/test_exasol_source_e2e.py's
module docstring for why: seeding a fixture is not a reason to add a runtime dependency).

Exasol 8 always serves TLS with a self-signed, per-container-boot certificate, so there is no CA
to import; the driver's documented alternative is pinning the SHA-256 fingerprint inline as
``<host>/<FINGERPRINT>:<port>``, which exaplus itself reports on a plain-connection PKIX failure —
this mirrors tests/integration/test_exasol_source_e2e.py's tls_fingerprint()/_exaplus() helpers
exactly (that test is this fixture's proof it works).

``PROVISA_DEMO_PREFIX`` names the compose project prefix provision.py started this source under
(``up --prefix <P>`` -> project ``<P>-exasol``) — see demo/sources/firebird/prime.py's identical
convention/rationale (three-instance isolation: find THIS run's own container by label, never any
other Exasol that happens to be running).

The fingerprint is generated fresh on every container boot, so it cannot be a fixed constant the
e2e test types into the form — it is written to ``PROVISA_DEMO_EXASOL_FINGERPRINT_FILE`` (default
a path under the OS temp dir; ephemeral, read once by the caller right after this script exits,
never meant to survive past the run) so the caller can read it back and fill the Source form's own
TLS Fingerprint field with the SAME value this run's container actually presents.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

_PREFIX = os.environ.get("PROVISA_DEMO_PREFIX", "provisa-demo")
_FINGERPRINT_FILE = Path(
    os.environ.get(
        "PROVISA_DEMO_EXASOL_FINGERPRINT_FILE",
        str(Path(tempfile.gettempdir()) / "provisa-demo-exasol-fingerprint.txt"),
    )
)
_PROJECT = f"{_PREFIX}-exasol"
_USER = "sys"
_PASSWORD = "exasol"  # image default (exasol/docker-db)
_SCHEMA = "PROVISA"
_TABLE = "WIDGETS"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]
_FINGERPRINT_RE = re.compile(r"localhost/([0-9A-F]{64}):8563")


def _container_id() -> str:
    out = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={_PROJECT}",
            "--filter",
            "label=com.docker.compose.service=exasol",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"no running exasol container for compose project {_PROJECT!r}")
    return ids[0]


def _tls_fingerprint(container_id: str) -> str | None:
    proc = subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            "exaplus",
            "-c",
            "localhost:8563",
            "-u",
            _USER,
            "-p",
            _PASSWORD,
            "-sql",
            "SELECT 1;",
            "-q",
        ],  # fmt: skip
        capture_output=True,
        text=True,
    )
    match = _FINGERPRINT_RE.search(proc.stdout + proc.stderr)
    return match.group(1) if match else None


def _exaplus(container_id: str, fingerprint: str, statement: str) -> None:
    terminated = statement if statement.rstrip().endswith(";") else f"{statement};"
    proc = subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            "exaplus",
            "-c",
            f"localhost/{fingerprint}:8563",
            "-u",
            _USER,
            "-p",
            _PASSWORD,
            "-x",  # a failed statement becomes a failed process, not a 0-exit with an error on stdout
            "-sql",
            terminated,
        ],  # fmt: skip
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"exaplus failed (rc={proc.returncode}) for {statement!r}:\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )


def main() -> int:
    container_id = _container_id()
    deadline = time.monotonic() + 300  # EXAStorage cold init genuinely takes minutes, not seconds

    fingerprint = None
    last_err: str | None = None
    while time.monotonic() < deadline and fingerprint is None:
        fingerprint = _tls_fingerprint(container_id)
        if fingerprint is None:
            time.sleep(3)
    if fingerprint is None:
        print("exasol never offered a certificate fingerprint", file=sys.stderr)
        return 1
    _FINGERPRINT_FILE.write_text(fingerprint)

    ddl = [
        f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE",
        f"CREATE SCHEMA {_SCHEMA}",
        f"CREATE TABLE {_SCHEMA}.{_TABLE} (id DECIMAL(10,0), name VARCHAR(64))",
        *(f"INSERT INTO {_SCHEMA}.{_TABLE} VALUES ({wid}, '{name}')" for wid, name in _WIDGETS),
    ]
    while time.monotonic() < deadline:
        try:
            for stmt in ddl:
                _exaplus(container_id, fingerprint, stmt)
            print(
                f"exasol demo source primed: {len(_WIDGETS)} widgets in {_SCHEMA}.{_TABLE} "
                f"({_PROJECT}, fingerprint {fingerprint})"
            )
            return 0
        except RuntimeError as e:
            last_err = str(e)
            time.sleep(3)
    print(f"exasol widgets seed never succeeded: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
