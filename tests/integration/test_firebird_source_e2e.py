# Copyright (c) 2026 Kenneth Stott
# Canary: 0c049f96-54b7-4be6-8d1e-d5ae20a38a2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Firebird read through DuckDB's `firebird` COMMUNITY EXTENSION (REQ-899, REQ-1097).

Firebird is neither a direct driver (provisa/executor/drivers/registry.py has no factory for it)
nor a Trino connector — the ONLY way Provisa reaches it is DuckDBFirebirdConnector
(provisa/federation/connector_duckdb.py:244), one of the seven REQ-899 DuckDB community-extension
connectors wired into the DuckDB partial-federator engine (provisa/federation/engine.py:453/477
build_duckdb_engine). Unlike the Trino-catalog bucket (test_cassandra_source_e2e.py) there is no
coordinator process to talk to: the engine IS an in-process DuckDB connection
(provisa.federation.duckdb_backend.DuckDBBackend / DuckDBFederationRuntime), and reaching Firebird
means that connection LOADs the `firebird` DuckDB extension and issues
``ATTACH '<dsn>' AS "<id>" (TYPE firebird)`` — exactly the DDL DuckDBFirebirdConnector.details()
builds. This test drives that real seam directly against an in-process DuckDB connection, the same
one DuckDBBackend wraps.

The one genuine extra dependency, and why it's handled here instead of skipped
------------------------------------------------------------------------------
DuckDB's `firebird` community extension is a thin wrapper around Firebird's OWN native client
library (libfbclient) — same shape as postgres_scanner needing libpq, sqlite needing nothing (it's
embedded), mssql needing FreeTDS-equivalent, etc. `INSTALL firebird FROM community; LOAD firebird`
succeeds standalone (verified below), but the extension's ATTACH only becomes FUNCTIONAL once it
can dlopen a real Firebird client library — it does not bundle one. No package manager on this
host ships it (checked: no Homebrew formula, no conda-forge package, no wheel bundling it).
Firebird's own project DOES publish a signed macOS installer per release
(https://github.com/FirebirdSQL/firebird/releases — a universal .pkg carrying `libfbclient.dylib`
+ its handful of sibling libs), so `_ensure_firebird_client_lib()` below self-provisions from that
official artifact instead of skipping: it downloads the .pkg once, expands it with `pkgutil`
(no system install — no `sudo`, no `/Library` writes), and caches the extracted libs under
``~/.cache/provisa-fdw/firebird-client-<version>-<arch>/``. One post-processing step is required:
the shipped `libfbclient.dylib` links a sibling (`libtommath.dylib`) via a bare `@rpath` entry with
NO accompanying `LC_RPATH` load command in the binary (upstream packaging quirk — every other
dependency in the same dylib resolves fine) — dyld refuses to resolve it, and `DYLD_LIBRARY_PATH`
is read only at process *launch*, too late to set from inside a running pytest process. The cache
step rewrites that one dependency to ``@loader_path/libtommath.dylib`` with
``install_name_tool -change`` (rewriting an EXISTING load command in place — unlike
``-add_rpath``, which needs headroom this binary's Mach-O header doesn't have) so the library is
fully self-contained: no environment variable needs to reach a live dyld process, only
``DUCKDB_FIREBIRD_CLIENT_LIBRARY`` pointing the extension at the patched file. Verified end-to-end
against a live `jacobalberty/firebird:v4` container before writing this test.

Non-macOS hosts: the client lib is looked for at common Linux install paths (a CI image that
already carries `libfbclient.so.2`, e.g. via `apt-get install firebird3.0-utils`); if genuinely
absent there, the test SKIPS with that specific reason — not a dodge, a real platform gap this
harness doesn't (yet) self-provision for, distinct from "the extension doesn't work."
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration]

_ITEST_PROJECT = os.environ.get("PROVISA_ITEST_PROJECT", "provisa-itest")

_FB_HOST = os.environ.get("FIREBIRD_HOST", "localhost")
_FB_PORT = int(os.environ.get("FIREBIRD_PORT", "3050"))
_FB_USER = "provisa"
_FB_PASSWORD = "provisa"
_FB_DB_PATH_IN_CONTAINER = "/firebird/data/test.fdb"  # FIREBIRD_DATABASE=test.fdb (compose env)

_TABLE = "widgets"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]

_FB_RELEASE_TAG = "v5.0.4"
_FB_RELEASE_BASE = f"https://github.com/FirebirdSQL/firebird/releases/download/{_FB_RELEASE_TAG}"
_ARCH_MAP = {"arm64": "arm64", "x86_64": "x64"}  # Firebird's macOS pkg naming vs. platform.machine()

_LINUX_CLIENT_CANDIDATES = [
    "/usr/lib/x86_64-linux-gnu/libfbclient.so.2",
    "/usr/lib/x86_64-linux-gnu/libfbclient.so",
    "/usr/lib/libfbclient.so.2",
    "/usr/lib/libfbclient.so",
    "/usr/local/firebird/lib/libfbclient.so",
]


def _macos_cache_dir() -> Path:
    arch = _ARCH_MAP.get(platform.machine())
    if arch is None:
        raise RuntimeError(f"no known Firebird macOS package for arch {platform.machine()!r}")
    return Path.home() / ".cache" / "provisa-fdw" / f"firebird-client-{_FB_RELEASE_TAG}-{arch}"


def _fetch_and_extract_macos_client(cache_dir: Path) -> None:
    """Download the official Firebird macOS .pkg and expand it (no system install) into cache_dir,
    then patch libfbclient.dylib's one bare-@rpath dependency to @loader_path (see module docstring).
    """
    import tempfile

    arch = _ARCH_MAP[platform.machine()]
    url = f"{_FB_RELEASE_BASE}/Firebird-5.0.4.1812-0-macos-{arch}.pkg"
    with tempfile.TemporaryDirectory(prefix="provisa_fb_pkg_") as tmp:
        tmp_path = Path(tmp)
        pkg = tmp_path / "firebird.pkg"
        subprocess.run(["curl", "-sL", "-o", str(pkg), url], check=True)
        expanded = tmp_path / "expanded"
        subprocess.run(["pkgutil", "--expand", str(pkg), str(expanded)], check=True)
        payload_dir = next(expanded.glob("*.pkg"))
        extracted = tmp_path / "payload"
        extracted.mkdir()
        with (payload_dir / "Payload").open("rb") as payload_fh:
            subprocess.run(
                "(gzip -dc 2>/dev/null || cat) | cpio -id",
                shell=True,
                cwd=extracted,
                check=True,
                stdin=payload_fh,
            )
        # The framework's Resources/lib carries libfbclient.dylib + every sibling it needs
        # (libtommath, libicu*, ...) flat in one directory — copy that directory as-is.
        src_lib = next(extracted.glob("**/Resources/lib"))
        cache_dir.mkdir(parents=True, exist_ok=True)
        for f in src_lib.iterdir():
            shutil.copy(f, cache_dir / f.name)

    client = cache_dir / "libfbclient.dylib"
    subprocess.run(
        [
            "install_name_tool",
            "-change",
            "@rpath/lib/libtommath.dylib",
            "@loader_path/libtommath.dylib",
            str(client),
        ],
        check=True,
    )


def _ensure_firebird_client_lib() -> str:
    """Return a path to a working Firebird client lib for DUCKDB_FIREBIRD_CLIENT_LIBRARY, self-
    provisioning it on macOS (see module docstring). Skips (not a dodge — a real platform gap) if
    no client is available and none can be self-provisioned."""
    if sys.platform == "darwin":
        cache_dir = _macos_cache_dir()
        client = cache_dir / "libfbclient.dylib"
        if not client.exists():
            _fetch_and_extract_macos_client(cache_dir)
        return str(client)
    for candidate in _LINUX_CLIENT_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    pytest.skip(
        "no Firebird client library found and no self-provisioning route on "
        f"{sys.platform}/{platform.machine()} (macOS self-provisions from the official .pkg; "
        "Linux needs libfbclient pre-installed, e.g. `apt-get install firebird3.0-utils`)"
    )


def _firebird_container_id() -> str:
    out = subprocess.run(
        [
            "docker", "ps", "-q",
            "--filter", f"label=com.docker.compose.project={_ITEST_PROJECT}",
            "--filter", "label=com.docker.compose.service=firebird",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"No running firebird container for project {_ITEST_PROJECT!r}")
    return ids[0]


def _isql(container_id: str, script: str) -> None:
    subprocess.run(
        [
            "docker", "exec", "-i", container_id,
            "/usr/local/firebird/bin/isql",
            "-user", _FB_USER, "-password", _FB_PASSWORD,
            _FB_DB_PATH_IN_CONTAINER,
        ],  # fmt: skip
        input=script,
        capture_output=True,
        text=True,
        check=True,
    )


def _seed_firebird() -> None:
    """Create widgets(id, name) + 3 rows via isql inside the container, retrying while the server
    finishes creating FIREBIRD_DATABASE right after the healthcheck first passes."""
    container_id = _firebird_container_id()
    ddl = (
        "CREATE TABLE widgets (id INTEGER, name VARCHAR(64));\n"
        "COMMIT;\n"
        + "\n".join(f"INSERT INTO widgets VALUES ({wid}, '{name}');" for wid, name in _WIDGETS)
        + "\nCOMMIT;\n"
    )
    deadline = time.monotonic() + 60
    last_err: subprocess.CalledProcessError | None = None
    while time.monotonic() < deadline:
        try:
            _isql(container_id, ddl)
            return
        except subprocess.CalledProcessError as e:
            last_err = e
            time.sleep(3)
    detail = last_err.stderr if last_err is not None else "<no seed attempt>"
    raise RuntimeError(f"firebird table seed never succeeded: {last_err!r}: {detail!r}")


@pytest.mark.requires_firebird
def test_firebird_attached_and_queried_through_duckdb_engine():
    """Drive the REAL DuckDBFirebirdConnector.details() ATTACH DDL against an in-process DuckDB
    connection — the same seam provisa.federation.duckdb_backend.DuckDBBackend's persistent
    DuckDBFederationRuntime uses (REQ-899/1097). No landing: `firebird_scan` reads the source live.
    """
    import duckdb

    from provisa.core.models import Source, SourceType
    from provisa.federation.connector_duckdb import DuckDBFirebirdConnector

    os.environ["DUCKDB_FIREBIRD_CLIENT_LIBRARY"] = _ensure_firebird_client_lib()

    _seed_firebird()

    src = Source(
        id="firebird_itest",
        type=SourceType.firebird,
        host=_FB_HOST,
        port=_FB_PORT,
        username=_FB_USER,
        password=_FB_PASSWORD,
        path=_FB_DB_PATH_IN_CONTAINER,
    )
    connector = DuckDBFirebirdConnector()
    details = connector.details(src)
    assert "ATTACH" in details["attach"] and "(TYPE firebird)" in details["attach"]

    conn = duckdb.connect()
    try:
        install_sql = connector._install_sql()  # "INSTALL firebird FROM community"
        conn.execute(install_sql)
        conn.execute(f"LOAD {connector.extension}")

        rows = conn.execute(
            "SELECT count(*) FROM duckdb_functions() "
            f"WHERE function_name = '{connector.probe_symbol}'"
        ).fetchall()
        assert rows[0][0] >= 1  # firebird_scan registered — extension genuinely loaded

        conn.execute(details["attach"])  # the real ATTACH DDL, unmodified

        result = conn.execute(f'SELECT id, name FROM "{src.id}".{_TABLE} ORDER BY id').fetchall()
        assert [(r[0], r[1]) for r in result] == _WIDGETS
    finally:
        conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
