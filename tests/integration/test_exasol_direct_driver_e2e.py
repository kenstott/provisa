# Copyright (c) 2026 Kenneth Stott
# Canary: 2b3b6024-981b-4ffd-a05e-878bb2bf060a
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: ExasolDriver — Provisa's own direct read (REQ-1731), via pyexasol.

Distinct from test_exasol_source_e2e.py, which drives ``exasol`` as a Trino connector-source
(TrinoExasolConnector, JDBC — Trino reads it live, nothing lands). This is the OTHER shape:
Provisa reads Exasol directly then lands a replica, the same way singlestore/snowflake/databricks
are — reachable on ANY engine (REQ-947's ``complete_reach``), not just Trino.

exasol/docker-db is published linux/amd64 only; under QEMU on arm64 its EXAStorage boot never
completes, so this is arch-gated the same way test_exasol_source_e2e.py is — a documented platform
gap, not a dodge.
"""

from __future__ import annotations

import hashlib
import os
import platform
import socket
import ssl

import pytest

from provisa.executor.drivers.exasol import ExasolDriver

_AMD64 = platform.machine() in ("x86_64", "amd64")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    pytest.mark.requires_exasol,
    pytest.mark.skipif(
        not _AMD64, reason="exasol/docker-db is amd64-only; unbootable under arm64 emulation"
    ),
]

_HOST = os.environ.get("EXASOL_HOST", "localhost")
_PORT = int(os.environ.get("EXASOL_PORT", "8563"))
_USER = "sys"
_PASSWORD = "exasol"  # image default (verified: exasol/docker-db, github.com/exasol/docker-db)
_SCHEMA = "PROVISA"
_TABLE = "DIRECT_DRIVER_WIDGETS"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def _served_fingerprint(host: str, port: int) -> str:
    """The SHA-256 fingerprint of the certificate the server presents -- what an operator reads
    off the server and enters as the source's ``tls_fingerprint``. Exasol 8 generates its
    certificate at boot, so there is no CA to trust; the driver verifies it by this pin, and an
    unpinned connect fails certificate verification (the self-signed chain)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE  # reading the certificate in order to pin it, not trusting it
    with socket.create_connection((host, port), timeout=30) as raw:
        with ctx.wrap_socket(raw, server_hostname=host) as tls:
            der = tls.getpeercert(binary_form=True)
    assert der is not None
    return hashlib.sha256(der).hexdigest().upper()


async def test_exasol_direct_driver_ddl_dml_select() -> None:
    """Real CREATE SCHEMA/TABLE, INSERT, SELECT through ExasolDriver against a real Exasol,
    reached the way a configured source reaches it: TLS pinned to the server's fingerprint."""
    driver = ExasolDriver()
    driver.configure({"tls_fingerprint": _served_fingerprint(_HOST, _PORT)})
    await driver.connect(host=_HOST, port=_PORT, database="", user=_USER, password=_PASSWORD)
    try:
        await driver.execute(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}")
        await driver.execute(f"OPEN SCHEMA {_SCHEMA}")
        await driver.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        await driver.execute(f"CREATE TABLE {_TABLE} (ID INTEGER, NAME VARCHAR(64))")
        values = ", ".join(f"({wid}, '{name}')" for wid, name in _WIDGETS)
        await driver.execute(f"INSERT INTO {_TABLE} VALUES {values}")
        result = await driver.execute(f"SELECT ID, NAME FROM {_TABLE} ORDER BY ID")
        assert result.column_names == ["ID", "NAME"]
        assert result.rows == _WIDGETS
    finally:
        await driver.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        await driver.close()
