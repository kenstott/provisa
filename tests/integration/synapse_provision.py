# Copyright (c) 2026 Kenneth Stott
# Canary: 14174e77-008b-4271-8a0d-c80892ade4cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Ephemeral Azure Synapse serverless lane: provision -> test -> destroy.

Why this exists
---------------
Synapse is Azure-only and billable, so unlike every self-provisioning source (cassandra, exasol,
clickhouse) it cannot live in docker-compose and must not be left standing between runs. This module
provisions the whole lane on demand and destroys it in a ``finally``, so nothing accrues charges
while no test is running. A serverless SQL pool is what makes that affordable: the pool bills per TB
scanned rather than by wall clock, the workspace itself is free, and the only clock-billed resource
is the ADLS account holding one ~2 KB Parquet -- so a run costs a fraction of a cent.

Everything is created inside ONE stamped resource group, and teardown deletes that group. A single
delete cannot half-succeed and leave an orphan the way per-resource deletes can, and the stamp means
a leaked group from an aborted run can never be silently reused by the next one (reuse would also
mean querying its stale seed data).

Auth
----
There is no secret to configure. ``MssqlWarehouseRuntime`` authenticates with
``DefaultAzureCredential`` and the Azure CLI is the credential source, so ``az login`` is both the
provisioning credential and the query credential -- one identity, no keys in ``.env``. Serverless
``OPENROWSET`` without an explicit DATA_SOURCE reads storage as the *calling* AAD identity, which is
why the signed-in user is granted Storage Blob Data Contributor on the account it creates.

Set ``SYNAPSE_SQL_SERVER`` / ``SYNAPSE_DATABASE`` / ``SYNAPSE_ADLS_URL`` to pin a pre-existing
workspace; provisioning and teardown are then both skipped and the standing resources are used
as-is.
"""

from __future__ import annotations

import os
import secrets
import string
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

# Regions tried, in order, when SYNAPSE_AZ_LOCATION is unset. US-first for round-trip latency from
# the developer host; the European entries exist so a US-wide capacity block cannot strand the lane.
_LOCATION_CANDIDATES = (
    "eastus",
    "eastus2",
    "centralus",
    "westus2",
    "westus3",
    "westeurope",
    "northeurope",
)
_FILESYSTEM = "synapsefs"
_DATABASE = "provisa_syn"
_PARQUET_PATH = "provisa_ext/orders.parquet"
_BLOB_ROLE = "Storage Blob Data Contributor"

_AZ_TIMEOUT_S = 1800  # workspace create is the long pole (~5 min); RG delete can reach ~15 min
_RBAC_TIMEOUT_S = 600  # AAD role assignments are eventually consistent -- minutes, not seconds
_AZ_TRANSIENT_MARKERS = (
    "read timed out",
    "connection aborted",
    "connection reset",
    "remote end closed connection",
)
_AZ_TRANSIENT_RETRIES = 4


def _az(*args: str, timeout: int = _AZ_TIMEOUT_S) -> str:
    # az CLI's own HTTP client to management.azure.com has a fixed ~300s read timeout that is
    # shorter than our subprocess budget, so a single slow ARM control-plane response fails the
    # call well before `timeout` is reached -- retry those transient transport failures here.
    for attempt in range(_AZ_TRANSIENT_RETRIES + 1):
        proc = subprocess.run(
            ["az", *args], capture_output=True, text=True, timeout=timeout, check=False
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
        stderr = proc.stderr.strip()
        if attempt == _AZ_TRANSIENT_RETRIES or not any(
            m in stderr.lower() for m in _AZ_TRANSIENT_MARKERS
        ):
            raise RuntimeError(f"az {' '.join(args)} failed ({proc.returncode}): {stderr}")
        time.sleep(5 * (attempt + 1))
    raise AssertionError("unreachable")


def cli_ready() -> str | None:
    """``None`` when the lane can provision; otherwise the reason it cannot."""
    try:
        _az("account", "show", "-o", "none", timeout=60)
    except FileNotFoundError:
        return "az CLI not installed"
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        return f"az CLI not logged in ({exc})"
    return None


def _sql_password() -> str:
    """A T-SQL-legal admin password. Never used to connect -- workspace create demands one, and AAD
    token auth is what the runtime actually uses -- but a weak literal would be a standing account."""
    body = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(24))
    return f"Pv{body}9x"


def _seed_parquet(directory: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = directory / "orders.parquet"
    pq.write_table(
        pa.table(
            {
                "order_id": pa.array([1, 2, 3], pa.int64()),
                "customer": pa.array(["ada", "grace", "alan"], pa.string()),
                "amount": pa.array([10.5, 20.25, 30.0], pa.float64()),
            }
        ),
        path,
    )
    return path


def _resolve_location() -> str:
    """The region this subscription may currently create a SQL server in.

    A Synapse workspace carries a Windows Azure SQL Database server, and Azure gates the creation of
    new ones per subscription AND region — a blocked region fails workspace create with
    ``SqlServerRegionDoesNotAllowProvisioning`` after several minutes of provisioning. Which regions
    are open changes over time and differs per subscription, so it is asked rather than assumed: the
    Microsoft.Sql capabilities endpoint reports ``Available`` for a region that will accept a new
    server and ``Visible`` for one that only lists in the portal. An explicit ``SYNAPSE_AZ_LOCATION``
    is used as given and never probed — pinning a region is a deliberate choice, and silently moving
    the lane elsewhere would hide a data-residency decision.
    """
    pinned = os.environ.get("SYNAPSE_AZ_LOCATION")
    if pinned:
        return pinned
    subscription = _az("account", "show", "--query", "id", "-o", "tsv", timeout=60)
    for location in _LOCATION_CANDIDATES:
        status = _az(
            "rest",
            "--method",
            "get",
            "--url",
            f"https://management.azure.com/subscriptions/{subscription}"
            f"/providers/Microsoft.Sql/locations/{location}/capabilities?api-version=2021-11-01",
            "--query",
            "status",
            "-o",
            "tsv",
            timeout=120,
        )
        if status == "Available":
            return location
    raise RuntimeError(
        "no candidate Azure region accepts new SQL server creation for this subscription "
        f"({', '.join(_LOCATION_CANDIDATES)}); set SYNAPSE_AZ_LOCATION to one that does"
    )


def _register_provider() -> None:
    state = _az(
        "provider", "show", "-n", "Microsoft.Synapse", "--query", "registrationState", "-o", "tsv"
    )
    if state != "Registered":
        _az("provider", "register", "-n", "Microsoft.Synapse", "--wait")


def _wait_for_openrowset(server: str, adls_url: str) -> None:
    """Block until the blob role assignment is live.

    The RBAC grant is eventually consistent, so the first OPENROWSET after ``role assignment create``
    routinely fails with an authorization error that resolves itself minutes later. Polling here
    means that latency surfaces as provisioning time rather than as a test failure.
    """
    import pyodbc

    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

    deadline = time.monotonic() + _RBAC_TIMEOUT_S
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            rt = MssqlWarehouseRuntime(server=server, database=_DATABASE, engine_name="synapse")
            try:
                cur = rt.connection.cursor()
                try:
                    cur.execute(
                        f"SELECT TOP 1 * FROM OPENROWSET(BULK '{adls_url}', FORMAT = 'PARQUET') AS r"
                    )
                    cur.fetchall()
                    return
                finally:
                    cur.close()
            finally:
                rt.close()
        except pyodbc.Error as exc:
            last = exc
            time.sleep(15)
    raise RuntimeError(
        f"OPENROWSET on {adls_url} never became readable within {_RBAC_TIMEOUT_S}s: {last}"
    )


def _create_database(server: str) -> None:
    """Serverless objects must live in a user database -- ``master`` rejects view creation.

    ``CREATE DATABASE`` cannot run inside a transaction, so the connection is switched to autocommit;
    pyodbc opens one implicitly otherwise.
    """
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

    rt = MssqlWarehouseRuntime(server=server, database="master", engine_name="synapse")
    try:
        rt.connection.autocommit = True
        cur = rt.connection.cursor()
        try:
            cur.execute(f"CREATE DATABASE [{_DATABASE}]")
        finally:
            cur.close()
    finally:
        rt.close()


def _teardown(resource_group: str) -> None:
    print(f"== teardown: resource group {resource_group} ==", flush=True)
    try:
        _az("group", "delete", "-n", resource_group, "--yes")
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        print(
            f"resource group {resource_group} delete failed ({exc}) -- DELETE IT MANUALLY; "
            "its storage account bills for as long as it exists",
            file=sys.stderr,
            flush=True,
        )
        return
    print(f"== teardown done: {resource_group} ==", flush=True)


@contextmanager
def synapse_lane():
    """Yield ``(sql_server, database, adls_url)``, destroying every resource on exit.

    A pre-set ``SYNAPSE_SQL_SERVER`` pins a standing workspace: nothing is created, so nothing is
    deleted either -- this must never delete a workspace it did not create.
    """
    pinned = os.environ.get("SYNAPSE_SQL_SERVER")
    if pinned:
        yield pinned, os.environ["SYNAPSE_DATABASE"], os.environ["SYNAPSE_ADLS_URL"]
        return

    stamp = datetime.now(timezone.utc).strftime("%y%m%d%H%M%S")
    resource_group = f"provisa-syn-e2e-{stamp}"
    workspace = f"provisa-syn-e2e-{stamp}"
    storage = f"provisasyn{stamp}"  # 22 chars, lowercase alnum -- the storage naming rule

    _register_provider()
    location = _resolve_location()
    user_oid = _az("ad", "signed-in-user", "show", "--query", "id", "-o", "tsv", timeout=120)

    print(f"== provisioning {resource_group} in {location} ==", flush=True)
    _az("group", "create", "-n", resource_group, "-l", location, "-o", "none")
    try:
        # Hierarchical namespace is not optional: Synapse requires ADLS Gen2 for its primary account.
        _az(
            "storage",
            "account",
            "create",
            "-n",
            storage,
            "-g",
            resource_group,
            "-l",
            location,
            "--sku",
            "Standard_LRS",
            "--kind",
            "StorageV2",
            "--enable-hierarchical-namespace",
            "true",
            "-o",
            "none",
        )
        key = _az(
            "storage",
            "account",
            "keys",
            "list",
            "-n",
            storage,
            "-g",
            resource_group,
            "--query",
            "[0].value",
            "-o",
            "tsv",
        )
        # The account key -- not --auth-mode login -- for the filesystem and upload: those run
        # seconds after the role assignment, well inside its propagation window. The key never
        # leaves this process and dies with the account.
        _az(
            "storage",
            "fs",
            "create",
            "-n",
            _FILESYSTEM,
            "--account-name",
            storage,
            "--account-key",
            key,
            "-o",
            "none",
        )
        with tempfile.TemporaryDirectory() as tmp:
            local = _seed_parquet(Path(tmp))
            _az(
                "storage",
                "fs",
                "file",
                "upload",
                "-f",
                _FILESYSTEM,
                "-s",
                str(local),
                "-p",
                _PARQUET_PATH,
                "--account-name",
                storage,
                "--account-key",
                key,
                "--overwrite",
                "-o",
                "none",
            )

        storage_scope = _az(
            "storage",
            "account",
            "show",
            "-n",
            storage,
            "-g",
            resource_group,
            "--query",
            "id",
            "-o",
            "tsv",
        )
        # Granted before the workspace is created so the ~5 minutes of workspace provisioning double
        # as RBAC propagation time.
        _az(
            "role",
            "assignment",
            "create",
            "--role",
            _BLOB_ROLE,
            "--assignee-object-id",
            user_oid,
            "--assignee-principal-type",
            "User",
            "--scope",
            storage_scope,
            "-o",
            "none",
        )

        print(f"== provisioning workspace {workspace} ==", flush=True)
        _az(
            "synapse",
            "workspace",
            "create",
            "-n",
            workspace,
            "-g",
            resource_group,
            "-l",
            location,
            "--storage-account",
            storage,
            "--file-system",
            _FILESYSTEM,
            "--sql-admin-login-user",
            "provisaadmin",
            "--sql-admin-login-password",
            _sql_password(),
            "-o",
            "none",
        )
        msi = _az(
            "synapse",
            "workspace",
            "show",
            "-n",
            workspace,
            "-g",
            resource_group,
            "--query",
            "identity.principalId",
            "-o",
            "tsv",
        )
        _az(
            "role",
            "assignment",
            "create",
            "--role",
            _BLOB_ROLE,
            "--assignee-object-id",
            msi,
            "--assignee-principal-type",
            "ServicePrincipal",
            "--scope",
            storage_scope,
            "-o",
            "none",
        )
        # The workspace SQL endpoint denies every client IP by default. Scoped to this host's egress
        # address: a 0.0.0.0-255.255.255.255 rule leaves a public SQL endpoint open to the internet
        # for as long as the workspace stands.
        egress = _az(
            "rest",
            "--method",
            "get",
            "--url",
            "https://api.ipify.org?format=json",
            "--skip-authorization-header",
            "--query",
            "ip",
            "-o",
            "tsv",
            timeout=120,
        )
        _az(
            "synapse",
            "workspace",
            "firewall-rule",
            "create",
            "-n",
            "provisa-e2e-client",
            "--workspace-name",
            workspace,
            "-g",
            resource_group,
            "--start-ip-address",
            egress,
            "--end-ip-address",
            egress,
            "-o",
            "none",
        )

        sql_server = f"{workspace}-ondemand.sql.azuresynapse.net"
        adls_url = f"https://{storage}.dfs.core.windows.net/{_FILESYSTEM}/{_PARQUET_PATH}"
        _create_database(sql_server)
        _wait_for_openrowset(sql_server, adls_url)
        print(f"== synapse lane ready: {sql_server} ==", flush=True)
        yield sql_server, _DATABASE, adls_url
    finally:
        _teardown(resource_group)
