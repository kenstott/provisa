# Copyright (c) 2026 Kenneth Stott
# Canary: b1c74e05-3d92-4a6f-8e10-27fd9c4a6b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An ephemeral AWS Redshift Serverless cluster, provisioned as a pytest fixture.

Why a fixture and not a wrapper script
--------------------------------------
Redshift is AWS-only and BILLABLE, so unlike every self-provisioning source (cassandra, exasol,
clickhouse) it cannot live in docker-compose and must not be left standing. The obvious shape — a
script that provisions, shells out to pytest, and deletes — puts the cluster up *before* pytest
starts, which means it stands through the entire isolated-stack bring-up: ``pytest_collection_finish``
(``tests/conftest.py:450``) waits on ``acquire_stack_slot()`` and then boots Trino and friends, and
on a busy Docker VM that is tens of minutes of a live cluster doing nothing.

As a session fixture it is provisioned at first *test setup* instead, which is strictly after the
compose stack is healthy, so the cluster's lifetime is the tests and nothing else.

Teardown runs from a ``finally`` and is idempotent, so a failing assertion, an errored fixture, or
Ctrl-C still deletes both resources. For the case it cannot cover — SIGKILL, a lost machine —
``reap_orphans`` deletes anything left behind by an earlier run, and is called at provision time so
every run cleans up after the last one.

Credentials live in the project's ``.env``, never ``~/.aws``:

    REDSHIFT_AWS_ACCESS_KEY_ID=AKIA...
    REDSHIFT_AWS_SECRET_ACCESS_KEY=...
    REDSHIFT_AWS_REGION=us-east-1

The ``REDSHIFT_AWS_`` prefix is required, not cosmetic. Plain ``AWS_ACCESS_KEY_ID`` /
``AWS_SECRET_ACCESS_KEY`` / ``AWS_ENDPOINT_OVERRIDE`` / ``AWS_REGION`` are already taken in that
file by Cloudflare R2; reusing those names would authenticate against the wrong service. Every
credential is passed to ``boto3.Session`` explicitly so boto3's own environment and shared-config
resolution is never reached and the R2 values cannot leak in.
"""

from __future__ import annotations

import os
import secrets
import socket
import ssl
import string
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import certifi
import pytest
from botocore.exceptions import ClientError
from dotenv import dotenv_values

_REPO = Path(__file__).resolve().parent.parent.parent
_ENV_FILE = _REPO / ".env"

_PREFIX = "provisa-e2e"
_DB = "dev"
_ADMIN_USER = "provisa"
_BASE_CAPACITY_RPU = 8  # Serverless minimum; anything larger only raises the per-second rate.
_AVAILABLE_TIMEOUT_S = 900
_DELETE_TIMEOUT_S = 900
# GetWorkgroup reporting AVAILABLE means the control plane finished creating the workgroup, not
# that the data-plane endpoint is reachable yet — DNS + ENI attachment + security-group propagation
# trail behind that status flip, which was surfacing as a bare `psycopg2.OperationalError: ...
# Operation timed out` in _seed_redshift() (REQ-1097 seeding). Measured directly (an isolated run of
# this fixture): a 180s budget was not enough — a fresh public workgroup endpoint took longer than
# that to become internet-reachable after AVAILABLE. 480s comfortably covers the observed lag.
_TCP_READY_TIMEOUT_S = 480
# A run that outlives this is dead, not slow: the whole session (stack boot included) is well
# under an hour, and anything older is debris from a killed process that is still billing.
_ORPHAN_AGE = timedelta(hours=2)

_AWS_KEYS = ("REDSHIFT_AWS_ACCESS_KEY_ID", "REDSHIFT_AWS_SECRET_ACCESS_KEY")
_ENV_KEYS = (
    "REDSHIFT_HOST",
    "REDSHIFT_PORT",
    "REDSHIFT_DATABASE",
    "REDSHIFT_USER",
    "REDSHIFT_PASSWORD",
)


def _conf() -> dict[str, str | None]:
    """.env plus the process environment, with the exported environment winning.

    The override direction lets a rotated key be tried without editing the file; ``.env`` is the
    normal source.
    """
    return {**dotenv_values(_ENV_FILE), **os.environ}


def have_aws_creds() -> bool:
    conf = _conf()
    return all(conf.get(k) for k in _AWS_KEYS)


def _session() -> boto3.Session:
    conf = _conf()
    missing = [k for k in _AWS_KEYS if not conf.get(k)]
    if missing:
        raise RuntimeError(
            f"{', '.join(missing)} not set in {_ENV_FILE} — create an AWS IAM user with "
            "scripts/redshift-e2e-iam-policy.json attached and put its access key there. The "
            "REDSHIFT_AWS_ prefix is required: plain AWS_* in this .env belongs to Cloudflare R2."
        )
    return boto3.Session(
        aws_access_key_id=conf["REDSHIFT_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=conf["REDSHIFT_AWS_SECRET_ACCESS_KEY"],
        region_name=conf.get("REDSHIFT_AWS_REGION") or "us-east-1",
    )


def _password() -> str:
    """A Redshift-legal admin password: upper+lower+digit required, ``/ " @`` and space rejected."""
    body = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(24))
    return f"Pv{body}9x"


def _egress_ip() -> str:
    # certifi's bundle explicitly, not the interpreter default: the python.org framework build has
    # no system CA store wired up, so a default-context urlopen fails CERTIFICATE_VERIFY_FAILED.
    # certifi is already present as a botocore dependency.
    ctx = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=15, context=ctx) as resp:
        return resp.read().decode().strip()


def _stamp_of(name: str) -> datetime | None:
    """The UTC creation time encoded in a ``provisa-e2e[-wg]-<YYYYmmddHHMMSS>`` name.

    Read from the name rather than the API's creationDate so a namespace and its workgroup are
    judged by the same instant, and so a name this repo did not create parses to None and is left
    alone.
    """
    tail = name.rsplit("-", 1)[-1]
    try:
        return datetime.strptime(tail, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def reap_orphans(rss: Any, *, now: datetime) -> None:
    """Delete ``provisa-e2e-*`` resources older than ``_ORPHAN_AGE``.

    Covers the one case the ``finally`` cannot: a SIGKILL between provision and teardown leaves a
    namespace billing for managed storage indefinitely. Age-gated so a concurrently running
    session's cluster is never deleted out from under it.
    """
    for wg in rss.list_workgroups().get("workgroups", []):
        name = wg["workgroupName"]
        stamp = _stamp_of(name)
        if name.startswith(_PREFIX) and stamp and now - stamp > _ORPHAN_AGE:
            print(f"reaping orphaned workgroup {name}", flush=True)
            _delete_workgroup(rss, name)
    for ns in rss.list_namespaces().get("namespaces", []):
        name = ns["namespaceName"]
        stamp = _stamp_of(name)
        if name.startswith(_PREFIX) and stamp and now - stamp > _ORPHAN_AGE:
            print(f"reaping orphaned namespace {name}", flush=True)
            _delete_namespace(rss, name)


def _wait_available(rss: Any, workgroup: str) -> dict:
    deadline = time.monotonic() + _AVAILABLE_TIMEOUT_S
    status = "PENDING"
    while time.monotonic() < deadline:
        try:
            wg = rss.get_workgroup(workgroupName=workgroup)["workgroup"]
        except ClientError:
            time.sleep(10)
            continue
        status = wg["status"]
        if status == "AVAILABLE":
            return wg
        time.sleep(10)
    raise RuntimeError(
        f"workgroup {workgroup} not AVAILABLE within {_AVAILABLE_TIMEOUT_S}s (status={status})"
    )


def _wait_tcp_ready(host: str, port: int) -> None:
    """Block until the endpoint accepts a raw TCP connection.

    Called after GetWorkgroup reports AVAILABLE and the security-group rule is authorized — status
    AVAILABLE only means the control plane is done, not that DNS/ENI/security-group propagation to
    the data-plane endpoint has finished (see _TCP_READY_TIMEOUT_S above).
    """
    deadline = time.monotonic() + _TCP_READY_TIMEOUT_S
    last_exc: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=5):
                return
        except OSError as exc:
            last_exc = exc
            time.sleep(5)
    raise RuntimeError(
        f"{host}:{port} not accepting TCP connections within {_TCP_READY_TIMEOUT_S}s"
    ) from last_exc


def _delete_workgroup(rss: Any, workgroup: str) -> None:
    try:
        rss.delete_workgroup(workgroupName=workgroup)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceNotFoundException":
            print(f"delete_workgroup {workgroup}: {exc}", flush=True)
    # The namespace cannot be deleted while a workgroup still references it, so block until gone.
    deadline = time.monotonic() + _DELETE_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            rss.get_workgroup(workgroupName=workgroup)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                return
        time.sleep(10)
    print(f"workgroup {workgroup} still present — DELETE IT MANUALLY", flush=True)


def _delete_namespace(rss: Any, namespace: str) -> None:
    try:
        # No finalSnapshotName/finalSnapshotRetentionPeriod: omitting both is what suppresses the
        # final snapshot. Passing a retention of 0 without a name is rejected by the API, and a
        # rejected delete would leave the namespace standing and billing for managed storage.
        rss.delete_namespace(namespaceName=namespace)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            return
        print(
            f"namespace {namespace} delete failed ({exc}) — DELETE IT MANUALLY; it accrues "
            "managed-storage charges while it exists",
            flush=True,
        )


@pytest.fixture(scope="session")
def redshift_cluster():
    """Provision a Serverless workgroup, export ``REDSHIFT_*``, delete both on the way out.

    Session-scoped so the source e2e and the pipeline e2e share one cluster: the billable window
    is one provision/delete pair for the whole run, not one per test file.

    A pre-set ``REDSHIFT_HOST`` pins a standing cluster: nothing is created, so nothing is deleted
    either — this must never delete a cluster it did not create. Same rule as ``synapse_lane`` in
    ``tests/integration/synapse_provision.py``.
    """
    if os.environ.get("REDSHIFT_HOST"):
        yield {
            "host": os.environ["REDSHIFT_HOST"],
            "port": int(os.environ["REDSHIFT_PORT"]),
            "database": os.environ["REDSHIFT_DATABASE"],
        }
        return

    session = _session()
    rss = session.client("redshift-serverless")
    ec2 = session.client("ec2")

    now = datetime.now(timezone.utc)
    reap_orphans(rss, now=now)

    # Stamped per run so a leaked cluster from an earlier aborted run can never collide with, or be
    # silently reused by, this one — reuse would also mean querying its stale seed data.
    stamp = now.strftime("%Y%m%d%H%M%S")
    namespace, workgroup = f"{_PREFIX}-{stamp}", f"{_PREFIX}-wg-{stamp}"
    password = _password()

    print(f"== provisioning redshift namespace {namespace} ==", flush=True)
    rss.create_namespace(
        namespaceName=namespace,
        dbName=_DB,
        adminUsername=_ADMIN_USER,
        adminUserPassword=password,
    )
    prior = {k: os.environ.get(k) for k in _ENV_KEYS}
    try:
        print(f"== provisioning workgroup {workgroup} ({_BASE_CAPACITY_RPU} RPU, public) ==", flush=True)
        rss.create_workgroup(
            workgroupName=workgroup,
            namespaceName=namespace,
            baseCapacity=_BASE_CAPACITY_RPU,
            publiclyAccessible=True,
        )
        wg = _wait_available(rss, workgroup)
        endpoint = wg["endpoint"]["address"]
        port = int(wg["endpoint"]["port"])

        # Redshift's default VPC security group does not allow inbound 5439. Scope the rule to this
        # host's egress IP: an 0.0.0.0/0 rule on a public database endpoint is a live exposure for
        # as long as the cluster stands. The isolated Trino container reaches Redshift out through
        # this same NAT address, so one rule covers both the seeding client and the engine.
        my_ip = _egress_ip()
        for group_id in wg["securityGroupIds"]:
            print(f"== authorizing {my_ip}/32 -> {group_id}:{port} ==", flush=True)
            try:
                ec2.authorize_security_group_ingress(
                    GroupId=group_id,
                    IpPermissions=[
                        {
                            "IpProtocol": "tcp",
                            "FromPort": port,
                            "ToPort": port,
                            "IpRanges": [{"CidrIp": f"{my_ip}/32", "Description": "provisa-e2e"}],
                        }
                    ],
                )
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                    raise

        print(f"== waiting for TCP reachability at {endpoint}:{port} ==", flush=True)
        _wait_tcp_ready(endpoint, port)

        os.environ["REDSHIFT_HOST"] = endpoint
        os.environ["REDSHIFT_PORT"] = str(port)
        os.environ["REDSHIFT_DATABASE"] = _DB
        os.environ["REDSHIFT_USER"] = _ADMIN_USER
        os.environ["REDSHIFT_PASSWORD"] = password
        print(f"== redshift ready at {endpoint}:{port} ==", flush=True)
        yield {"host": endpoint, "port": port, "database": _DB}
    finally:
        for key, value in prior.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        print(f"== deleting {workgroup} / {namespace} ==", flush=True)
        _delete_workgroup(rss, workgroup)
        _delete_namespace(rss, namespace)
        print("== redshift teardown done ==", flush=True)
