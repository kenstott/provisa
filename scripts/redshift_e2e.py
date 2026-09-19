# Copyright (c) 2026 Kenneth Stott
# Canary: 1d549658-81bd-4d27-8d86-b06be147e002
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CLI bridge to the ephemeral Redshift Serverless fixture, for callers outside pytest (REQ-1730).

`tests/integration/redshift_cluster.py`'s `redshift_cluster` fixture is pytest-scoped (a
generator), so nothing outside a pytest session can provision/tear down through it directly — the
Playwright-side `engine-swap.spec.ts` reboot harness needs exactly that (provision once in
`beforeAll`, register against it, survive a real backend kill+restart, tear down in `afterAll`).
Rather than reimplementing namespace/workgroup/security-group provisioning a second time, this
reuses `redshift_cluster.py`'s own module-level functions directly (`_session`, `_password`,
`reap_orphans`, `_wait_available`, `_wait_tcp_ready`, `_delete_workgroup`, `_delete_namespace`) —
the only new code here is the CLI plumbing and the state file that lets `up` and `down` be two
separate process invocations from a shell/Node caller instead of one Python generator's two halves.

    python3 scripts/redshift_e2e.py up    # provisions, seeds public.provisa_widgets_e2e, prints
                                           # {"host": ..., "port": ..., "database": ..., ...} JSON,
                                           # writes the same plus resource names to the state file
    python3 scripts/redshift_e2e.py down  # reads the state file, deletes workgroup+namespace

State file location: $REDSHIFT_E2E_STATE_FILE, default /tmp/redshift_e2e_state.json. `down` is a
no-op (not an error) if the state file is missing — makes it always safe to call from a test's
`afterAll` even if `up` never got far enough to write it.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from tests.integration.redshift_cluster import (  # noqa: E402
    _ADMIN_USER,
    _DB,
    _PREFIX,
    _delete_namespace,
    _delete_workgroup,
    _password,
    _session,
    _wait_available,
    _wait_tcp_ready,
    _egress_ip,
    reap_orphans,
)

_STATE_FILE = Path(os.environ.get("REDSHIFT_E2E_STATE_FILE", "/tmp/redshift_e2e_state.json"))
_SCHEMA = "public"
_TABLE = "provisa_widgets_e2e"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def _seed(host: str, port: int, database: str, user: str, password: str) -> None:
    import psycopg2

    deadline = time.monotonic() + 120
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
                connect_timeout=10,
            )
        except psycopg2.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(5)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_SCHEMA}.{_TABLE}")
        cur.execute(f"CREATE TABLE {_SCHEMA}.{_TABLE} (id INTEGER, name VARCHAR(64))")
        for wid, name in _WIDGETS:
            cur.execute(f"INSERT INTO {_SCHEMA}.{_TABLE} (id, name) VALUES (%s, %s)", (wid, name))
        cur.close()
    finally:
        conn.close()


def _up() -> None:
    # Idempotency guard (REQ-1730): playwright.config.ts's global `retries: 1` re-invokes a failed
    # test's `beforeAll` on a FRESH worker without ever calling the failed attempt's `afterAll` —
    # reproduced live, two full Serverless workgroups billing simultaneously from one retried test.
    # The state file survives across worker processes within the same run (unlike in-memory
    # state), so a retry that finds one already there reuses it instead of provisioning a second.
    session = _session()
    rss = session.client("redshift-serverless")
    ec2 = session.client("ec2")

    if _STATE_FILE.exists():
        state = json.loads(_STATE_FILE.read_text())
        try:
            rss.get_workgroup(workgroupName=state["workgroup"])
        except Exception:
            # Stale state from a run whose teardown never ran (crash) — its own workgroup is
            # already gone, so trusting the file would hand back dead credentials. Discard it and
            # provision fresh rather than silently proceeding against nothing.
            print(
                f"== stale state file points at deleted {state['workgroup']} — discarding ==",
                file=sys.stderr,
                flush=True,
            )
            _STATE_FILE.unlink()
        else:
            print(
                f"== reusing already-provisioned {state['workgroup']} (retry) ==",
                file=sys.stderr,
                flush=True,
            )
            print(json.dumps(state))
            return

    now = datetime.now(timezone.utc)
    reap_orphans(rss, now=now)

    stamp = now.strftime("%Y%m%d%H%M%S")
    namespace, workgroup = f"{_PREFIX}-{stamp}", f"{_PREFIX}-wg-{stamp}"
    password = _password()

    print(f"== provisioning redshift namespace {namespace} ==", file=sys.stderr, flush=True)
    rss.create_namespace(
        namespaceName=namespace,
        dbName=_DB,
        adminUsername=_ADMIN_USER,
        adminUserPassword=password,
    )
    print(f"== provisioning workgroup {workgroup} ==", file=sys.stderr, flush=True)
    rss.create_workgroup(
        workgroupName=workgroup, namespaceName=namespace, baseCapacity=8, publiclyAccessible=True
    )
    wg = _wait_available(rss, workgroup)
    endpoint = wg["endpoint"]["address"]
    port = int(wg["endpoint"]["port"])

    my_ip = _egress_ip()
    for group_id in wg["securityGroupIds"]:
        print(f"== authorizing {my_ip}/32 -> {group_id}:{port} ==", file=sys.stderr, flush=True)
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
        except Exception as exc:
            if "InvalidPermission.Duplicate" not in str(exc):
                raise

    print(f"== waiting for TCP reachability at {endpoint}:{port} ==", file=sys.stderr, flush=True)
    _wait_tcp_ready(endpoint, port)

    print("== seeding public.provisa_widgets_e2e ==", file=sys.stderr, flush=True)
    _seed(endpoint, port, _DB, _ADMIN_USER, password)

    state = {
        "namespace": namespace,
        "workgroup": workgroup,
        "host": endpoint,
        "port": port,
        "database": _DB,
        "user": _ADMIN_USER,
        "password": password,
    }
    _STATE_FILE.write_text(json.dumps(state))
    print(json.dumps(state))


def _down() -> None:
    if not _STATE_FILE.exists():
        print("no state file, nothing to tear down", file=sys.stderr, flush=True)
        return
    state = json.loads(_STATE_FILE.read_text())
    session = _session()
    rss = session.client("redshift-serverless")
    print(
        f"== deleting {state['workgroup']} / {state['namespace']} ==", file=sys.stderr, flush=True
    )
    _delete_workgroup(rss, state["workgroup"])
    _delete_namespace(rss, state["namespace"])
    _STATE_FILE.unlink(missing_ok=True)
    print("== redshift teardown done ==", file=sys.stderr, flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("up", "down"):
        print("usage: redshift_e2e.py <up|down>", file=sys.stderr)
        sys.exit(1)
    (_up if sys.argv[1] == "up" else _down)()
