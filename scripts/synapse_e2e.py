# Copyright (c) 2026 Kenneth Stott
# Canary: 91f05830-6338-420e-92a9-febe30ba1e8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CLI bridge to the ephemeral Azure Synapse fixture, for callers outside pytest (REQ-1730).

`tests/integration/synapse_provision.py`'s `synapse_lane` is a plain `@contextmanager` generator,
so nothing outside one process's `with` block can provision/tear down through it directly — the
Playwright-side `engine-swap.spec.ts` reboot harness needs exactly that (provision once in
`beforeAll`, register against it, survive a real backend kill+restart, tear down in `afterAll`).
Mirrors `redshift_e2e.py`'s own split of `redshift_cluster.py`: this reuses
`synapse_provision.py`'s own module-level `_provision()`/`_teardown()` functions directly — the
only new code here is the CLI plumbing and the state file that lets `up` and `down` be two
separate process invocations instead of one generator's two halves.

    python3 scripts/synapse_e2e.py up    # provisions, prints {"sql_server": ..., "database": ...,
                                          # "adls_url": ...} JSON, writes the same plus
                                          # resource_group to the state file
    python3 scripts/synapse_e2e.py down  # reads the state file, deletes the resource group

State file location: $SYNAPSE_E2E_STATE_FILE, default /tmp/synapse_e2e_state.json. `down` is a
no-op (not an error) if the state file is missing — makes it always safe to call from a test's
`afterAll` even if `up` never got far enough to write it.

Idempotency guard (REQ-1730): playwright.config.ts's global `retries: 1` re-invokes a failed
test's `beforeAll` on a FRESH worker without ever calling the failed attempt's `afterAll` —
reproduced live for `redshift_e2e.py`'s own lane this session, two full Serverless workgroups
billing simultaneously from one retried test. The same shape applies here (a resource group is
just as billable while standing), so `_up()` takes the identical precaution: a state file that
survives across worker processes is checked FIRST, and its resource group's continued existence
is verified against Azure (not just the file's presence) before reusing it.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from tests.integration.synapse_provision import _az, _provision, _teardown  # noqa: E402

_STATE_FILE = Path(os.environ.get("SYNAPSE_E2E_STATE_FILE", "/tmp/synapse_e2e_state.json"))


def _resource_group_exists(resource_group: str) -> bool:
    try:
        _az("group", "show", "-n", resource_group, "-o", "none", timeout=60)
    except Exception:
        return False
    return True


def _up() -> None:
    pinned = os.environ.get("SYNAPSE_SQL_SERVER")
    if pinned:
        state = {
            "resource_group": None,
            "sql_server": pinned,
            "database": os.environ["SYNAPSE_DATABASE"],
            "adls_url": os.environ["SYNAPSE_ADLS_URL"],
        }
        print(json.dumps(state))
        return

    if _STATE_FILE.exists():
        state = json.loads(_STATE_FILE.read_text())
        if state["resource_group"] and _resource_group_exists(state["resource_group"]):
            print(
                f"== reusing already-provisioned {state['resource_group']} (retry) ==",
                file=sys.stderr,
                flush=True,
            )
            print(json.dumps(state))
            return
        print(
            "== stale state file points at a gone/foreign resource group — discarding ==",
            file=sys.stderr,
            flush=True,
        )
        _STATE_FILE.unlink()

    resource_group, sql_server, database, adls_url = _provision()
    state = {
        "resource_group": resource_group,
        "sql_server": sql_server,
        "database": database,
        "adls_url": adls_url,
    }
    _STATE_FILE.write_text(json.dumps(state))
    print(json.dumps(state))


def _down() -> None:
    if not _STATE_FILE.exists():
        print("no state file, nothing to tear down", file=sys.stderr, flush=True)
        return
    state = json.loads(_STATE_FILE.read_text())
    if not state["resource_group"]:
        print("state file is a pinned workspace — nothing to delete", file=sys.stderr, flush=True)
        _STATE_FILE.unlink(missing_ok=True)
        return
    _teardown(state["resource_group"])
    _STATE_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("up", "down"):
        print("usage: synapse_e2e.py <up|down>", file=sys.stderr)
        sys.exit(1)
    (_up if sys.argv[1] == "up" else _down)()
