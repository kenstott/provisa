# Copyright (c) 2026 Kenneth Stott
# Canary: 1de43e81-79fa-4e75-b063-95ecab05a791
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud.sh's sandbox reset (reset_sandbox_state, REQ-2006/REQ-2068) drops the sandbox
orgs' schemas from every database each regions-tier store holds: the databases the store has, not
the ones a later first-launch.sh names. A sandbox VM still on a release whose stores hold provisa
alone (no ledger, no warehouse) is reset, and one whose stores hold a second region's database has
that one reset too.

A template the current code would record the same way is kept whole (REQ-2006, a patch keeps warm
demo templates): its control-plane recording names the DDL fingerprint of the tables the code
creates, which the reset reads from the pushed code itself. Sandboxes are reset as before."""

# Requirements: REQ-2006, REQ-2068

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"

# A docker on the sandbox VM: its containers, its regions-tier stores and the databases each holds
# (STORE_DBS: "store=db,db store=db"). psql into a database the store lacks fails as postgres does.
_DOCKER = r"""#!/bin/bash
set -euo pipefail
dbs_of() {
  for e in $STORE_DBS; do if [ "${e%%=*}" = "$1" ]; then echo "${e#*=}" | tr ',' '\n'; fi; done
}
case "$1" in
  inspect) echo provisa-image:test ;;
  run) echo "fingerprint ${*:2}" >> "$LOG"; echo "$DDL" ;;
  ps) if [ "$2" = -a ]; then printf '%s\n' $ALL; else printf '%s\n' $ALL | grep region-pg; fi ;;
  stop|start) echo "$1 ${*:2}" >> "$LOG" ;;
  exec)
    shift; [ "$1" = -i ] && shift
    store="$1"; db=""; query=""
    while [ $# -gt 0 ]; do
      case "$1" in -d) db="$2"; shift ;; -tAc|-c) query="$2"; shift ;; esac; shift
    done
    if [ -n "$query" ]; then
      case "$query" in *pg_database*) dbs_of "$store"; exit 0 ;; esac
    fi
    cat >> "$LOG.sql"
    dbs_of "$store" | grep -qx "$db" || {
      echo "psql: error: FATAL:  database \"$db\" does not exist" >&2; exit 2; }
    echo "dropped $store/$db" >> "$LOG" ;;
  *) echo "unexpected docker $*" >&2; exit 9 ;;
esac
"""


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


def _reset(
    tmp_path: Path, store_dbs: str, *, kept: str = "", ddl: str = "0123456789abcdef"
) -> tuple[subprocess.CompletedProcess, str]:
    """Run the reset; ``kept`` is what the control plane answers for the templates recorded
    under ``ddl``. The VM's /root is ``tmp_path/root``."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(_DOCKER)
    # sudo runs its command, with the VM's /root under this test's own folder.
    root = tmp_path / "root"
    (bin_dir / "sudo").write_text(f'#!/bin/bash\nset -- "${{@//\\/root\\//{root}/}}"\n"$@"\n')
    for f in ("docker", "sudo"):
        (bin_dir / f).chmod(0o755)
    stage = tmp_path / "stage"
    stage.mkdir()
    log = tmp_path / "log"
    log.touch()
    (tmp_path / "log.sql").touch()
    containers = (
        "compose-provisa-sandbox-1 compose-provisa-sandbox-regions-1 "
        "compose-region-pg-eu-1 compose-region-pg-us-1"
    )
    script = "\n".join(
        [
            "set -euo pipefail",
            f'STAGE="{stage}"',
            "NODE=provisa-saas-sandbox",
            'ssh_node() { bash -c "$1"; }',
            'run_sql() { echo "control plane: $2" >> "$LOG"; cat "$1" >> "$LOG.sql";'
            " [ \"$2\" != kept-templates ] || printf 'Tuples only is on.\\n%s\\n' $KEPT; }",
            _function("sandbox_containers"),
            _function("reset_sandbox_state"),
            "reset_sandbox_state",
        ]
    )
    result = subprocess.run(
        ["bash", "-c", script],
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin",
            "LOG": str(log),
            "ALL": containers,
            "STORE_DBS": store_dbs,
            "KEPT": kept,
            "DDL": ddl,
        },
        capture_output=True,
        text=True,
    )
    return result, log.read_text() + (tmp_path / "log.sql").read_text()


def test_a_sandbox_vm_whose_stores_hold_no_second_region_database_is_reset(tmp_path):
    """The sandbox VM on release v0.1.0-alpha.491: its stores hold postgres and provisa only."""
    result, log = _reset(
        tmp_path,
        "compose-region-pg-eu-1=postgres,provisa compose-region-pg-us-1=postgres,provisa",
    )
    assert result.returncode == 0, result.stderr
    assert "does not exist" not in result.stderr
    for store in ("compose-region-pg-eu-1", "compose-region-pg-us-1"):
        for db in ("postgres", "provisa"):
            assert f"dropped {store}/{db}" in log, log
    assert "control plane: reset-sandbox" in log


def test_a_second_region_database_a_store_holds_is_reset_too(tmp_path):
    result, log = _reset(
        tmp_path,
        "compose-region-pg-eu-1=postgres,provisa,ledger "
        "compose-region-pg-us-1=postgres,provisa,warehouse",
    )
    assert result.returncode == 0, result.stderr
    assert "dropped compose-region-pg-eu-1/ledger" in log, log
    assert "dropped compose-region-pg-us-1/warehouse" in log, log


def test_a_store_that_lists_no_database_fails_the_reset_by_name(tmp_path):
    result, log = _reset(tmp_path, "compose-region-pg-us-1=postgres,provisa")
    assert result.returncode != 0
    assert "compose-region-pg-eu-1" in result.stderr
    # The tier is started again on the way out (the reset's EXIT trap).
    assert "start compose-provisa-sandbox-1" in log, log


def _folders(tmp_path: Path, *names: str) -> list[Path]:
    made = []
    for data in ("sandbox-data", "sandbox-regions-data", "sandbox-regions-warehouse-data"):
        for name in names:
            folder = tmp_path / "root" / ".provisa" / data / "orgs" / name
            folder.mkdir(parents=True)
            made.append(folder)
    return made


def test_a_template_recorded_under_the_pushed_codes_ddl_is_kept_and_the_rest_reset(tmp_path):
    _folders(tmp_path, "sbxtplweblogs", "sbxtplretail", "sbx0123456789ab", "sbxrus0123456789ab")
    result, log = _reset(
        tmp_path,
        "compose-region-pg-eu-1=postgres,provisa compose-region-pg-us-1=postgres,provisa",
        kept="sbxtplweblogs",
    )
    assert result.returncode == 0, result.stderr
    # The fingerprint is the pushed code's own, read through the tier's image and mounts.
    assert "fingerprint --rm --volumes-from compose-provisa-sandbox-1" in log, log
    assert "'%@0123456789abcdef'" in log, log
    orgs = tmp_path / "root" / ".provisa" / "sandbox-data" / "orgs"
    assert sorted(p.name for p in orgs.iterdir()) == ["sbxtplweblogs"]
    regions = tmp_path / "root" / ".provisa" / "sandbox-regions-data" / "orgs"
    assert sorted(p.name for p in regions.iterdir()) == ["sbxtplweblogs"]
    # Its schemas and its orgs row too, on the control plane and in each region's store.
    assert "ARRAY['sbxtplweblogs']::text[]" in log and "ARRAY[]::text[]" not in log, log


def test_with_no_template_recorded_under_the_pushed_codes_ddl_everything_is_reset(tmp_path):
    _folders(tmp_path, "sbxtplweblogs", "sbx0123456789ab")
    result, log = _reset(tmp_path, "compose-region-pg-eu-1=provisa compose-region-pg-us-1=provisa")
    assert result.returncode == 0, result.stderr
    assert not any((tmp_path / "root" / ".provisa" / "sandbox-data" / "orgs").iterdir())
    assert "ARRAY[]::text[]" in log and "sbxtplweblogs'" not in log, log


def test_a_fingerprint_the_pushed_code_cannot_give_fails_the_reset_by_name(tmp_path):
    _folders(tmp_path, "sbxtplweblogs")
    result, log = _reset(
        tmp_path, "compose-region-pg-eu-1=provisa compose-region-pg-us-1=provisa", ddl="Traceback"
    )
    assert result.returncode != 0
    assert "DDL fingerprint" in result.stderr
    assert "start compose-provisa-sandbox-1" in log, log
