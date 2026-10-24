# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""scripts/ci/lanes/with_warehouses.py: the core UI lane's Redshift and Synapse stand up only for
a run that reads them, and a missing credential fails the run by name, never a skipped spec."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _module():
    spec = importlib.util.spec_from_file_location(
        "with_warehouses", REPO / "scripts" / "ci" / "lanes" / "with_warehouses.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_only_a_run_reading_the_warehouse_spec_stands_them_up():
    w = _module()
    assert w.wanted("")  # every spec
    assert w.wanted("e2e/source-to-query-cloud-warehouse.spec.ts:302")
    assert not w.wanted("e2e/tables-role-ttl.spec.ts e2e/reports-tab.spec.ts")


def test_a_run_that_does_not_read_them_runs_its_command_alone(monkeypatch):
    w = _module()
    monkeypatch.setenv("PROVISA_E2E_SPECS", "e2e/tables-role-ttl.spec.ts")
    for name in w.NEEDS:
        monkeypatch.delenv(name, raising=False)
    assert w.main([sys.executable, "-c", "raise SystemExit(3)"]) == 3


def test_a_missing_credential_fails_the_run_by_name(monkeypatch, capsys):
    w = _module()
    monkeypatch.delenv("PROVISA_E2E_SPECS", raising=False)
    for name in w.NEEDS:
        monkeypatch.setenv(name, "x")
    monkeypatch.delenv("SYNAPSE_ADLS_URL")
    assert w.main([sys.executable, "-c", "pass"]) == 1
    assert "SYNAPSE_ADLS_URL" in capsys.readouterr().out
