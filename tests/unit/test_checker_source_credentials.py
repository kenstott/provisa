# Copyright (c) 2026 Kenneth Stott
# Canary: 0f91a9ca-21a7-4429-b0c9-3b91f4d0f712
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""#276: a data-quality checker source's connection is what its steward entered in the Sources
form -- a value, or a reference they typed -- kept as entered and resolved where a scan uses it.
The product writes no reference of its own, and reading the model never resolves one, so a model
holding a reference whose value is not set still loads."""

# Requirements: REQ-1742

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]


def test_the_mapping_holds_the_connection_the_steward_entered():
    from provisa.api.admin._dq_registration import checker_mapping

    entered = SimpleNamespace(
        host="pg.internal",
        port=5439,
        database="provisa",
        username="dq_reader",
        password="${env:MY_DQ_PASSWORD}",
    )
    assert checker_mapping(entered) == {
        "host": "pg.internal",
        "port": 5439,
        "database": "provisa",
        "user": "dq_reader",
        "password": "${env:MY_DQ_PASSWORD}",
    }
    # left empty, it is stored empty: nothing is assumed in its place
    blank = SimpleNamespace(host=None, port=None, database=None, username=None, password=None)
    assert set(checker_mapping(blank).values()) == {None}


def test_creating_a_source_writes_no_reference_or_default_of_its_own():
    tree = ast.parse((REPO / "provisa/api/admin/schema_mutation.py").read_text())
    spelled = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Constant)
        and isinstance(n.value, str)
        and "${env:PROVISA_DQ" in n.value
    ]
    assert spelled == []
    assert "PROVISA_DQ_" not in (REPO / "provisa/api/admin/schema_mutation.py").read_text()


def test_a_model_holding_a_checker_reference_that_is_not_set_loads(monkeypatch):
    from provisa.core.config_loader import parse_config_dict

    monkeypatch.delenv("PROVISA_TEST_UNSET_DQ_PASSWORD", raising=False)
    monkeypatch.setenv("PROVISA_TEST_PG_PASSWORD", "pw")
    mapping = {
        "host": "localhost",
        "port": 5439,
        "database": "provisa",
        "user": "org_admin",
        "password": "${env:PROVISA_TEST_UNSET_DQ_PASSWORD}",
    }
    config = parse_config_dict(
        {
            "sources": [
                {"id": "dq", "type": "soda", "mapping": mapping},
                {
                    "id": "pg",
                    "type": "postgresql",
                    "host": "h",
                    "port": 5432,
                    "database": "d",
                    "username": "u",
                    "password": "${env:PROVISA_TEST_PG_PASSWORD}",
                },
            ],
            "domains": [],
            "tables": [],
            "roles": [],
        }
    )
    by_id = {s.id: s for s in config.sources}
    # the checker's connection is kept as written, for the scan to resolve
    assert by_id["dq"].mapping == mapping
    # every other source is resolved as before
    assert by_id["pg"].password == "pw"


def test_a_scan_resolves_the_reference_when_it_runs(monkeypatch):
    from provisa.dq.scan import checker_connection

    monkeypatch.setenv("PROVISA_TEST_DQ_PASSWORD", "secret")
    mapping = {
        "host": "localhost",
        "port": 5439,
        "database": "provisa",
        "user": "org_admin",
        "password": "${env:PROVISA_TEST_DQ_PASSWORD}",
    }
    assert checker_connection(mapping)["password"] == "secret"
    monkeypatch.delenv("PROVISA_TEST_DQ_PASSWORD")
    with pytest.raises(KeyError, match="PROVISA_TEST_DQ_PASSWORD"):
        checker_connection(mapping)
