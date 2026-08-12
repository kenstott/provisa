# Copyright (c) 2026 Kenneth Stott
# Canary: 90d69bd5-becd-4781-a86c-6fb3c9ec2d6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1432: traces are enabled per subsystem, and the catalog database is off by default.

asyncpg carries every catalog read and metadata write, so instrumenting it drowns the query spans
an operator opened the live trace panel to read. The switches are named for subsystems rather than
for the instrumentation libraries behind them.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from provisa.core.models import OtelConfig, SubsystemTracesConfig

_REPO = Path(__file__).resolve().parents[2]


def test_catalog_database_traces_are_off_by_default():
    assert SubsystemTracesConfig().catalog_database is False


def test_every_other_subsystem_traces_by_default():
    defaults = SubsystemTracesConfig().model_dump()
    assert defaults.pop("catalog_database") is False
    assert all(defaults.values()), defaults


def test_the_switches_are_part_of_the_otel_config():
    assert OtelConfig().subsystem_traces.catalog_database is False


def test_an_unknown_subsystem_is_rejected_rather_than_stored():
    with pytest.raises(ValueError):
        SubsystemTracesConfig(**{"asyncpg": True})


def test_shipped_config_states_the_switches():
    cfg = yaml.safe_load((_REPO / "config" / "provisa.yaml").read_text())
    shipped = cfg["observability"]["subsystem_traces"]
    assert shipped["catalog_database"] is False
    assert set(shipped) == set(SubsystemTracesConfig.model_fields)


def test_each_instrumentor_is_gated_on_its_subsystem():
    # The gate has to be in setup_otel itself: an instrumentor patches the driver globally, so a
    # switch consulted anywhere later cannot un-instrument it.
    src = (_REPO / "provisa" / "api" / "otel_setup.py").read_text()
    tree = ast.parse(src)
    setup = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "setup_otel"
    )
    gated = {
        n.test.attr
        for n in ast.walk(setup)
        if isinstance(n, ast.If)
        and isinstance(n.test, ast.Attribute)
        and isinstance(n.test.value, ast.Name)
        and n.test.value.id == "_subsystems"
    }
    assert gated == set(SubsystemTracesConfig.model_fields)
