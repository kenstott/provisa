# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e2a7f-1b3d-4f86-a0e5-7d2c8b6f4a19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No named demo declares two relationships of one type from one table. The model store keeps a
type once per source table (relationships_source_alias_unique), so the second was not registered:
retail's ASKS_ABOUT to us_customers was dropped at load, and its subject_access view's US join was
refused as one no relationship approves."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest
import yaml

_NAMED = Path(__file__).resolve().parents[2] / "demo" / "named"
CONFIGS = sorted(_NAMED.glob("*/config.yaml"))


@pytest.mark.parametrize("path", CONFIGS, ids=[p.parent.name for p in CONFIGS])
def test_each_relationship_type_is_declared_once_per_source_table(path):
    config = yaml.safe_load(path.read_text())
    declared = Counter(
        (rel["source_table_id"], rel["alias"])
        for rel in config.get("relationships", [])
        if rel.get("alias")
    )
    assert [pair for pair, n in declared.items() if n > 1] == [], path.parent.name


# The sources whose tables are registered after the configuration's own, by their registration
# (graphql_remote_router, the gRPC remote's): a relationship may name one of theirs.
_REGISTERED_LATER = {"graphql_remote", "grpc_remote"}


@pytest.mark.parametrize("path", CONFIGS, ids=[p.parent.name for p in CONFIGS])
def test_each_relationship_names_a_table_by_the_name_it_is_registered_under(path):
    """A relationship reaches a table by its registered name, its alias else its table name; a
    view is registered as ``view_<id>`` (config_loader.views_as_tables). A name nothing registers
    fails the load by name (REQ-388, REQ-1919): agriculture's answer view was named by its id."""
    from provisa.core.config_loader import views_as_tables

    config = views_as_tables(yaml.safe_load(path.read_text()))
    if any(s.get("type") in _REGISTERED_LATER for s in config.get("sources", [])):
        pytest.skip("a remote source registers tables of its own after the configuration's")
    names = {t.get("alias") or t["table"] for t in config.get("tables", [])}
    unknown = [
        (rel["id"], rel[end])
        for rel in config.get("relationships", [])
        for end in ("source_table_id", "target_table_id")
        if rel[end] not in names
    ]
    assert unknown == [], path.parent.name
