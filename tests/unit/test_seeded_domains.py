# Copyright (c) 2026 Kenneth Stott
# Canary: 90bb7796-2fbf-4851-8e82-9f8236ae1b3a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The domains every org's control plane is seeded with are the system's own: no domain, meta and
ops. A demo's domains (the pet store's shelter) are its config's. A seeded shelter appeared in
every org and in every other demo, where the browser's domain filter counted it among the
domains to show or hide."""

from __future__ import annotations

import re
from pathlib import Path

from provisa.core.db import SEEDED_DOMAIN_IDS

SCHEMA_SQL = Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"


def test_only_the_systems_own_domains_are_seeded() -> None:
    assert SEEDED_DOMAIN_IDS == {"", "meta", "ops", "ontology"}


def test_schema_sql_seeds_the_same_domains() -> None:
    seeded = set(
        re.findall(r"INSERT INTO domains \([^)]*\) VALUES \('([^']*)'", SCHEMA_SQL.read_text())
    )
    assert seeded == SEEDED_DOMAIN_IDS


def test_the_standard_demo_declares_shelter_itself() -> None:
    import yaml

    root = Path(__file__).resolve().parents[2]
    for config in ("config/provisa-install.yaml", "config/provisa.yaml"):
        declared = {d["id"] for d in yaml.safe_load((root / config).read_text())["domains"]}
        assert "shelter" in declared, config
