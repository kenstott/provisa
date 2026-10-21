# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A shipped config's SQL-defined views are written in the model's terms (REQ-1922).

A view's ``view_sql`` is its ``semantic_sql``, and every build governs it through the one
pipeline, which refuses a physical source-catalog reference
(``"pet_store_sqlite"."pet_store"."pets"``) as internal. A definition naming one never builds:
the startup probe of dim_pet / fact_pet_inquiries failed so on every boot of the install config.
Each table a view reads is named ``"domain"."table"``, two parts, never three.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
import yaml
from sqlglot import exp

_ROOT = Path(__file__).resolve().parents[2]
_CONFIGS = sorted(
    [*(_ROOT / "config").glob("*.yaml"), *(_ROOT / "demo" / "named").glob("*/config.yaml")]
)


def _views(path: Path) -> list[tuple[str, str]]:
    doc = yaml.safe_load(path.read_text())
    if not isinstance(doc, dict):
        return []
    return [
        (t["table"], t["view_sql"])
        for t in doc.get("tables") or []
        if isinstance(t, dict) and t.get("view_sql")
    ]


_CASES = [
    pytest.param(sql, id=f"{path.relative_to(_ROOT)}:{table}")
    for path in _CONFIGS
    for table, sql in _views(path)
]


def test_the_install_config_defines_views() -> None:
    assert any("provisa-install.yaml" in str(c.id) for c in _CASES)


@pytest.mark.parametrize("view_sql", _CASES)
def test_a_view_names_each_table_by_its_domain_and_name(view_sql: str) -> None:
    three_part = [
        t.sql(dialect="postgres")
        for t in sqlglot.parse_one(view_sql, read="postgres").find_all(exp.Table)
        if t.catalog
    ]
    assert three_part == []
