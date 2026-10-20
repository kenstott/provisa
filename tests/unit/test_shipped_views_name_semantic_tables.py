# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A shipped config's views name the semantic tables the catalog shows, never a source's physical
catalog: the one pipeline refuses a physical source name, so a view naming one is never built
(its build and its event-loop probe go through that pipeline)."""

# Requirements: REQ-1921

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
import yaml
from sqlglot import exp

from provisa.compiler.naming import source_to_catalog

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]
CONFIGS = [
    REPO / "config/provisa-install.yaml",
    *sorted((REPO / "demo/named").glob("*/config.yaml")),
]


@pytest.mark.parametrize("path", CONFIGS, ids=lambda p: str(p.relative_to(REPO)))
def test_no_shipped_view_names_a_physical_source(path):
    config = yaml.safe_load(path.read_text())
    catalogs = {source_to_catalog(s["id"]) for s in config.get("sources") or []}
    offending = []
    for table in config.get("tables") or []:
        view_sql = table.get("view_sql")
        if not view_sql:
            continue
        for ref in sqlglot.parse_one(view_sql, read="postgres").find_all(exp.Table):
            if ref.catalog and ref.catalog in catalogs:
                offending.append(f"{table.get('table')}: {ref.sql(dialect='postgres')}")
    assert not offending, offending
