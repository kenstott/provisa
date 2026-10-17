# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No shipped configuration relates a glossary term to itself.

The config load adds each declared term's edges (config_loader), and the glossary refuses an edge
from a term to itself (repositories.glossary.add_edge: "a term cannot relate to itself"); a refused
edge fails the server's start. A term and an edge's target are the same term when their names are,
trimmed and lowercased, as the load looks them up. The opioids demo shipped one."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CONFIGS = sorted(
    [
        *(_REPO / "config").glob("provisa*.yaml"),
        *(_REPO / "demo" / "named").glob("*/config.yaml"),
    ]
)


def _name(text: str) -> str:
    # As config_loader looks a term up: every term name in the catalog is lowercase.
    return text.strip().lower()


def test_there_are_shipped_configs_with_glossary_terms():
    assert any((yaml.safe_load(p.read_text()) or {}).get("glossary_terms") for p in _CONFIGS)


@pytest.mark.parametrize("path", _CONFIGS, ids=lambda p: str(p.relative_to(_REPO)))
def test_no_glossary_term_relates_to_itself(path):
    config = yaml.safe_load(path.read_text()) or {}
    selves = [
        term["name"]
        for term in config.get("glossary_terms") or []
        for edge in term.get("edges") or []
        if _name(edge["to"]) == _name(term["name"])
    ]
    assert selves == [], f"{path.name}: terms with an edge to themselves: {selves}"
