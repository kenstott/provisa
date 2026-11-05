# Copyright (c) 2026 Kenneth Stott
# Canary: 29edfd49-f048-42fb-b3f9-ea3dc909a41b
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


# REQ-1387 (A CONCEPT'S PROPERTIES ARE ITS PARTS): the shipped seed links a concept to the property
# terms that describe it with PART_OF, property to concept, so the concept is grounded through the
# columns they hold. RELATED_TO conducts no grounding and would leave the concept naming no data.
_SEEDS = [_REPO / "config" / "provisa.yaml", _REPO / "config" / "provisa-install.yaml"]
_CONCEPT_PARTS = {
    "shift coverage": {"shift start", "shift end", "schedule location"},
    "referral": {"clinic", "specialty"},
}


def _edges(path: Path) -> set[tuple[str, str, str]]:
    config = yaml.safe_load(path.read_text()) or {}
    return {
        (_name(term["name"]), edge["rel_type"], _name(edge["to"]))
        for term in config.get("glossary_terms") or []
        for edge in term.get("edges") or []
    }


@pytest.mark.parametrize("path", _SEEDS, ids=lambda p: str(p.relative_to(_REPO)))
def test_a_concepts_properties_are_part_of_it(path):
    edges = _edges(path)
    for concept, parts in _CONCEPT_PARTS.items():
        for part in parts:
            assert (part, "PART_OF", concept) in edges, f"{path.name}: {part} PART_OF {concept}"
        loose = {e for e in edges if e[1] == "RELATED_TO" and concept in (e[0], e[2])}
        assert loose == set(), f"{path.name}: {concept} still RELATED_TO its parts: {loose}"
