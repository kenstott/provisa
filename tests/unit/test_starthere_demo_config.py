# Copyright (c) 2026 Kenneth Stott
# Canary: 5c697eb7-e7c1-4322-874d-c7fb6bf0e1b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Start here demo is the standard demo's own objects (REQ-2006).

demo/named/starthere is the standard demo (config/provisa-install.yaml) on its two SQLite files:
its files/ links the standard demo's files, and every object its config holds is the standard
config's object of the same id, each one over those sources held, so a change to the standard demo
that is not made here fails."""

# Requirements: REQ-2006

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.core.config_loader import read_config_with_includes

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_HOME = _REPO / "demo" / "named" / "starthere"
_STANDARD = read_config_with_includes(_REPO / "config" / "provisa-install.yaml")
_DEMO = yaml.safe_load((_HOME / "config.yaml").read_text())
# The standard demo's sources that need servers of its own run: the Start here demo has none.
_LEFT_OUT = {"petstore-api", "graphql-demo"}
_SOURCES = {s["id"] for s in _STANDARD["sources"]} - _LEFT_OUT


def _by(entries: list[dict], key: str) -> dict:
    return {entry[key]: entry for entry in entries}


def test_its_sources_are_the_standard_demo_s_on_the_same_files():
    standard = _by(_STANDARD["sources"], "id")
    demo = _by(_DEMO["sources"], "id")
    assert set(demo) == _SOURCES
    for sid, source in demo.items():
        if source["type"] != "sqlite":
            assert source == standard[sid]
            continue
        name = standard[sid]["path"].rsplit("/", 1)[1]
        assert source == {**standard[sid], "path": f"${{env:PROVISA_NAMED_DEMO_DIR}}/files/{name}"}
        assert (_HOME / "files" / name).resolve() == (_REPO / "demo" / "files" / name).resolve()


def test_it_holds_every_standard_table_and_relationship_over_those_sources():
    tables = [t for t in _STANDARD["tables"] if t["source_id"] not in _LEFT_OUT]
    assert _DEMO["tables"] == tables
    names = {t.get("alias") or t["table"] for t in tables} | {t["table"] for t in tables}
    relationships = [
        r
        for r in _STANDARD["relationships"]
        if r["source_table_id"] in names and r["target_table_id"] in names
    ]
    assert _DEMO["relationships"] == relationships


@pytest.mark.parametrize(
    ("section", "key"),
    [
        ("domains", "id"),
        ("data_products", "id"),
        ("glossary_terms", "name"),
        ("metrics", "name"),
    ],
)
def test_each_other_object_is_the_standard_demo_s(section, key):
    standard = _by(_STANDARD[section], key)
    for name, entry in _by(_DEMO[section], key).items():
        if section == "domains":
            assert entry["id"] == standard[name]["id"]
            continue
        assert entry == standard[name], name


def test_its_tags_are_the_standard_demo_s_over_those_sources():
    assert _DEMO["tag_assignments"] == [
        t for t in _STANDARD["tag_assignments"] if t["table_ref"].split(".")[0] in _SOURCES
    ]


def test_its_analyst_role_is_the_standard_demo_s_on_its_one_domain():
    # REQ-2155: the standard demo's own analyst, a child of the standard analyst.
    (standard,) = [r for r in _STANDARD["roles"] if r["id"] == "pet_analyst"]
    (role,) = _DEMO["roles"]
    assert role["id"] == standard["id"]
    assert role["parents"] == standard["parents"] == ["analyst"]
    assert role["capabilities"] == standard["capabilities"]
    assert role["domain_access"] == [d["id"] for d in _DEMO["domains"]] == ["pet-store"]
