# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9e6a14-8d57-4b30-a1f2-6e4b9d0c7385
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Selection and ordering of scripts/purview_legacy_cleanup.py against a fake account (REQ-254)."""

# Requirements: REQ-254

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest

from provisa.api.metadata_export.atlas import (
    PROVISA_COLUMN_TYPE,
    PROVISA_DATA_PRODUCT_TYPE,
    PROVISA_SOURCE_TYPE,
    PROVISA_TABLE_TYPE,
)

_SPEC = importlib.util.spec_from_file_location(
    "purview_legacy_cleanup",
    Path(__file__).resolve().parents[2] / "scripts" / "purview_legacy_cleanup.py",
)
assert _SPEC and _SPEC.loader
cleanup = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = cleanup
_SPEC.loader.exec_module(cleanup)


class FakeAccount:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.fail: set[tuple] = set()
        self.items = {
            "dataProducts": {
                "p1": {"id": "p1", "name": "Customer 360 provisa_e2e_ab", "status": "Published"},
                "p2": {"id": "p2", "name": "Other", "status": "Draft"},
            },
            "terms": {
                "t1": {"id": "t1", "name": "customer", "status": "Published"},
                "t2": {"id": "t2", "name": "order", "status": "Published"},
                "t3": {"id": "t3", "name": "keep me", "status": "Published"},
            },
        }
        self.search = {
            PROVISA_SOURCE_TYPE: [
                {"id": "g-src", "qualifiedName": "mdx-wh@provisa"},
                {"id": "g-other-src", "qualifiedName": "mdx-wh-abcd1234@provisa"},
            ],
            PROVISA_TABLE_TYPE: [
                {"id": "g-t", "qualifiedName": "mdx-wh.mdx_e2e.public.orders@provisa"},
                {"id": "g-nt", "qualifiedName": "other.s.t@provisa"},
            ],
            PROVISA_COLUMN_TYPE: [
                {"id": "g-c", "qualifiedName": "mdx-wh.mdx_e2e.public.orders.id@provisa"}
            ],
            PROVISA_DATA_PRODUCT_TYPE: [{"id": "g-dp", "qualifiedName": "data-product.x@provisa"}],
        }

    def _rec(self, *call):
        self.calls.append(call)
        if call in self.fail:
            raise httpx.HTTPError(f"boom {call}")

    def uc_list(self, kind):
        return list(self.items[kind].values())

    def uc_get(self, kind, item_id):
        self._rec("get", kind, item_id)
        return dict(self.items[kind][item_id])

    def uc_relationships(self, kind, item_id, category):
        return [{"entityId": f"{item_id}-{category}"}]

    def uc_delete_relationship(self, kind, item_id, category, rel):
        self._rec("unrelate", kind, item_id, category)

    def uc_put(self, kind, item_id, body):
        self._rec("draft", kind, item_id, body["status"])

    def uc_delete(self, kind, item_id):
        self._rec("delete", kind, item_id)

    def assets_over(self, guids):
        return [{"id": "a1", "source": {"assetId": guids[0]}}]

    def asset_delete(self, asset_id):
        self._rec("delete-asset", asset_id)

    def dm_search(self, type_name):
        return self.search[type_name]

    def dm_entities(self, guids):
        return [
            {"guid": g, "typeName": "t", "attributes": {"qualifiedName": f"qn-{g}"}} for g in guids
        ]

    def dm_delete_entities(self, guids):
        self._rec("bulk-delete", tuple(guids))

    def dm_glossaries(self):
        return [
            {"guid": "gl", "qualifiedName": "provisa_glossary.mdx_purview_e2e@provisa"},
            {"guid": "gl2", "qualifiedName": "provisa_glossary.mdx_purview_e2e_ab@provisa"},
        ]

    def dm_glossary_terms(self, glossary_guid):
        return [{"guid": "gt1", "name": "customer"}]

    def dm_delete_glossary_term(self, guid):
        self._rec("delete-glossary-term", guid)

    def dm_delete_glossary(self, guid):
        self._rec("delete-glossary", guid)


def test_selection_takes_only_legacy_objects():
    _, ids = cleanup.find_legacy(FakeAccount())
    assert ids["products"] == ["p1", "p2"]
    assert ids["terms"] == ["t1", "t2"]
    assert sorted(ids["entities"]) == ["g-c", "g-dp", "g-src", "g-t"]
    assert ids["data_assets"] == ["a1"]
    assert ids["glossaries"] == ["gl"]
    assert ids["glossary_terms"] == ["gt1"]


def test_never_selects_domain_or_typedefs():
    rows, _ = cleanup.find_legacy(FakeAccount())
    text = cleanup.render_table(rows)
    assert "provisa-e2e-sales" not in text
    assert not cleanup.is_legacy_data_map_name("typedef", "mdx-wh@provisa")
    assert not cleanup.is_legacy_data_map_name(PROVISA_SOURCE_TYPE, "mdx-wh@other")


def test_table_has_a_row_per_object():
    rows, _ = cleanup.find_legacy(FakeAccount())
    table = cleanup.render_table(rows)
    assert table.startswith("| kind | name / qualifiedName | id / guid | status | created |")
    assert "| term | customer | t1 | Published |" in table


def _ids():
    return {
        "products": ["p1"],
        "terms": ["t1"],
        "data_assets": ["a1"],
        "entities": ["g-src"],
        "glossary_terms": ["gt1"],
        "glossaries": ["gl"],
    }


def test_delete_runs_in_dependency_order_and_only_given_ids():
    fake = FakeAccount()
    assert cleanup.delete_ids(fake, _ids()) == []
    ops = [c for c in fake.calls if c[0] != "get"]
    assert ops == [
        ("unrelate", "dataProducts", "p1", "DATAASSET"),
        ("unrelate", "terms", "t1", "DATAASSET"),
        ("unrelate", "terms", "t1", "TERM"),
        ("draft", "dataProducts", "p1", "DRAFT"),
        ("draft", "terms", "t1", "DRAFT"),
        ("delete", "dataProducts", "p1"),
        ("delete", "terms", "t1"),
        ("delete-asset", "a1"),
        ("bulk-delete", ("g-src",)),
        ("delete-glossary-term", "gt1"),
        ("delete-glossary", "gl"),
    ]


def test_draft_item_is_not_put_again():
    fake = FakeAccount()
    ids = {k: [] for k in cleanup.ID_KINDS} | {"products": ["p2"]}
    assert cleanup.delete_ids(fake, ids) == []
    assert not [c for c in fake.calls if c[0] == "draft"]


def test_every_delete_attempted_and_failures_reported():
    fake = FakeAccount()
    fake.fail = {("delete", "dataProducts", "p1"), ("delete-asset", "a1")}
    failures = cleanup.delete_ids(fake, _ids())
    assert len(failures) == 2
    assert any("dataProducts p1: delete" in f for f in failures)
    assert any("data asset a1" in f for f in failures)
    assert ("delete-glossary", "gl") in fake.calls


def test_unreadable_item_is_a_failure_not_a_skip():
    fake = FakeAccount()
    fake.fail = {("get", "terms", "t1")}
    failures = cleanup.delete_ids(fake, _ids())
    assert any("terms t1: read" in f for f in failures)


def test_parse_ids_rejects_malformed():
    with pytest.raises(ValueError):
        cleanup.parse_ids(json.dumps({"products": ["p"]}))
    with pytest.raises(ValueError):
        cleanup.parse_ids(json.dumps({**_ids(), "extra": []}))
    with pytest.raises(ValueError):
        cleanup.parse_ids(json.dumps({**_ids(), "terms": ["ok", ""]}))
    assert cleanup.parse_ids(json.dumps(_ids())) == _ids()
