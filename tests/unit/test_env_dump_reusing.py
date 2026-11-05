# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e7c93-2d48-4f06-a9e3-7c6d0f2b8a15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882: a write-through renders only the objects a change touched.

Every model change rendered every file of the model to YAML again -- most of a write-through's time,
and a write-through follows each change. The last projection's text is now kept per environment and
reused for every entity equal to the one it was rendered from. What is committed is unchanged: at
every step the files, and so the git tree, are byte-identical to a full render.
"""

# Requirements: REQ-1882

from __future__ import annotations

import copy
import datetime

from dulwich.repo import Repo

from provisa.core import env_files
from provisa.core.env_files import dump, dump_reusing, fingerprint
from provisa.core.env_repo import _build_tree


def _model() -> dict[str, dict]:
    return {
        "sales/domain.yaml": {"id": "sales", "description": "Sales — EU and US"},
        "sales/tables/orders.yaml": {
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["*"], "masked": False},
                {"column_name": "amount", "visible_to": ["steward"], "precision": 2},
            ],
            "cache_ttl": 300,
            "weight": 1.5,
        },
        "sales/tables/customers.yaml": {
            "table_name": "customers",
            "tags": ["pii", "gold"],
            "since": datetime.date(2026, 1, 2),
        },
        "sources/pg_main.yaml": {"id": "pg_main", "type": "postgresql", "port": 5432},
    }


def _edits() -> list:
    """A run of model changes, each as one write-through sees it."""

    def retitle(m):
        m["sales/domain.yaml"]["description"] = "Sales"

    def mask(m):
        m["sales/tables/orders.yaml"]["columns"][1]["masked"] = True

    def add(m):
        m["sales/tables/refunds.yaml"] = {"table_name": "refunds", "cache_ttl": 0}

    def drop(m):
        del m["sales/tables/customers.yaml"]

    def retype(m):
        # 300 -> 300.0 -> True: equal under ==, different text.
        m["sales/tables/orders.yaml"]["cache_ttl"] = 300.0

    def retype_again(m):
        m["sales/tables/orders.yaml"]["cache_ttl"] = True

    def reorder(m):
        m["sales/tables/orders.yaml"] = dict(reversed(list(m["sales/tables/orders.yaml"].items())))

    return [retitle, mask, add, drop, retype, retype_again, reorder]


def test_every_step_commits_exactly_what_a_full_render_commits(tmp_path) -> None:
    repo = Repo.init_bare(str(tmp_path), mkdir=False)
    model, last = _model(), {}
    for edit in [lambda _m: None, *_edits()]:
        edit(model)
        files, last = dump_reusing(copy.deepcopy(model), last)
        full = dump(copy.deepcopy(model))
        assert files == full
        assert list(files) == list(full)
        assert _build_tree(repo, files) == _build_tree(repo, full)


def test_only_the_entities_a_change_touched_are_rendered(monkeypatch) -> None:
    rendered: list = []
    real = env_files._emit

    def _counting(entity):
        rendered.append(entity.get("id") or entity.get("table_name"))
        return real(entity)

    monkeypatch.setattr(env_files, "_emit", _counting)
    model = _model()
    _files, last = dump_reusing(model, {})
    assert len(rendered) == 4

    rendered.clear()
    model["sales/tables/orders.yaml"]["columns"][1]["masked"] = True
    model["sales/tables/refunds.yaml"] = {"table_name": "refunds"}
    _files, last = dump_reusing(model, last)
    assert sorted(rendered) == ["orders", "refunds"]

    rendered.clear()
    _files, last = dump_reusing(model, last)
    assert rendered == []


def test_values_equal_under_equality_but_of_other_types_have_other_fingerprints() -> None:
    marks = {fingerprint(v) for v in (1, 1.0, True, "1", [1], (1,), {"a": 1})}
    assert len(marks) == 7
    assert fingerprint({"b": 1, "a": [2]}) == fingerprint({"a": [2], "b": 1})
