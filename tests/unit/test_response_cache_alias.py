# Copyright (c) 2026 Kenneth Stott
# Canary: 704bf045-1b50-4b35-bd38-40d355aa8d87
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two ``@cached`` reads of one field under different aliases share one correct entry (REQ-544).

``query @cached { a: s__orders { id } }`` then ``query @cached { b: s__orders { id } }``: both
compile to the same SQL, so both have the same key; the entry held the whole response under
the first read's alias, and the second read looked its own alias up in it and answered 500
(``KeyError: 'b'``). The key decides the rows — every alias below the root is in the SQL — so the
entry holds the field's rows alias-free, under the field, and each read places them under its
own alias."""

# Requirements: REQ-544, REQ-1896

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.data.endpoint import cached_field_rows
from provisa.api.data.endpoint_executors import _store_response_cache
from provisa.cache.middleware import check_cache
from tests.unit.test_response_cache_shared import FakeCacheStore

ROWS = [{"id": 1}, {"id": 2}]


def _compiled(alias: str):
    return SimpleNamespace(root_field=alias, canonical_field="s__orders", sources={"pg"})


def _state(store):
    return SimpleNamespace(
        response_cache_store=store,
        response_cache_default_ttl=300,
        source_cache={},
        table_cache={},
    )


async def test_a_read_under_another_alias_is_served_the_same_rows():
    store = FakeCacheStore()
    state = _state(store)
    ctx = SimpleNamespace(tables={"s__orders": SimpleNamespace(table_id=7, source_id="pg")})
    first = _compiled("a")
    await _store_response_cache(
        state, "k", {"data": {"a": ROWS}}, "a", ctx, first, None, True, org_id="acme"
    )

    cached = await check_cache(store, "k", "acme")
    assert cached is not None
    assert cached_field_rows(cached, _compiled("b")) == ROWS
    assert cached_field_rows(cached, first) == ROWS
