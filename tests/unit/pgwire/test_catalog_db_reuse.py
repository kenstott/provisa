# Copyright (c) 2026 Kenneth Stott
# Canary: add19692-b136-4f7b-aa60-ada6da3dc8a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""There is no catalog to build (REQ-2190): every statement reads the model as it stands, each role
reads its own, and a table registered is shown by the next read with nothing rebuilt."""

# Requirements: REQ-2190

from __future__ import annotations

import contextvars
import threading
from unittest.mock import MagicMock

from provisa.pgwire.catalog import answer

from .test_catalog import _ColMeta, _TableMeta


def _state(*tables: _TableMeta):
    ctx = MagicMock()
    ctx.tables = {t.table_name: t for t in tables}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {"analyst": ctx, "auditor": MagicMock(tables={})}
    state.schema_build_cache = {
        "column_types": {t.table_id: [_ColMeta("id", "integer", False)] for t in tables}
    }
    state.roles = {}
    return state


_LISTED = (
    "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
    "ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY 1"
)
_DOGS = _TableMeta(1, "dogs", "provisa", "public", "dogs")
_CATS = _TableMeta(2, "cats", "provisa", "public", "cats")


def test_statements_of_a_role_read_the_same_model():
    state = _state(_DOGS)
    for _ in range(3):
        assert answer(_LISTED, "analyst", state).rows == [("dogs",)]


def test_a_role_reads_its_own_model():
    state = _state(_DOGS)
    assert answer(_LISTED, "analyst", state).rows == [("dogs",)]
    assert answer("SELECT current_user", "auditor", state).rows == [("auditor",)]


def test_a_table_registered_is_shown_by_the_next_read():
    assert answer(_LISTED, "analyst", _state(_DOGS)).rows == [("dogs",)]
    assert answer(_LISTED, "analyst", _state(_DOGS, _CATS)).rows == [("cats",), ("dogs",)]


def test_concurrent_statements_are_each_answered():
    state = _state(_DOGS)
    assert answer(_LISTED, "analyst", state).rows == [("dogs",)]
    answers: list[list] = []

    def read() -> None:
        answers.append(answer(_LISTED, "analyst", state).rows)

    threads = [
        threading.Thread(target=contextvars.copy_context().run, args=(read,)) for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert answers == [[("dogs",)]] * 4
