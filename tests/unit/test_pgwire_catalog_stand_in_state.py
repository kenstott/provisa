# Copyright (c) 2026 Kenneth Stott
# Canary: 99177985-6105-4514-9701-a35dcbe42d6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pgwire integration suite's stand-in AppState serves catalog reads (REQ-532, REQ-2190).

A client's catalog statement is governed as any read of the meta domain
(``provisa.pgwire.catalog._read_meta`` -> ``govern_catalog_plan``), and every governed request
resolves its debug-trace scope against the control plane (``trace_scope.request_is_debug``,
REQ-1910). A process with no control plane says so with ``state.admin_db is None``; a bare
``MagicMock`` attribute there is read as a control plane, whose ``acquire()`` yields an AsyncMock
connection, so ``list_windows`` iterated a coroutine and every catalog read in
``tests/integration/test_pgwire_integration.py::TestPgwireCatalogIntercept`` failed with
"'coroutine' object is not iterable". This drives the same statements through ``catalog.answer``
on that suite's own stand-in, with no server and no containers.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from provisa.pgwire import catalog
from tests.integration.test_pgwire_integration import _make_mock_state


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT nspname FROM pg_catalog.pg_namespace",
        "SELECT table_name FROM information_schema.tables LIMIT 5",
        "SELECT schema_name FROM information_schema.schemata",
        "SELECT typname FROM pg_catalog.pg_type LIMIT 10",
        "SELECT name, setting FROM pg_catalog.pg_settings LIMIT 5",
    ],
)
def test_the_suites_stand_in_state_answers_a_catalog_read(sql):
    state = _make_mock_state("admin", "simple")
    with patch("provisa.api.app.state", state):
        result = catalog.answer(sql, "admin", state, run=asyncio.run)
    assert isinstance(result.rows, list)


def test_the_suites_stand_in_state_has_no_control_plane():
    """The request path reads operator settings only from a control plane the state has."""
    assert _make_mock_state("admin", "simple").admin_db is None
