# Copyright (c) 2026 Kenneth Stott
# Canary: 14e62fe0-28b1-48a0-b94d-c2f7fffe79e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""set_config(name, value, is_local) answers the value it set, as PostgreSQL does."""

from __future__ import annotations

import pytest

from provisa.pgwire.catalog import answer

from .test_catalog import _make_state


@pytest.mark.parametrize(
    ("sql", "value"),
    [
        (
            "SELECT pg_catalog.set_config('application_name', 'conformance', false) AS v",
            "conformance",
        ),
        ("SELECT set_config('search_path', 'a, b', true)", "a, b"),
        ("SELECT set_config('x.y', 'it''s', false)", "it's"),
    ],
)
def test_set_config_answers_the_value_it_set(sql, value):
    result = answer(sql, "testrole", _make_state({}, {}))
    assert result.rows == [(value,)]


def test_alongside_current_setting_it_answers_its_value():
    result = answer(
        "SELECT current_setting('server_version') AS a, set_config('application_name', 'x', false) AS b",
        "testrole",
        _make_state({}, {}),
    )
    assert result.rows[0][1] == "x"
