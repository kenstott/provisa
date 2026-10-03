# Copyright (c) 2026 Kenneth Stott
# Canary: 7fd4e835-c92a-42e4-8323-90690e3f6875
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A description written to Snowflake is data in the statement that writes it.

Snowflake reads a backslash inside a string literal as an escape. A table or column description
holding a backslash and a quote stays one comment: it never ends its literal and adds a
statement. Checked by parsing each statement in Snowflake's grammar."""

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

HOSTILE = ["orders\\'; DROP TABLE S.T; --", "it's", "back\\slash", "x'; DROP TABLE S.T; --"]


def _one_statement_holding(sql: str, value: str) -> None:
    statements = [s for s in sqlglot.parse(sql.rstrip(";"), read="snowflake") if s is not None]
    assert len(statements) == 1, statements
    assert value in [lit.this for lit in statements[0].find_all(exp.Literal)]


@pytest.mark.parametrize("value", HOSTILE)
def test_a_horizon_comment_is_one_statement(value):
    from provisa.api.metadata_export.snowflake_horizon import _set_comment

    _one_statement_holding(_set_comment("TABLE", ("DB", "S", "T"), None, value), value)
    _one_statement_holding(_set_comment("TABLE", ("DB", "S", "T"), "c", value), value)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_store_comment_is_one_statement(value):
    from provisa.federation.landed_keys import KeyTarget
    from provisa.federation.snowflake_store import comment_statements

    target = KeyTarget(
        replica=("DB", "S", "T"),
        view=None,
        primary_key=(),
        description=value,
        column_descriptions={"c": value},
    )
    statements = comment_statements({"t": target}, {})
    assert len(statements) == 2
    for sql in statements:
        _one_statement_holding(sql, value)
