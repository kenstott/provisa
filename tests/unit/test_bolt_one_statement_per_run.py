# Copyright (c) 2026 Kenneth Stott
# Canary: 21ff7780-ee48-4ec1-91d7-7e7d9f1fdd73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt RUN holds one statement, as Neo4j's does: "stmt1; stmt2" is refused whole before any
branch runs it -- a catalog query, a command call or a read -- never answered with only part."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.bolt.session import _execute_cypher

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


@pytest.mark.parametrize(
    "cypher",
    [
        "SHOW DATABASES; SHOW DATABASES",
        "CALL random_python_set(1, 2); CALL random_python_set(3, 4)",
        "MATCH (a:A) RETURN a.x; MATCH (b:B) RETURN b.y",
    ],
)
async def test_two_statements_in_one_run_are_refused_before_any_runs(cypher):
    with (
        patch("provisa.api.app.state", SimpleNamespace(contexts={"admin": object()})),
        patch("provisa.bolt.session._system_query", side_effect=AssertionError("ran")),
        pytest.raises(ValueError, match="exactly one statement per query but got: 2"),
    ):
        await _execute_cypher(cypher, {}, "admin")
