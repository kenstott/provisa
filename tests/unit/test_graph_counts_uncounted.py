# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/graph-counts (REQ-392): a label the engine could not count is said, with its reason, and
a total is a total only when every label in it was counted -- never a sum that left some out."""

from __future__ import annotations

import logging

from fastapi.responses import JSONResponse

from provisa.api.rest.cypher_router import _total, _uncounted


def test_an_uncounted_label_says_why_and_is_logged(caplog):
    refused = JSONResponse(
        status_code=400, content={"error": "Catalog petstore is not attached", "sql": "SELECT"}
    )
    with caplog.at_level(logging.WARNING, logger="provisa.api.rest.cypher_router"):
        entry = _uncounted("MATCH (n:PetStore:INVENTORY) RETURN count(n) AS cnt", refused)
    assert entry == {"count": None, "error": "Catalog petstore is not attached"}
    assert "PetStore:INVENTORY" in caplog.text
    assert "Catalog petstore is not attached" in caplog.text


def test_the_total_is_null_when_any_label_is_uncounted():
    assert _total({"A": {"count": 3}, "B": {"count": 4}}) == 7
    assert _total({"A": {"count": 3}, "B": {"count": None, "error": "not attached"}}) is None
    assert _total({}) == 0
