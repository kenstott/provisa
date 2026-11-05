# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a1c52-8d0e-4b7a-9c41-5e2b7d9a0f16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every write refusal the admission raises by name is answered over HTTP as a 400 with its code
and the table, not escaped as a 500 (REQ-1981: a Hive table's MERGE is refused by name)."""

# Requirements: REQ-1981

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from provisa.api.app import create_app
from provisa.compiler.write_admission import (
    ReturningNotSupported,
    WriteFormNotSupported,
    WriteNotSupported,
)


@pytest.mark.parametrize(
    ("exc", "code", "params"),
    [
        (WriteNotSupported("pets", "update"), "data.write_not_supported", {"operation": "UPDATE"}),
        (
            WriteFormNotSupported("pets", "merge"),
            "data.write_form_not_supported",
            {"form": "merge"},
        ),
        (ReturningNotSupported("pets"), "data.returning_not_supported", {}),
    ],
)
def test_a_write_refusal_is_answered_400_naming_the_table(exc, code, params):
    app = create_app()

    @app.get("/_refusal_probe")
    async def _probe():  # pyright: ignore[reportUnusedFunction]
        raise exc

    resp = TestClient(app, raise_server_exceptions=False).get("/_refusal_probe")
    body = resp.json()
    assert resp.status_code == 400, body
    assert body["code"] == code, body
    assert "pets" in body["detail"], body
    assert body["params"] == {"table": "pets", **params}, body
