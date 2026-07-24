# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1194/REQ-1195: buffered transports surface a materialize handle via their native side-channel.

GraphQL already returns the handle in a top-level ``redirect`` key. These tests pin the analogous
channels for the other two buffered transports — JSON:API ``meta`` and Bolt's trailing PULL SUCCESS
metadata — plus the shared request→directive translator ``delivery_from_request``.
"""

from __future__ import annotations

import io

import pytest

from provisa.executor.redirect import Delivery, delivery_from_request


class TestDeliveryFromRequest:
    """The shared translator: a caller redirect request → a _Plan.materialize directive (or None)."""

    def test_no_force_returns_none(self):
        # No redirect asked for → None, the opt-out streaming transports also use.
        assert (
            delivery_from_request(
                force_redirect=False, redirect_format="parquet", threshold=None, role="r"
            )
            is None
        )

    def test_force_builds_delivery_with_role_and_format(self):
        d = delivery_from_request(
            force_redirect=True, redirect_format="orc", threshold=None, role="analyst"
        )
        assert isinstance(d, Delivery)
        assert d.output_format == "orc"
        assert d.role == "analyst"

    def test_threshold_override_enables_config(self):
        d = delivery_from_request(
            force_redirect=True, redirect_format="parquet", threshold=50, role=None
        )
        assert d is not None
        assert d.config.enabled is True
        assert d.config.threshold == 50

    def test_default_format_when_unspecified(self, monkeypatch):
        monkeypatch.delenv("PROVISA_REDIRECT_FORMAT", raising=False)
        d = delivery_from_request(
            force_redirect=True, redirect_format=None, threshold=None, role=None
        )
        assert d is not None
        assert d.output_format == "parquet"


class _FakeWriter:
    def __init__(self) -> None:
        self._buf = io.BytesIO()

    def write(self, data: bytes) -> None:
        self._buf.write(data)

    async def drain(self) -> None:
        pass

    def get_extra_info(self, key: str, default: object = None) -> object:
        _ = key
        return default

    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


class TestBoltPullSurfacesRedirect:
    """Bolt's side-channel: the trailing PULL SUCCESS metadata carries the handle, no records stream."""

    def _session(self):
        from provisa.bolt.session import BoltSession, State

        session = BoltSession(_FakeWriter(), (5, 4))  # type: ignore[arg-type]
        session.roles = ["analyst"]
        session.role_id = "analyst"
        session.state = State.STREAMING
        return session

    def test_redirect_in_trailing_success_metadata(self):
        handle = {"sink": "object-store", "redirect_url": "http://x/f.parquet", "row_count": 9}
        session = self._session()
        session._result_columns = []
        session._result_rows = []
        session._result_redirect = handle
        session._pull_offset = 0

        sent: list[dict] = []
        session.send_success = lambda meta=None: sent.append(meta or {})  # type: ignore[method-assign]
        session.send_record = lambda values: pytest.fail("no records should stream on a redirect")  # type: ignore[method-assign]

        session.handle_pull([{"n": -1}])

        assert sent, "PULL must emit a SUCCESS summary"
        summary = sent[-1]
        assert summary["has_more"] is False
        assert summary["redirect"] == handle

    def test_no_redirect_key_when_not_materialized(self):
        session = self._session()
        session._result_columns = ["x"]
        session._result_rows = [[1]]
        session._result_redirect = None
        session._pull_offset = 0

        sent: list[dict] = []
        session.send_success = lambda meta=None: sent.append(meta or {})  # type: ignore[method-assign]
        session.send_record = lambda values: None  # type: ignore[method-assign]

        session.handle_pull([{"n": -1}])

        assert "redirect" not in sent[-1]
