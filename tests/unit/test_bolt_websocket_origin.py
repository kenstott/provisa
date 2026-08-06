# Copyright (c) 2026 Kenneth Stott
# Canary: 78d6d0b9-10e5-48e2-96d8-6d17801f38a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Bolt WebSocket upgrade checks Origin (REQ-802).

A page on any site can open a WebSocket to the Bolt port — the browser sends the connection without
asking, and the same-origin policy does not stop it. These tests pin the allowlist that decides
which sites may drive the port.
"""

from __future__ import annotations

import pytest

from provisa.bolt.websocket import _check_origin


class _Writer:
    def __init__(self):
        self.written = b""

    def write(self, data: bytes) -> None:
        self.written += data


class TestOriginCheck:
    def test_a_driver_sends_no_origin_and_is_admitted(self, monkeypatch):
        """Only browsers send Origin, so a header-less upgrade is not what this check governs."""
        monkeypatch.delenv("PROVISA_BOLT_ALLOWED_ORIGINS", raising=False)
        writer = _Writer()
        _check_origin(None, writer)  # type: ignore[arg-type]
        assert writer.written == b""

    def test_a_listed_origin_is_admitted(self, monkeypatch):
        monkeypatch.setenv(
            "PROVISA_BOLT_ALLOWED_ORIGINS", "https://browser.test, https://console.test"
        )
        writer = _Writer()
        _check_origin("https://console.test", writer)  # type: ignore[arg-type]
        assert writer.written == b""

    def test_an_unlisted_origin_is_refused(self, monkeypatch):
        monkeypatch.setenv("PROVISA_BOLT_ALLOWED_ORIGINS", "https://browser.test")
        writer = _Writer()
        with pytest.raises(ConnectionError, match="evil.test"):
            _check_origin("https://evil.test", writer)  # type: ignore[arg-type]
        assert writer.written.startswith(b"HTTP/1.1 403 Forbidden")

    def test_with_nothing_listed_every_browser_origin_is_refused(self, monkeypatch):
        """Unset means no site is listed — a browser upgrade is not admitted by default."""
        monkeypatch.delenv("PROVISA_BOLT_ALLOWED_ORIGINS", raising=False)
        with pytest.raises(ConnectionError):
            _check_origin("https://browser.test", _Writer())  # type: ignore[arg-type]
