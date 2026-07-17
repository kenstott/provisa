# Copyright (c) 2026 Kenneth Stott
# Canary: 3a43c9b0-26bc-4bcf-844e-0098e29ce127
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-837: platform and tenant control planes use two independent SQLAlchemy
engines/pools; the platform DB is configured by PLATFORM_DATABASE_URL and is
required at startup with no fallback.

Runs against SQLite in-memory (no Docker).
"""

from __future__ import annotations

import pytest

from provisa.core.database import create_engine_from_url


def test_engines_are_independent():
    # Two URLs -> two distinct AsyncEngine instances with distinct pools.
    # (URLs are not connected; engine construction is lazy.)
    admin = create_engine_from_url("postgresql+asyncpg://u:p@h/admin")
    tenant = create_engine_from_url("postgresql+asyncpg://u:p@h/tenant")
    assert admin is not tenant
    assert admin.pool is not tenant.pool


def test_any_async_backend_accepted():
    # PLATFORM_DATABASE_URL supports any async SQLAlchemy URI, not just asyncpg.
    for url, backend in (
        ("postgresql+asyncpg://u:p@h/db", "postgresql"),
        ("sqlite+aiosqlite:////tmp/provisa_req837.db", "sqlite"),
    ):
        engine = create_engine_from_url(url)
        assert engine.url.get_backend_name() == backend


def test_platform_url_default_has_no_fallback(monkeypatch):
    # REQ-837: ControlPlaneConfig.platform_url references PLATFORM_DATABASE_URL
    # with no ``:-default`` — an unset var must fail loud, never silently default.
    from provisa.core.models import ControlPlaneConfig
    from provisa.core.secrets import resolve_secrets

    ref = ControlPlaneConfig().platform_url
    assert "PLATFORM_DATABASE_URL" in ref
    assert ":-" not in ref, f"platform_url must not carry a fallback default: {ref!r}"

    # Resolution fails loud when the env var is unset (no fallback).
    monkeypatch.delenv("PLATFORM_DATABASE_URL", raising=False)
    with pytest.raises(KeyError):
        resolve_secrets(ref)

    # Resolution honours the env var when set.
    monkeypatch.setenv("PLATFORM_DATABASE_URL", "postgresql+asyncpg://u:p@h/plat")
    assert resolve_secrets(ref) == "postgresql+asyncpg://u:p@h/plat"
