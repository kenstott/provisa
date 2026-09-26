# Copyright (c) 2026 Kenneth Stott
# Canary: c674b4d7-32ab-49df-bdd9-70d5b9466b4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``EngineRuntime.execute_pg_engine_passthrough`` — the REQ-1863 counterpart for the ENGINE route
when the bound federation engine is itself Postgres (REQ-904, PROVISA_ENGINE=pg): pgwire and the
engine both speak real Postgres wire protocol end to end, so the same raw-DataRow passthrough
mechanism (provisa/pgwire/pg_passthrough.py) applies to the ENGINE route too, not just DIRECT."""

from __future__ import annotations

import pytest

from provisa.federation.runtime import EngineRuntime, _strip_driver_suffix
from provisa.pgwire.pg_passthrough import PassthroughError


class _FakeEngine:
    def __init__(self, materialize_store: str | None = None) -> None:
        self.backend = object()
        self._materialize_store = materialize_store

    def default_materialize_store(self) -> str | None:
        return self._materialize_store


def test_no_configured_url_raises_passthrough_error(monkeypatch):
    import provisa.federation.engine as engine_mod

    monkeypatch.setattr(engine_mod, "configured_engine_url", lambda: None)
    rt = EngineRuntime(_FakeEngine(materialize_store=None), state=object())

    with pytest.raises(PassthroughError, match="no configured URL"):
        rt.execute_pg_engine_passthrough("SELECT 1", None, [0], loop=object())


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("postgresql+psycopg2://u:p@host:5432/db", "postgresql://u:p@host:5432/db"),
        ("postgresql://u:p@host:5432/db", "postgresql://u:p@host:5432/db"),
        ("postgres+asyncpg://u:p@host/db", "postgres://u:p@host/db"),
    ],
)
def test_strip_driver_suffix(raw: str, expected: str) -> None:
    assert _strip_driver_suffix(raw) == expected
