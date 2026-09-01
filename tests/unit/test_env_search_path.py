# Copyright (c) 2026 Kenneth Stott
# Canary: 7c62d1ab-90f4-4e35-8f17-2b0d9a4e6c31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Every schema a request lands in follows the environment being served (REQ-1623).

The four sites here each named ``org_<id>`` outright. Fixed that way, a request served for a
non-prod environment read and wrote prod's copy of the model: the roles table a runtime build
re-asserts rights into, the schema ``provisa_admin`` resolves an unqualified name in, and the
Trino terminal's default schema. The fourth -- the pgwire surface's reported ``search_path`` --
was not an environment problem at all but a process-global naming a schema no pgwire client can
see, so it is gone rather than routed.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import make_url

from provisa.api.org_runtime import reset_current_env, set_current_env
from provisa.core import trino_system_catalogs as tsc
from provisa.core.environments import PROD
from provisa.federation.trino_lifecycle import terminal_conn_kwargs

_URL = make_url("postgresql://cloud_user:cloud_pw@10.1.2.3:6543/provisa_cloud")


@pytest.fixture
def in_env():
    token = set_current_env("feature_x")
    yield "feature_x"
    reset_current_env(token)


# --- provisa_admin's currentSchema (core/trino_system_catalogs.py) -------------------------------


def test_control_plane_spec_follows_the_bound_environment(in_env):
    spec = tsc.control_plane_spec(_URL, "acme")
    assert spec.properties["connection-url"].endswith("?currentSchema=org_acme_env_feature_x")


def test_control_plane_spec_unbound_is_prod():
    spec = tsc.control_plane_spec(_URL, "acme")
    assert spec.properties["connection-url"].endswith("?currentSchema=org_acme")


def test_two_environments_do_not_share_the_control_plane_schema():
    a = set_current_env("feature_x")
    url_a = tsc.control_plane_spec(_URL, "acme").properties["connection-url"]
    reset_current_env(a)
    b = set_current_env("feature_y")
    url_b = tsc.control_plane_spec(_URL, "acme").properties["connection-url"]
    reset_current_env(b)
    assert url_a != url_b


# --- the Trino terminal's default schema (federation/trino_lifecycle.py) ------------------------


class _State:
    active_engine_endpoint = ("trino-host", 8080)
    active_isolated_org = None
    active_org_id = "acme"


def test_terminal_schema_follows_the_bound_environment(in_env):
    assert terminal_conn_kwargs(_State())["schema"] == "org_acme_env_feature_x"


def test_terminal_schema_unbound_is_prod():
    assert terminal_conn_kwargs(_State())["schema"] == "org_acme"


# --- the roles table a runtime build asserts into (core/db.py) ----------------------------------


class _Conn:
    def __init__(self, seen: list[str]) -> None:
        self._seen = seen

    async def execute(self, sql: str, *args: Any) -> None:
        self._seen.append(sql)

    async def execute_core(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def fetch(self, *args: Any, **kwargs: Any) -> list:
        return []


class _Acquire:
    def __init__(self, seen: list[str]) -> None:
        self._seen = seen

    async def __aenter__(self) -> _Conn:
        return _Conn(self._seen)

    async def __aexit__(self, *exc: Any) -> None:
        return None


class _Pool:
    dialect = "postgresql"

    def __init__(self) -> None:
        self.seen: list[str] = []

    def acquire(self) -> _Acquire:
        return _Acquire(self.seen)


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        (None, "SET search_path TO org_acme"),
        (PROD, "SET search_path TO org_acme"),
        ("feature_x", "SET search_path TO org_acme_env_feature_x"),
    ],
)
@pytest.mark.asyncio
async def test_role_grants_are_asserted_in_the_named_environment(env, expected):
    from provisa.core.db import apply_tenancy_role_grants

    pool = _Pool()
    await apply_tenancy_role_grants(pool, "acme", multitenancy=True, env=env)
    assert pool.seen[0] == expected


# --- the pgwire surface's reported search_path (api/app_startup.py) ------------------------------


def test_pgwire_reports_the_schema_it_actually_presents():
    # The catalog presents tables under ``public`` and ``current_schema()`` answers ``public``;
    # org_<id> holds none of a pgwire client's tables. Nothing may write the control plane's
    # schema into the reported setting.
    from provisa.pgwire.catalog_data import _KNOWN_SETTINGS

    assert _KNOWN_SETTINGS["search_path"] == 'public, "$user"'


def test_app_startup_does_not_override_the_pgwire_search_path():
    from pathlib import Path

    src = Path(__file__).resolve().parents[2] / "provisa" / "api" / "app_startup.py"
    assert '_KNOWN_SETTINGS["search_path"]' not in src.read_text()
