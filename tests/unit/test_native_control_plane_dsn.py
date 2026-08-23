# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6c1e52-8a4d-4f27-9e13-2d7ab0645cf1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The libpq DSN the native engine ATTACHes its control plane by (REQ-1535)."""

import pytest
from sqlalchemy import make_url

from provisa.federation.native_backend import libpq_dsn

pytestmark = pytest.mark.unit


def test_a_tcp_url_names_its_host_and_port():
    dsn = libpq_dsn(make_url("postgresql+asyncpg://provisa:pw@db.internal:5433/provisa"))
    assert dsn == "host=db.internal port=5433 dbname=provisa user=provisa password=pw"


def test_the_embedded_plane_carries_socket_and_port_in_the_query():
    # REQ-1535: pgserver listens on a unix socket, so there is no host in the netloc at all.
    dsn = libpq_dsn(
        make_url("postgresql+asyncpg://provisa:provisa@/provisa?host=/tmp/cp-pg&port=54833")
    )
    assert dsn == "host=/tmp/cp-pg port=54833 dbname=provisa user=provisa password=provisa"


def test_a_url_naming_no_host_is_refused_rather_than_attached_as_none():
    with pytest.raises(ValueError, match="names no host"):
        libpq_dsn(make_url("postgresql+asyncpg:///provisa?port=54833"))


def test_a_url_naming_no_port_is_refused_rather_than_attached_as_none():
    with pytest.raises(ValueError, match="names no port"):
        libpq_dsn(make_url("postgresql+asyncpg:///provisa?host=/tmp/cp-pg"))


def _cp(url: str):
    from provisa.core.models import ControlPlaneConfig

    return ControlPlaneConfig(tenant_url=url, platform_url=url)


def test_tenant_parts_reads_the_socket_port_the_embedded_plane_listens_on():
    # A socket file is named after the port, and the embedded plane picks a free one — reading the
    # default 5432 here addresses a socket nothing is listening on.
    assert _cp(
        "postgresql+asyncpg://provisa:provisa@/provisa?host=/tmp/cp-pg&port=54833"
    ).tenant_parts() == ("/tmp/cp-pg", 54833, "provisa", "provisa", "provisa")


def test_tenant_parts_keeps_the_default_port_for_a_tcp_url_that_states_none():
    assert _cp("postgresql+asyncpg://provisa:pw@db.internal/provisa").tenant_parts() == (
        "db.internal",
        5432,
        "provisa",
        "provisa",
        "pw",
    )
