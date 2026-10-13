# Copyright (c) 2026 Kenneth Stott
# Canary: 1f8848bd-1a08-483b-a043-5ce92d66abee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2014: the embedded control plane's address is read from its postmaster.pid on every
platform -- a unix socket directory where it has one, a TCP host on Windows (no unix sockets)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.control_plane_pg import _address


def test_a_server_on_a_unix_socket_is_addressed_by_its_directory_and_port():
    info = SimpleNamespace(socket_dir="/run/pg", hostname=None, port=5433)
    assert _address(info) == ("/run/pg", 5433)


def test_a_server_on_tcp_is_addressed_by_its_host_and_port():
    info = SimpleNamespace(socket_dir=None, hostname="127.0.0.1", port=61234)
    assert _address(info) == ("127.0.0.1", 61234)


def test_a_postmaster_that_reports_no_port_is_refused():
    with pytest.raises(RuntimeError, match="no port"):
        _address(SimpleNamespace(socket_dir="/run/pg", hostname=None, port=None))
