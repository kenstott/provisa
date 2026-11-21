# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2d4e61-3a7b-4f59-9d10-6b5e2f8a7c43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The embedded control plane's data directory sits under the product's own directory
(~/.provisa/demo/control-pg), which a first start has not created; pgserver refuses a pgdata whose
parent is missing, so starting creates the parent."""

from __future__ import annotations

import sys
import types

from provisa.core import control_plane_pg


def test_server_creates_the_data_directorys_missing_parent(monkeypatch, tmp_path):
    seen = {}

    def get_server(pgdata, cleanup_mode):
        seen["parent_exists"] = pgdata.parent.is_dir()
        return object()

    monkeypatch.setitem(
        sys.modules, "pixeltable_pgserver", types.SimpleNamespace(get_server=get_server)
    )
    control_plane_pg._server(str(tmp_path / "demo" / "control-pg"))
    assert seen["parent_exists"] is True
