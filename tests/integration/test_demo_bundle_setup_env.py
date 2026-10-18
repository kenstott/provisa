# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The bundle build's setup server starts where the build has no HOME (scripts/demo_bundle.py).

The build gives its servers an environment of their own. Without HOME, DuckDB cannot place its
per-user extension directory: the server failed to start ("To set the TimeZone setting, the icu
extension needs to be loaded. But it could not be autoloaded."). The build now names where DuckDB
finds its extensions (_duckdb_env), and this sets up a demo with HOME unset to show the server
starts and its setup runs."""

# Requirements: REQ-2016, REQ-2049

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml

from tests.integration.test_demo_sandboxes_e2e import _demo

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_REPO = Path(__file__).resolve().parents[2]


def _builder():
    spec = importlib.util.spec_from_file_location(
        "demo_bundle", _REPO / "scripts" / "demo_bundle.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _NoStores:
    """The demo reads its own sqlite file: no stores for the build to name."""

    env: dict[str, str] = {}


def test_the_setup_server_starts_and_sets_up_with_home_absent(tmp_path, monkeypatch):
    named = tmp_path / "named"
    _demo(named, "alpha", published=True)
    demo = named / "alpha"
    # Its sqlite file by its path: the setup server is given only the variables the build holds.
    config = demo / "config.yaml"
    config.write_text(config.read_text().replace("${env:PROVISA_NAMED_DEMO_DIR}", str(demo)))
    (demo / "setup.yaml").write_text(yaml.safe_dump({"steps": []}))
    monkeypatch.delenv("HOME", raising=False)
    # Raises SystemExit naming an early exit (the server did not start) or a timeout.
    _builder()._set_up(tmp_path, demo, _NoStores(), 300)
