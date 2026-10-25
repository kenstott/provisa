# Copyright (c) 2026 Kenneth Stott
# Canary: 944778c9-f07c-4cba-9033-743b4e1a4b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo whose stores the node could not bring up is refused to the visitor waiting on it, not
left on "Starting this demo..." (REQ-2049; cloud-dev, 2026-10-22). The node says why in
<name>/failed (scripts/demo_stores_sync.sh); the status the waiting page polls answers with it;
trying again is a new open, which asks the node again and clears it."""

# Requirements: REQ-2049

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.api.errors import ApiError
from provisa.core import demo_bundles


@pytest.fixture
def bundles(tmp_path, monkeypatch):
    root = tmp_path / "bundles"
    (root / "weblogs").mkdir(parents=True)
    monkeypatch.setattr(demo_bundles, "bundles_root", lambda: root)
    monkeypatch.setenv("PROVISA_DEMO_BUNDLES", "weblogs")
    return root / "weblogs"


def _demo(*, stores_ready: bool):
    return SimpleNamespace(name="weblogs", title="Web logs", stores_ready=stores_ready)


def test_a_demo_whose_stores_failed_is_refused_to_its_waiting_visitor(bundles, caplog):
    from provisa.api.demo_sandbox import _refuse_failed_stores

    why = "Error response from daemon: network b11f5609 not found"
    (bundles / "failed").write_text(why + "\n")
    with pytest.raises(ApiError) as refused:
        _refuse_failed_stores(_demo(stores_ready=False))  # pyright: ignore[reportArgumentType]
    assert refused.value.status_code == 503
    assert refused.value.code == "sandbox.demo_stores_failed"
    assert "Web logs could not be started" in str(refused.value.detail)
    assert "b11f5609" not in str(refused.value.detail)  # the daemon's words are the operator's
    assert why in caplog.text


def test_a_demo_starting_or_up_is_not_refused(bundles):
    from provisa.api.demo_sandbox import _refuse_failed_stores

    _refuse_failed_stores(_demo(stores_ready=False))  # pyright: ignore[reportArgumentType]
    (bundles / "failed").write_text("an earlier version's failure\n")
    _refuse_failed_stores(_demo(stores_ready=True))  # pyright: ignore[reportArgumentType]


def test_asking_for_a_demo_again_clears_why_it_failed(bundles: Path):
    (bundles / "failed").write_text("it did not start\n")
    assert demo_bundles.stores_failed("weblogs") == "it did not start"
    demo_bundles.request("weblogs")
    assert demo_bundles.stores_failed("weblogs") is None
    assert (bundles / "requested").read_text().strip() == "requested"
