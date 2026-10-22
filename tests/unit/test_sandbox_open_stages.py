# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2f4b61-0e7a-4c93-b5a8-6f1c3e9d2a07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An open that stalls names where (REQ-2006): each stage of opening a demo sandbox is logged as it
starts and timed as it ends, and a wait on a materialize store's lock names the store. A worldcup
open on cloud-dev ran past ten minutes with no line at all to say where."""

# Requirements: REQ-2006

from __future__ import annotations

import fcntl
import logging
import threading
import time

import pytest

from provisa.api import demo_sandbox
from provisa.federation import materialize_broker


@pytest.mark.asyncio
async def test_a_stage_is_named_as_it_starts_and_timed_as_it_ends(caplog):
    with caplog.at_level(logging.WARNING, logger=demo_sandbox.__name__):
        async with demo_sandbox._stage("sbx1", "worldcup", "its runtime"):
            assert caplog.messages == ["demo sandbox sbx1 of worldcup: its runtime ..."]
    assert caplog.messages[-1].startswith("demo sandbox sbx1 of worldcup: its runtime took ")


@pytest.mark.asyncio
async def test_a_stage_that_fails_is_still_timed(caplog):
    with caplog.at_level(logging.WARNING, logger=demo_sandbox.__name__):
        with pytest.raises(RuntimeError):
            async with demo_sandbox._stage("sbx1", "worldcup", "the template's store"):
                raise RuntimeError("copy failed")
    assert caplog.messages[-1].startswith(
        "demo sandbox sbx1 of worldcup: the template's store took"
    )


def test_an_uncontended_store_lock_says_nothing(tmp_path, caplog):
    path = tmp_path / "materialize.duckdb.lock"
    with (
        open(path, "a+") as mine,
        caplog.at_level(logging.WARNING, logger=materialize_broker.__name__),
    ):
        materialize_broker._lock_exclusive(mine.fileno(), str(path))
    assert caplog.messages == []


def test_a_held_store_lock_names_the_store_while_it_waits(tmp_path, caplog):
    path = tmp_path / "materialize.duckdb.lock"
    holder = open(path, "a+")
    fcntl.flock(holder.fileno(), fcntl.LOCK_EX)

    def release() -> None:
        time.sleep(0.3)
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)

    threading.Thread(target=release).start()
    try:
        with (
            open(path, "a+") as mine,
            caplog.at_level(logging.WARNING, logger=materialize_broker.__name__),
        ):
            materialize_broker._lock_exclusive(mine.fileno(), str(path))
    finally:
        holder.close()
    assert caplog.messages[0] == f"waiting on the materialize store lock {path}"
    assert caplog.messages[1].startswith(f"materialize store lock {path}: waited ")
