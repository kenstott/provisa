# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6f2a8e-5b14-4d7e-9e3a-7a1f4c2d9b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tag every paid cloud resource a test creates carries: the run that made it and when.

Maintainer 2026-11-10 (.claude/refs/swarm-mode.md, "Paid cloud resources"): a test that needs a
billable cloud resource creates it in its run, deletes it in teardown and reads it back; every
resource it creates carries the run that made it and its creation time. The cloud audit
(scripts/ci/cloud_audit.py, every 4 hours) keeps a resource only while its run is in progress and
deletes every other one in a test scope, untagged ones included.

The two keys and their values fit every provider's tag or label rules (Azure tags, AWS tags, GCP
labels: lowercase keys, values of lowercase letters, digits, ``-`` and ``_``):

- ``provisa-run``: ``gh-<GITHUB_RUN_ID>`` in a GitHub Actions run, else ``local-<12 hex>``, one
  per process;
- ``provisa-created``: the creation time, in whole seconds since the epoch (UTC).
"""

from __future__ import annotations

import functools
import os
import time
import uuid

RUN_KEY = "provisa-run"
CREATED_KEY = "provisa-created"
GITHUB_PREFIX = "gh-"
LOCAL_PREFIX = "local-"


@functools.cache
def run_id() -> str:
    """This run's id: the GitHub Actions run's, or one of this process's own outside Actions."""
    github = os.environ.get("GITHUB_RUN_ID")
    if github:
        return f"{GITHUB_PREFIX}{github}"
    return f"{LOCAL_PREFIX}{uuid.uuid4().hex[:12]}"


def run_tags(now: float | None = None) -> dict[str, str]:
    """The tags of a resource this run creates now."""
    created = int(time.time() if now is None else now)
    return {RUN_KEY: run_id(), CREATED_KEY: str(created)}


def az_tag_args(now: float | None = None) -> list[str]:
    """The tags as ``az ... --tags`` arguments (``key=value`` each)."""
    return [f"{key}={value}" for key, value in run_tags(now).items()]


def run_token() -> str:
    """A short lowercase-alphanumeric token of this run, for resource names whose rules allow
    nothing else (a storage account, a Fabric capacity): the GitHub run id's digits, or the local
    run's hex."""
    return run_id().removeprefix(GITHUB_PREFIX).removeprefix(LOCAL_PREFIX)
