# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino's spooled data is encrypted: every config that runs fault-tolerant execution (which spools
exchange data) says fault-tolerant-execution.exchange-encryption-enabled=true outright, and none
turns on legacy spill-to-disk without spill-encryption-enabled=true."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from provisa.api import trino_setup
from tests.unit.test_helm_auth import _LOCAL, _documents, _render

_REPO = Path(__file__).resolve().parents[2]
_ENCRYPTED = "fault-tolerant-execution.exchange-encryption-enabled=true"


def _lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines()]


def _configs() -> dict[str, str]:
    """Every Trino config.properties the product writes or ships, by where it comes from."""
    found = {"trino_setup._FTE_TEMPLATE": trino_setup._FTE_TEMPLATE}
    for path in ("trino/etc/config.properties", "trino/etc/worker/config.properties"):
        found[path] = (_REPO / path).read_text()
    rendered = _render(*_LOCAL)
    assert rendered.returncode == 0, rendered.stderr
    for doc in _documents(rendered.stdout):
        if doc.get("kind") == "ConfigMap":
            for key, value in (doc.get("data") or {}).items():
                if key.endswith("config.properties"):
                    found[f"helm {doc['metadata']['name']}/{key}"] = value
    return found


@pytest.mark.parametrize("name", sorted(_configs()))
def test_every_config_that_spools_encrypts_it(name):
    lines = _lines(_configs()[name])
    if "retry-policy=TASK" in lines:
        assert _ENCRYPTED in lines


def test_the_helm_coordinator_and_worker_both_spool_encrypted():
    helm = [name for name in _configs() if name.startswith("helm ")]
    assert len([n for n in helm if _ENCRYPTED in _lines(_configs()[n])]) >= 2


def test_no_config_spills_to_disk_unencrypted():
    tracked = subprocess.run(
        ["git", "ls-files", "--", "*.properties", "*.py", "*.yaml", "*.yml", "*.tf"],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    texts = {path: (_REPO / path).read_text(errors="replace") for path in tracked}
    texts.update(_configs())
    for name, text in texts.items():
        if "spill-enabled=true" in text.replace(" ", ""):
            assert "spill-encryption-enabled=true" in text.replace(" ", ""), name
