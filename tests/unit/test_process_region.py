# Copyright (c) 2026 Kenneth Stott
# Canary: 763d550f-6e41-4dc1-b586-3aa4103a033b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A node is launched in a mode and, when the platform declares regions, a region
(REQ-1916, REQ-1922). The launch is refused, naming what is wrong, before any store is opened."""

# Requirements: REQ-1916, REQ-1922

from __future__ import annotations

import pytest

from provisa.core import process_mode, process_region
from provisa.core.regions import DEFAULT_REGION

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture(autouse=True)
def _restore():
    mode, region = process_mode.mode(), process_region._region
    yield
    process_mode.set_mode(mode)
    process_region._region = region


def test_with_no_platform_regions_a_node_runs_in_the_one_implicit_region():
    process_region.bind_launch({}, requested=None)
    assert process_region.region() == DEFAULT_REGION


def test_with_no_platform_regions_a_requested_region_is_refused():
    with pytest.raises(process_region.LaunchRefused, match="the platform declares no regions"):
        process_region.bind_launch({}, requested="eu")


def test_a_node_launched_without_a_region_is_refused_listing_the_platforms():
    with pytest.raises(process_region.LaunchRefused) as refused:
        process_region.bind_launch(_PLATFORM, requested=None)
    said = str(refused.value)
    assert "--region" in said
    assert "eu (https://eu.example.com)" in said and "us (https://us.example.com)" in said


def test_a_region_the_platform_does_not_declare_is_refused():
    with pytest.raises(process_region.LaunchRefused, match=r"'ap' is not one of the platform's"):
        process_region.bind_launch(_PLATFORM, requested="ap")


def test_a_declared_region_is_the_nodes():
    process_region.bind_launch(_PLATFORM, requested="eu")
    assert process_region.region() == "eu"


def test_the_launch_reads_its_mode_and_region_from_the_environment(monkeypatch):
    monkeypatch.setenv("PROVISA_MODE", "coordinator")
    monkeypatch.setenv("PROVISA_REGION", "us")
    process_region.bind_from_environment({"platform": _PLATFORM})
    assert process_mode.mode() == process_mode.COORDINATOR
    assert process_region.region() == "us"


def test_an_unknown_mode_is_refused(monkeypatch):
    monkeypatch.setenv("PROVISA_MODE", "batch")
    monkeypatch.delenv("PROVISA_REGION", raising=False)
    with pytest.raises(process_region.LaunchRefused, match="unknown process mode 'batch'"):
        process_region.bind_from_environment({})


def test_the_run_command_takes_the_mode_and_region(monkeypatch):
    """``provisa run --mode query --region eu`` hands both to the process it starts."""
    from provisa import cli

    seen: dict = {}

    def _run(args):
        import os

        seen["mode"], seen["region"] = (
            os.environ.get("PROVISA_MODE"),
            os.environ.get("PROVISA_REGION"),
        )
        return 0

    monkeypatch.setattr(cli, "_cmd_run", _run)
    monkeypatch.delenv("PROVISA_MODE", raising=False)
    monkeypatch.delenv("PROVISA_REGION", raising=False)
    assert cli.main(["run", "--mode", "query", "--region", "eu"]) == 0
    assert seen == {"mode": "query", "region": "eu"}
