# Copyright (c) 2026 Kenneth Stott
# Canary: 8f4d2e73-9a15-4b07-c6e1-3f5d9b8a2c01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit regression: fixture roles in test_command_assignment_every_surface need command_execution.

REQ-870 (amended 2026-11-09): COMMAND EXECUTION IS A CAPABILITY, THE ASSIGNMENT IS THE GRANT.
A role runs a command only when it holds ``command_execution``, is assigned the command and
reaches the command's domain.

Root cause on release/v0.1.0-alpha.508 CI run 42821626805: the fixture role ``reader`` in
test_command_assignment_every_surface.py was declared with data-reading capabilities
(query_development, full_results) but without ``command_execution``. Every command call by
``reader`` was refused with::

    Unknown command: 'region_count'

on every surface (SQL HTTP, pgwire, MCP, Cypher HTTP, Bolt, GraphQL) — ``command_reachable``
returned False for it, so the command was invisible as if the role were ``outsider``.

Fix: add ``command_execution`` to the ``reader`` role's capabilities in the fixture.

This test reproduces the fault as a container-free unit test: the fixture role before the fix
cannot reach its assigned command.
"""

# Requirements: REQ-870

from __future__ import annotations

import pytest

from provisa.security.inheritance import OWN_DOMAIN_ACCESS
from provisa.security.mutation_authz import (
    CommandNotFound,
    admit_command,
    command_reachable,
)
from provisa.security.rights import Capability

RUN = Capability.COMMAND_EXECUTION.value

# Command exactly as registered in the test_command_assignment_every_surface fixture.
_REGION_COUNT = {
    "name": "region_count",
    "kind": "query",
    "domain_id": "sales",
    "visible_to": ["org_admin", "reader"],
}

# Data-reading capabilities the fixture declares (before the fix).
_READS = ["query_development", "full_results"]

# Capabilities before and after the fix.
_READER_CAPS_BEFORE_FIX = [*_READS]  # missing command_execution
_READER_CAPS_AFTER_FIX = [*_READS, RUN]  # the fix: add command_execution


def _role(rid: str, caps: list[str]) -> dict:
    return {
        "id": rid,
        "description": rid,
        "capabilities": caps,
        "domain_access": ["*"],
        "parents": [],
        OWN_DOMAIN_ACCESS: ["*"],
    }


class TestReaderBeforeFix:
    """Reproduces the loss: reader without command_execution cannot reach region_count."""

    def test_reader_without_command_execution_cannot_reach_region_count(self):
        """The bug: reader is in visible_to but lacks command_execution, so it is refused."""
        reader = _role("reader", _READER_CAPS_BEFORE_FIX)
        # command_reachable returns False → command is hidden, not admitted.
        assert not command_reachable(_REGION_COUNT, reader)

    def test_reader_without_command_execution_raises_command_not_found(self):
        reader = _role("reader", _READER_CAPS_BEFORE_FIX)
        with pytest.raises(CommandNotFound):
            admit_command(_REGION_COUNT, reader, "region_count")

    def test_outsider_not_in_visible_to_cannot_reach_region_count(self):
        """outsider is not assigned the command and cannot reach it regardless of capabilities."""
        outsider = _role("outsider", [*_READS, "write"])
        assert not command_reachable(_REGION_COUNT, outsider)


class TestReaderAfterFix:
    """After adding command_execution: reader reaches its assigned command."""

    def test_reader_with_command_execution_reaches_region_count(self):
        reader = _role("reader", _READER_CAPS_AFTER_FIX)
        assert command_reachable(_REGION_COUNT, reader)

    def test_reader_with_command_execution_admits_region_count(self):
        reader = _role("reader", _READER_CAPS_AFTER_FIX)
        admit_command(_REGION_COUNT, reader, "region_count")  # no exception

    def test_outsider_still_cannot_reach_region_count_after_fix(self):
        """Adding command_execution to reader does not affect outsider's visibility."""
        outsider = _role("outsider", [*_READS, "write", RUN])
        # outsider is NOT in visible_to: the assignment gates it.
        assert not command_reachable(_REGION_COUNT, outsider)

    def test_outsider_still_raises_command_not_found(self):
        outsider = _role("outsider", [*_READS, "write", RUN])
        with pytest.raises(CommandNotFound):
            admit_command(_REGION_COUNT, outsider, "region_count")
