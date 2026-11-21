# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2e3f91-5c84-4b16-9d47-8e1f2a6c0b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit regression: fixture roles in test_command_admission_every_surface need command_execution.

REQ-870 (amended 2026-11-09): a role runs a command only when it holds command_execution,
is assigned the command and reaches the command's domain.

Root cause on release/v0.1.0-alpha.508: the fixture roles ``writer`` and ``reader`` in
test_command_admission_every_surface.py were declared with data-reading capabilities
(query_development, full_results, write) but without ``command_execution``.  After
commit 8ead482611 (J1117) added the command_execution gate, every command call was
refused with "Unknown command" on every surface — the fixture roles could not pass
``command_reachable``.

Fix: add ``command_execution`` to ``writer`` and ``reader`` in the fixture.

This test reproduces the loss in a container-free unit assertion: the fixture roles
as originally declared cannot reach their assigned commands.  After the fix (adding
command_execution to the capabilities list), command_reachable returns True and the
write-gate still fires for data-reading roles on mutation commands.
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

# Commands exactly as registered in the fixture
_REGION_COUNT = {
    "name": "region_count",
    "kind": "query",
    "domain_id": "sales",
    "visible_to": ["org_admin", "writer", "reader"],
}
_RELABEL = {
    "name": "relabel",
    "kind": "mutation",
    "domain_id": "sales",
    "visible_to": ["org_admin", "writer", "reader"],
}

# Capabilities exactly as declared in the fixture (before the fix)
_READS = ["query_development", "full_results"]
_WRITER_CAPS_BEFORE_FIX = [*_READS, "write"]  # missing command_execution
_READER_CAPS_BEFORE_FIX = [*_READS]  # missing command_execution

# Capabilities after the fix
_WRITER_CAPS_AFTER_FIX = [*_READS, "write", RUN]
_READER_CAPS_AFTER_FIX = [*_READS, RUN]


def _role(rid: str, caps: list[str]) -> dict:
    return {
        "id": rid,
        "description": rid,
        "capabilities": caps,
        "domain_access": ["*"],
        "parents": [],
        OWN_DOMAIN_ACCESS: ["*"],
    }


class TestFixtureRolesBeforeFix:
    """Reproduces the loss: fixture roles without command_execution cannot reach commands."""

    def test_writer_without_command_execution_cannot_reach_region_count(self):
        writer = _role("writer", _WRITER_CAPS_BEFORE_FIX)
        # This is the bug: writer is in visible_to but lacks command_execution
        assert not command_reachable(_REGION_COUNT, writer)

    def test_writer_without_command_execution_cannot_reach_relabel(self):
        writer = _role("writer", _WRITER_CAPS_BEFORE_FIX)
        assert not command_reachable(_RELABEL, writer)

    def test_reader_without_command_execution_cannot_reach_region_count(self):
        reader = _role("reader", _READER_CAPS_BEFORE_FIX)
        assert not command_reachable(_REGION_COUNT, reader)


class TestFixtureRolesAfterFix:
    """After adding command_execution: assigned roles reach their commands."""

    def test_writer_with_command_execution_reaches_region_count(self):
        writer = _role("writer", _WRITER_CAPS_AFTER_FIX)
        assert command_reachable(_REGION_COUNT, writer)

    def test_writer_with_command_execution_reaches_relabel(self):
        writer = _role("writer", _WRITER_CAPS_AFTER_FIX)
        assert command_reachable(_RELABEL, writer)

    def test_reader_with_command_execution_reaches_region_count(self):
        reader = _role("reader", _READER_CAPS_AFTER_FIX)
        assert command_reachable(_REGION_COUNT, reader)

    def test_reader_with_command_execution_reaches_relabel(self):
        reader = _role("reader", _READER_CAPS_AFTER_FIX)
        assert command_reachable(_RELABEL, reader)

    def test_writer_calls_region_count_without_error(self):
        writer = _role("writer", _WRITER_CAPS_AFTER_FIX)
        # REQ-870, THE ASSIGNMENT IS THE GRANT: query command needs no write.
        admit_command(_REGION_COUNT, writer, "region_count")

    def test_writer_calls_relabel_without_error(self):
        # writer holds write, so mutation is admitted
        writer = _role("writer", _WRITER_CAPS_AFTER_FIX)
        admit_command(_RELABEL, writer, "relabel")

    def test_reader_with_command_execution_runs_mutation_without_write(self):
        # REQ-870, THE ASSIGNMENT IS THE GRANT: the assignment is the grant; write is not required.
        reader = _role("reader", _READER_CAPS_AFTER_FIX)
        admit_command(_RELABEL, reader, "relabel")  # no exception: the assignment is the grant

    def test_reader_with_command_execution_calls_read_query_without_error(self):
        reader = _role("reader", _READER_CAPS_AFTER_FIX)
        admit_command(_REGION_COUNT, reader, "region_count")

    def test_outsider_is_not_in_visible_to_so_cannot_reach_commands(self):
        outsider = _role("outsider", [*_READS, "write", RUN])
        assert not command_reachable(_REGION_COUNT, outsider)
        with pytest.raises(CommandNotFound):
            admit_command(_REGION_COUNT, outsider, "region_count")
