# Copyright (c) 2026 Kenneth Stott
# Canary: 1f3f1524-1daa-4efc-8b49-6817aad93159
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The roles a test server's MCP agent may act as (REQ-2075).

A loopback MCP call with no bearer acts only within the roles the operator pinned in mcp.role
(PROVISA_MCP_ROLE). A test server's operator is its harness, and the agent it drives acts as the
roles the server's configuration declares and the reserved org_admin (REQ-1349): the harness pins
exactly those. A test that pins otherwise sets PROVISA_MCP_ROLE in the environment it passes,
which is applied after the harness's.
"""

from __future__ import annotations

from pathlib import Path


def pinned_roles(config: dict) -> str:
    """``mcp.role`` for a server on ``config``: org_admin and every role it declares."""
    declared = [r["id"] for r in config.get("roles") or []]
    return ",".join(dict.fromkeys(["org_admin", *declared]))


def pinned_roles_of(config_path: str | Path) -> str:
    """:func:`pinned_roles` of the configuration file at ``config_path``, its includes read."""
    from provisa.core.config_loader import read_config_with_includes

    return pinned_roles(read_config_with_includes(Path(config_path)))
