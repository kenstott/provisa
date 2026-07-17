# Copyright (c) 2026 Kenneth Stott
# Canary: 0567f31e-73c6-4e71-9206-477c4848d0c9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Model Context Protocol (MCP) server package (REQ-1008, phase 1).

A protocol adapter that exposes catalog discovery and governed SQL execution to
external AI agents. Zero new governance logic — every SQL path routes through the
existing choke point ``_govern_and_route`` in ``provisa.pgwire._pipeline`` and
catalog reads reuse ``build_catalog_tables`` from ``provisa.api.flight.catalog``.

Phase 1 scope: deterministic drill-down (list_schemas / list_tables /
describe_table) + governed run_sql / explain_sql. The semantic ``search_catalog``
tool (embeddings, DuckDB VSS, get_chunk, incremental reindex) is DEFERRED to
phase 2 and intentionally not implemented here.
"""

from provisa.api.mcp.server import build_mcp_server, start_mcp_server

__all__ = ["build_mcp_server", "start_mcp_server"]
