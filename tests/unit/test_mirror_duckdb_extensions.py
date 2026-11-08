# Copyright (c) 2026 Kenneth Stott
# Canary: 30035524-316a-4035-bd3a-061141187519
#
# This source code is licensed under the Business Source License 1.1
"""The DuckDB extensions an air-gapped install bundles (required_extensions, which the mirror reads)
are every one the DuckDB engine's connectors declare, the core scanners, and what a file
source's location needs (REQ-1997, REQ-1998). The list once came from a module the connectors
had moved out of, and an install got postgres and sqlite alone."""

from __future__ import annotations

from provisa.federation.duckdb_extensions import required_extensions


def test_every_extension_the_duckdb_engine_loads_is_mirrored():
    from provisa.federation.engine import build_duckdb_engine

    listed = required_extensions()
    declared = {
        c.extension: bool(getattr(c, "install_from_community", False))
        for c in build_duckdb_engine().connectors.values()
        if getattr(c, "extension", None)
    }
    for ext, community in declared.items():
        assert listed[ext] == community, ext
    assert {"motherduck", "snowflake", "mssql", "iceberg"} <= set(listed)


def test_the_core_scanners_and_file_location_extensions_are_mirrored_from_core():
    listed = required_extensions()
    for ext in ("postgres", "sqlite", "httpfs", "azure"):
        assert listed[ext] is False, ext  # DuckDB's core repository


def test_what_no_connector_declares_is_mirrored():
    # REQ-294: avro is what iceberg loads when it loads, and with no install at runtime it has to be
    # on disk; vss backs the catalog index, crypto sha224/384/512.
    listed = required_extensions()
    assert listed["avro"] is False and listed["vss"] is False
    assert listed["crypto"] is True  # the community repository
