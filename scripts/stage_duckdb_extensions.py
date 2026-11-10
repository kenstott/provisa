#!/usr/bin/env python
# Copyright (c) 2026 Kenneth Stott
# Canary: c8d11db1-1d9c-45d3-8507-93f58757e6a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Stage every DuckDB extension on a source checkout or a CI runner, then probe each (REQ-294).

A checkout is not an install: this INSTALLs from DuckDB's own repositories, once, into DuckDB's per-user
directory (or ``PROVISA_DUCKDB_EXT_DIR``). An install uses ``python -m provisa.runtime_deps.stage_assets``,
from the package index. Exit code is non-zero if any probe failed, so a build can gate on it.

A platform with no pgwire bundle (Linux arm64, REQ-294 amended 2026-11-10) gets the DuckDB extensions
but no pgwire connector: there each pgwire connector's probe must be refused with exactly the
runtime's own refusal (``bundle_variant``), and any other outcome -- another error, or a pgwire
connector that loads -- fails the build.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Iterable

from provisa.federation.duckdb_extensions import (
    CORE_EXTENSION_WHEELS,
    EXTENSION_REPOSITORIES,
    connect,
    duckdb_extension_connectors,
    extension_directory,
    ExtensionProbe,
    probe_extensions,
)
from provisa.runtime_deps.pgwire_bundles import BUNDLE_CONNECTOR, BundleUnavailable, bundle_variant


def install_statements(extension: str, *, from_community: bool) -> list[str]:
    """The statements that INSTALL ``extension``: from its named repository for an extension of
    EXTENSION_REPOSITORIES (created where the connection does not know it), else from community or
    the core registry."""
    if extension in EXTENSION_REPOSITORIES:
        repository, prefix = EXTENSION_REPOSITORIES[extension]
        return [
            "SET allow_extension_repositories = 'allowed'",
            f"CREATE EXTENSION REPOSITORY IF NOT EXISTS {repository} WITH PREFIX '{prefix}'",
            f"INSTALL {extension} FROM {repository}",
        ]
    return [f"INSTALL {extension} FROM community" if from_community else f"INSTALL {extension}"]


def wanted() -> dict[str, bool]:
    """name -> from the community registry: the core wheels' extensions, what the connectors declare."""
    extensions = dict.fromkeys(CORE_EXTENSION_WHEELS, False)
    extensions.update(dict.fromkeys(EXTENSION_REPOSITORIES, False))
    for connector in duckdb_extension_connectors():
        name = connector.extension or ""
        extensions[name] = extensions.get(name, False) or bool(connector.install_from_community)
    return extensions


def install() -> dict[str, str]:
    """INSTALL each extension on a connection of its own; return those with no build here, each
    with why. httpfs goes first: a named repository's https URL is fetched through it."""
    unavailable: dict[str, str] = {}
    extensions = wanted()
    for name in sorted(extensions, key=lambda n: (n != "httpfs", n)):
        con = connect()
        try:
            if name in EXTENSION_REPOSITORIES:
                con.execute("LOAD httpfs")
            for statement in install_statements(name, from_community=extensions[name]):
                con.execute(statement)
        except Exception as error:  # noqa: BLE001 -- no build for this DuckDB is a named row
            unavailable[name] = str(error).splitlines()[0][:90]
        finally:
            con.close()
    return unavailable


def pgwire_platform_refusal() -> str | None:
    """The runtime's refusal of every pgwire connector on this host, or None where the release
    builds a bundle for it. Read from ``bundle_variant`` itself, so the platforms are the runtime's
    ``BUNDLE_VARIANTS``, never a list kept here."""
    try:
        bundle_variant()
    except BundleUnavailable as refused:
        return str(refused)
    return None


def grade(
    probe: ExtensionProbe, unavailable: Iterable[str], refusal: str | None
) -> tuple[str, bool]:
    """(mark, failed) for one probe row. On a host with no pgwire bundle (``refusal`` set) a pgwire
    connector passes only when refused with exactly ``refusal``; one that loads, or fails for any
    other reason, fails. Elsewhere: an extension with no build for this DuckDB is skipped, any other
    unavailable probe fails."""
    if refusal is not None and probe.source_type in BUNDLE_CONNECTOR:
        if not probe.available and probe.reason == refusal:
            return "NONE", False
        return "FAIL", True
    if probe.available:
        return "OK  ", False
    if probe.extension in unavailable:
        return "SKIP", False
    return "FAIL", True


async def _main() -> int:
    print(f"Staging DuckDB extensions into: {extension_directory() or 'DuckDB per-user directory'}")
    unavailable = install()
    for name, why in unavailable.items():
        print(f"  [SKIP] {name}: {why}")
    refusal = pgwire_platform_refusal()
    probes = await probe_extensions()
    width = max(len(p.key) for p in probes)
    failed = refused = 0
    for p in probes:
        # An extension with no build for this DuckDB is its row above: the engine shows it as
        # not attachable when a source of it is added (REQ-1142), and the build does not stop.
        mark, bad = grade(p, unavailable, refusal)
        failed += bad
        refused += mark == "NONE"
        reason = p.reason
        if bad and refusal is not None and p.source_type in BUNDLE_CONNECTOR:
            reason = f"{p.reason} -- expected the refusal: {refusal}"
        print(f"  [{mark}] {p.key.ljust(width)}  {p.extension:<10} -> {p.source_type:<12} {reason}")
        if not p.available and p.remediation and mark != "NONE":
            print(f"         remediation: {p.remediation}")
    print(f"\n{len(probes) - failed}/{len(probes)} extensions probed OK.")
    if refused:
        print(f"{refused} pgwire connectors refused as expected: {refusal}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
