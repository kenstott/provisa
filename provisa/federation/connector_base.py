# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connector base abstractions for the federation engine (REQ-842).

Mechanism/Capability/CatalogEntry/ProbeResult value types and the Connector ABC.
Extracted from connector.py; concrete connectors live in connector.py and its
engine-family modules.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.models import Source


class Mechanism(str, Enum):  # REQ-841, REQ-947, REQ-951
    # How the engine/Provisa OBTAINS a source's rows. Materialization (landing into a store) is an
    # orthogonal STRATEGY, not a mechanism — any readable source is landable (see ``materializable``).
    ATTACH_RW = "attach_rw"  # engine attaches the live source in place, read + write upstream
    ATTACH_R = "attach_r"  # engine attaches/scans the live source in place, read-only (files, RO)
    SCAN = "scan"  # engine reads a file/object in place as a view (read_csv/read_parquet, iceberg_scan,
    # lakehouse table engines) — no live DB attached, no copy; freshness follows the underlying file
    DIRECT = "direct"  # Provisa's native driver reads the source directly, bypassing the engine
    FETCH = "fetch"  # Provisa's adapter reads an API/push source (openapi/graphql_remote/grpc/…)
    # ATTACH_* → the engine reads the LIVE source (VIRTUAL). SCAN → the engine reads a file/object in
    # place (a view, no copy). DIRECT/FETCH → the engine can't read it live, so Provisa reads it
    # (driver/adapter) and materializes a refreshed REPLICA (an MV of the source) that the engine reads
    # → MATERIALIZED. "Materialize into a store" is that replica, an OUTPUT of a DIRECT/FETCH read — not
    # a reach mechanism of its own (REQ-951).


# Reach modes where the engine reads the source LIVE IN PLACE — a live DB attach or a file/object
# scan — so the source MUST NOT be landed (the engine reads it directly). DIRECT/FETCH are excluded:
# Provisa reads those and lands a refreshed replica the engine reads (REQ-951).
LIVE_IN_PLACE = frozenset({Mechanism.ATTACH_RW, Mechanism.ATTACH_R, Mechanism.SCAN})


class DriverProvider(str, Enum):  # REQ-948
    """Who provides the runtime driver/library a connector's extension links."""

    SYSTEM = "system"  # OS-provided (e.g. libsqlite3 on macOS/Linux)
    BUNDLED = "bundled"  # Provisa ships + relocates it
    OPERATOR = "operator"  # BYO — the operator must install it


@dataclass(frozen=True)
class RuntimeDep:  # REQ-948
    """A non-core shared library a connector's extension links at runtime, tagged by who provides it.
    Structured (not free text) so the capability report / source dropdown can reason over provenance:
    an OPERATOR-provided dep that is not installed renders the source disabled with its remediation."""

    lib: str
    provider: DriverProvider


@dataclass(frozen=True)
class Capability:  # REQ-842, REQ-897
    """What a connector's engine can do with a source of this type.

    This is the connector-level PUSHDOWN capability trait of REQ-897 (predicate/join/aggregate) — a
    per (engine, source_type) planner INPUT, read via ``FederationEngine.connector_pushdown()`` and
    by promote.should_promote / plan_mask_evaluation. Orthogonal to the engine-wide DECLARED traits
    (reach/mpp/file_native/pooled/transactional/streaming) carried on ``EngineTraits``."""

    predicate_pushdown: bool = False
    join_pushdown: bool = False
    aggregate_pushdown: bool = False
    # Whether the engine can WRITE upstream through this connector (INSERT/UPDATE/DELETE reaching the
    # source of truth). Tracks the connector's write support so an engine-routed write path can prefer
    # a writable ATTACH connector; read-only attaches (file scanners, READ_ONLY warehouse links) are
    # False. Distinct from the DIRECT write path (executor/writable.py), which bypasses connectors.
    write: bool = False


@dataclass(frozen=True)
class CatalogEntry:  # REQ-842, REQ-843
    """A derived engine-catalog row projected from a registry asset.

    ``details`` is engine+source_type specific (Trino .properties, DuckDB ATTACH dsn or
    scanner view DDL, or empty for a warehouse-native land-into-self).
    """

    name: str
    engine: str
    source_type: str
    mechanism: Mechanism
    details: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ProbeResult:  # REQ-904
    """The outcome of probing a connector's dependency against a live engine.

    ``available`` is functional truth (the extension/FDW loads and works), not mere presence — the
    lesson being that a file on disk, or a row in pg_available_extensions, does not mean it loads
    (module-magic ABI, shared_preload_libraries, etc.). ``remediation`` is the operator-facing fix.
    """

    available: bool
    reason: str
    remediation: str | None = None


class Connector(ABC):  # REQ-842
    """Engine-specific catalog operations for one ``(engine, source_type)`` pair."""

    engine: str
    source_type: str
    # The PRIMARY reach mode — the default federate() picks and CatalogEntry records. Kept for
    # back-compat; the full self-description is ``reach_modes`` (REQ-947).
    mechanism: Mechanism
    # The FULL set of reach modes this connector supports (REQ-947/951): a source may be reachable
    # more than one way — e.g. a relational source both attached by the engine AND read by Provisa's
    # native driver → {ATTACH_RW, DIRECT}. Empty ⇒ derive {mechanism}. Read via ``reach_modes``; the
    # planner/source UI choose a mode from it instead of assuming the single ``mechanism``.
    mechanisms: frozenset[Mechanism] = frozenset()
    # MATERIALIZATION is orthogonal to the reach mechanism (REQ-951): any readable source can be
    # landed into a store (for latency/CDC/freshness, or — for FETCH/DIRECT sources the engine can't
    # read live — as the ONLY way the engine sees it). Nearly always True; False only for a source
    # that must never be cached.
    materializable: bool = True
    key: str = (
        ""  # stable identity for probe reports + override strike-list (falls back to source_type)
    )
    # DuckDB connectors backed by a loadable extension declare it (REQ-899): ``extension`` is the name
    # to INSTALL/LOAD before an attach; ``install_from_community`` selects the community vs core registry;
    # ``probe_symbol`` is the scanner/attach function whose presence after LOAD proves the extension is
    # installed — the load-only probe (never opens a live source). None where no extension is needed.
    extension: str | None = None
    install_from_community: bool = False
    probe_symbol: str | None = None
    # REQ-846: connectors are what the engine can REACH; this flags a reachable backend that can ALSO
    # serve as the engine's MATERIALIZED STORE — i.e. it can be read back (this connector) AND has a
    # write face to land into (store_writer). The engine's usable-store set is the connectors so
    # flagged. False for a reach-only source (no write face, or not a landing target).
    materialized_store: bool = False
    # Non-core shared libraries this connector's extension links at runtime, each a structured
    # ``RuntimeDep(lib, provider)`` (REQ-948). Empty for core contrib (postgres_fdw/file_fdw) with no
    # external dependency. Documents the packaging surface and feeds the capability report + dropdown.
    runtime_deps: tuple[RuntimeDep, ...] = ()

    @property
    def reach_modes(self) -> frozenset[Mechanism]:
        """The reach modes this connector supports (REQ-947) — the declared ``mechanisms`` set, or
        ``{mechanism}`` when none is declared. The complete self-description of how the engine can
        offer this source; the planner/source UI pick a mode from it."""
        return self.mechanisms or frozenset({self.mechanism})

    @property
    def reads_in_place(self) -> bool:
        """True iff the engine reads this source LIVE in place (attach or scan) — so it MUST NOT be
        landed (REQ-951). False for DIRECT/FETCH, which Provisa reads and lands as a replica."""
        return bool(self.reach_modes & LIVE_IN_PLACE)

    @property
    def operator_deps(self) -> tuple[RuntimeDep, ...]:
        """Runtime deps the OPERATOR must install (BYO). Non-empty ⇒ the source is offered but shown
        disabled with remediation until the deps + probe confirm availability (REQ-948)."""
        return tuple(d for d in self.runtime_deps if d.provider is DriverProvider.OPERATOR)

    @abstractmethod
    def capability(self) -> Capability: ...

    @abstractmethod
    def details(self, source: Source) -> dict:
        """The engine+source_type specific catalog payload for ``source``."""

    async def probe(self, fetch) -> ProbeResult:  # REQ-904
        """Report whether this connector's dependency actually functions on the live engine.

        ``fetch(sql)`` is an async callable returning rows. Default: available — an attach connector
        with no external dependency (Trino catalog, DuckDB scanner, land-into-self). Connectors that
        need an FDW/extension override this with a functional check.
        """
        del fetch  # base default needs no probe; overrides that do use it
        return ProbeResult(True, "no external dependency required")

    def catalog_entry(self, source: Source) -> CatalogEntry:  # REQ-842 catalog_add projection
        """Project a registry asset into its derived engine-catalog entry."""
        return CatalogEntry(
            name=source.id,
            engine=self.engine,
            source_type=self.source_type,
            mechanism=self.mechanism,
            details=self.details(source),
        )
