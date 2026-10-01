# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The benchmark setup contract's model (REQ-1911): what a loaded contract is.

``SetupError`` is raised for anything invalid and names the field. The parsing is in
``contract_parse``, the checks in ``contract_checks``, the join expansion in ``contract_joins``, and
``setup_contract`` loads and binds a contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

CONTRACT_VERSION = 1


DISTRIBUTION_KNOBS = (
    "fields",
    "filters",
    "rows",
    "joins_same_source",
    "joins_cross_source",
    "cache_repetition",
)


# How many earlier requests a client keeps so a later one can repeat it (memory per client).
MAX_LOOKBACK = 100_000


# The route knob: auto sends no hint (the deployment's own routing, what the optimistic test sends);
# direct and federated carry `route=direct` / `route=federated` (direct to the source, or through
# the engine); mixed: either, by probability. Whether a table is read live or from its replica is
# not a route: it is the operator's replication setting (``Replication``), which no request can
# choose.
ROUTE_MODES = ("auto", "direct", "federated", "mixed")


REPLICATION_SETTINGS = ("live", "replica")
CREDENTIAL_KINDS = ("password", "token")


LOAD_MODES = ("closed_loop", "open_loop")


TRANSPORT_NAME_FIELDS = ("cypher", "graphql", "grpc", "rest", "jsonapi")


class SetupError(ValueError):
    """The contract is invalid; the message names the field."""


# --------------------------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Distribution:
    kind: str
    params: Mapping[str, float | int]


@dataclass(frozen=True)
class Knob:
    probability: float
    distribution: Distribution


@dataclass(frozen=True)
class HostPort:
    host: str
    port: int


@dataclass(frozen=True)
class Endpoints:
    http_base_url: str
    pgwire: HostPort
    bolt: HostPort
    flight: HostPort
    grpc: HostPort


@dataclass(frozen=True)
class Credentials:
    mode: str  # "none" or "env"
    # kind: "password" (the variable holds the user's password: HTTP/gRPC/Flight log in for a
    # bearer token, pgwire and Bolt present the password) or "token" (the variable holds a ready
    # bearer token, a personal access token or an IdP token)
    kind: str | None
    user: str | None  # the login principal ("password"); None for a token
    env: str | None  # the variable that holds the secret; never the secret


@dataclass(frozen=True)
class Role:
    role: str
    weight: float


@dataclass(frozen=True)
class Domain:
    """Where an equality filter's value on a column is drawn from: a set of values, or an
    inclusive integer range (``values`` is then empty)."""

    kind: str  # "set" | "int_range"
    values: tuple[str | int | float, ...]
    low: int
    high: int


@dataclass(frozen=True)
class Column:
    name: str  # the registered column name
    domain: Domain | None  # where a filter's value is drawn from; a column with one is filterable
    # What each transport calls the column (sql, cypher, graphql, grpc, rest, jsonapi): resolved
    # from the deployment, empty until bound. A language missing here does not expose the column.
    names: Mapping[str, str]

    @property
    def filterable(self) -> bool:
        return self.domain is not None


@dataclass(frozen=True)
class Replication:
    """The setting a run is measured under: live (reads go to the source) or replica (reads come
    from the copy the engine holds, refreshed every ``ttl_seconds``). An operator setting on the
    source or table; no request can choose it."""

    setting: str  # "live" | "replica"
    ttl_seconds: int  # the replica's refresh TTL; 0 for live


@dataclass(frozen=True)
class Table:
    name: str  # the registered table name
    schema: str  # the registered schema
    weight: float
    base_column: str
    # Each transport's spelling of the table, resolved from the deployment (None until bound; a
    # None after binding means the transport does not expose the table).
    sql: str | None
    cypher_label: str | None
    cypher_var: str | None
    graphql_field: str | None
    grpc_type_name: str | None
    rest_path: str | None
    jsonapi_path: str | None
    jsonapi_type: str | None
    columns: tuple[Column, ...]
    replication: Replication | None = None  # None: the source's

    def has(self, language: str) -> bool:
        """Whether the deployment exposes the table in ``language`` (sql, cypher, graphql, grpc,
        rest or jsonapi)."""
        return {
            "sql": self.sql is not None,
            "cypher": self.cypher_label is not None,
            "graphql": self.graphql_field is not None,
            "grpc": self.grpc_type_name is not None,
            "rest": self.rest_path is not None,
            "jsonapi": self.jsonapi_path is not None,
        }[language]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


@dataclass(frozen=True)
class Join:
    left_source: str
    left_table: str
    left_column: str
    right_source: str
    right_table: str
    right_column: str
    # How GraphQL and Cypher traverse the registered relationship left -> right, resolved from the
    # deployment; None: that language does not expose it (SQL joins either way round).
    graphql_field: str | None
    cypher_rel: str | None


@dataclass(frozen=True)
class JoinStep:
    """One join of a request: ``child`` joined onto member ``parent`` (0 is the request's own
    table, step k adds member k)."""

    parent: int
    kind: str  # "same" | "cross"
    child_source: str
    child_table: str
    parent_column: str
    child_column: str
    forward: bool  # parent is the join's left table (the declared direction)
    graphql_field: str | None
    cypher_rel: str | None


@dataclass(frozen=True)
class Source:
    tables: tuple[Table, ...]
    joins: tuple[Join, ...]  # same-source
    replication: Replication  # what the run is measured under for this source's tables
    container: str | None  # docker container to sample for source CPU; None: not monitored
    kind: str | None = (
        None  # the source's registered type (postgresql, mongodb, ...); set when bound
    )


@dataclass(frozen=True)
class RouteMode:
    mode: str
    federated_probability: float


@dataclass(frozen=True)
class Knobs:
    distributions: Mapping[str, Knob]
    source_weights: Mapping[str, float]
    route: Mapping[str, RouteMode]
    cache_probability: float  # share of requests that opt into the response cache


@dataclass(frozen=True)
class TransportOverride:
    knobs: Mapping[str, Knob]
    source_weights: Mapping[str, float] | None
    cache_probability: float | None
    route: Mapping[str, RouteMode] | None


@dataclass(frozen=True)
class Load:
    mode: str
    steps: tuple[int, ...]  # connections per step; open loop: the one step's connections
    window_s: float
    processes: int | None  # None: half the cores (``half_cores``)
    rate_per_s: float | None
    # requests sent one at a time with the stats header per HTTP transport, for the per-request
    # time split; 0: no profile
    profile_requests: int
    # seconds of no load after the ramp, to measure what the deployment costs idle (the refresh cost
    # of replicated sources); 0 skips it
    idle_window_s: int


@dataclass(frozen=True)
class Setup:
    name: str
    endpoints: Endpoints
    credentials: Credentials
    roles: tuple[Role, ...]
    sources: Mapping[str, Source]
    cross_source_joins: tuple[Join, ...]
    base: Mapping[str, int]
    knobs: Knobs
    transports: Mapping[str, TransportOverride]
    load: Load
    seed: int

    def knob(self, transport: str, name: str) -> Knob:
        """The distribution knob ``name`` as it applies to ``transport`` (its override, else the
        global one)."""
        return self.transports[transport].knobs.get(name, self.knobs.distributions[name])

    def source_weights(self, transport: str) -> Mapping[str, float]:
        """Source weights as they apply to ``transport`` (its override, else the global ones)."""
        override = self.transports[transport].source_weights
        return self.knobs.source_weights if override is None else override

    def route_mode(self, transport: str, source: str) -> RouteMode:
        """The route mode of ``source`` on ``transport`` (its override, else the global one)."""
        override = self.transports[transport].route
        return (self.knobs.route if override is None else override)[source]

    def cache_probability(self, transport: str) -> float:
        """Share of this transport's requests that opt into the response cache."""
        override = self.transports[transport].cache_probability
        return self.knobs.cache_probability if override is None else override


# transport -> the language its request is written in (the table spelling it needs)
TRANSPORT_LANGUAGE = {
    "graphql": "graphql",
    "data_sql": "sql",
    "cypher_http": "cypher",
    "pgwire": "sql",
    "flight_sql": "sql",
    "bolt": "cypher",
    "grpc": "grpc",
    "rest": "rest",
    "jsonapi": "jsonapi",
}


# Transports whose request language can express a join (gRPC, REST and JSON:API read one table).
JOIN_TRANSPORTS = ("graphql", "data_sql", "cypher_http", "pgwire", "flight_sql", "bolt")
