# Copyright (c) 2026 Kenneth Stott
# Canary: a6a10edf-18bd-446b-adbc-21bfffb54c72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Parses the setup contract's YAML sections into the model (REQ-1911). Every error names its path."""

from __future__ import annotations

import math
from collections.abc import Collection, Mapping
from typing import Any

from contract_model import (
    CREDENTIAL_KINDS,
    DISTRIBUTION_KNOBS,
    LOAD_MODES,
    REPLICATION_SETTINGS,
    ROUTE_MODES,
    Column,
    Credentials,
    Distribution,
    Domain,
    Endpoints,
    HostPort,
    Join,
    Knob,
    Knobs,
    Load,
    Replication,
    Role,
    RouteMode,
    SetupError,
    Source,
    Table,
    TransportOverride,
)

_WEIGHT_TOLERANCE = 1e-9


# --------------------------------------------------------------------------------------------
# Parsing helpers — every error names its path
# --------------------------------------------------------------------------------------------


def _mapping(
    value: Any, path: str, *, required: tuple[str, ...], optional: tuple[str, ...] = ()
) -> dict:
    if not isinstance(value, dict):
        raise SetupError(f"{path}: must be a mapping")
    allowed = (*required, *optional)
    for key in value:
        if key not in allowed:
            raise SetupError(f"{path}: unknown key {key!r} (allowed: {', '.join(allowed)})")
    for key in required:
        if key not in value:
            raise SetupError(f"{path}.{key}: required" if path else f"{key}: required")
    return value


def _join(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _int(value: Any, path: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SetupError(f"{path}: value must be an integer, got {value!r}")
    if value < minimum:
        raise SetupError(f"{path}: value must be >= {minimum}, got {value}")
    return value


def _number(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SetupError(f"{path}: must be a number, got {value!r}")
    return float(value)


def _str(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise SetupError(f"{path}: must be a non-empty string")
    return value


def _bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise SetupError(f"{path}: must be true or false, got {value!r}")
    return value


def _list(value: Any, path: str) -> list:
    if not isinstance(value, list):
        raise SetupError(f"{path}: must be a list")
    return value


def _probability(value: Any, path: str) -> float:
    p = _number(value, path)
    if not 0 <= p <= 1:
        raise SetupError(f"{path}: must be between 0 and 1, got {p}")
    return p


def _weights_sum_to_one(weights: Collection[float], path: str) -> None:
    total = math.fsum(weights)
    if abs(total - 1) > _WEIGHT_TOLERANCE:
        raise SetupError(f"{path}: weights sum to {total:g}, must sum to 1")


def _resolve(value: Any, path: str, environ: Mapping[str, str]) -> str:
    """A string, or ``{env: NAME}`` read from the environment."""
    if isinstance(value, dict):
        _mapping(value, path, required=("env",))
        name = _str(value["env"], f"{path}.env")
        if name not in environ:
            raise SetupError(f"{path}: environment variable {name} is not set")
        return _str(environ[name], f"{path} (from {name})")
    return _str(value, path)


# --------------------------------------------------------------------------------------------
# Distributions and knobs
# --------------------------------------------------------------------------------------------

# kind -> (required params)
_DISTRIBUTIONS: dict[str, tuple[str, ...]] = {
    "constant": ("value",),
    "uniform": ("min", "max"),
    "zipf": ("exponent", "max"),
    "geometric": ("p", "max"),
}


def _distribution(raw: Any, path: str) -> Distribution:
    if not isinstance(raw, dict):
        raise SetupError(f"{path}: must be a mapping")
    if "kind" not in raw:
        raise SetupError(f"{path}.kind: required")
    kind = raw["kind"]
    if kind not in _DISTRIBUTIONS:
        raise SetupError(
            f"{path}: unknown distribution kind {kind!r} (known: {', '.join(_DISTRIBUTIONS)})"
        )
    for key in raw:
        if key != "kind" and key not in _DISTRIBUTIONS[kind]:
            raise SetupError(
                f"{path}: unknown key {key!r} for {kind} (allowed: {', '.join(_DISTRIBUTIONS[kind])})"
            )
    for key in _DISTRIBUTIONS[kind]:
        if key not in raw:
            raise SetupError(f"{path}: {kind} requires {key!r}")
    params: dict[str, float | int] = {}
    if kind == "constant":
        params["value"] = _int(raw["value"], path)
    elif kind == "uniform":
        params["min"] = _int(raw["min"], path)
        params["max"] = _int(raw["max"], path)
        if params["min"] > params["max"]:
            raise SetupError(f"{path}: min must be <= max ({params['min']} > {params['max']})")
    elif kind == "zipf":
        params["exponent"] = _number(raw["exponent"], path)
        if params["exponent"] <= 0:
            raise SetupError(f"{path}: exponent must be > 0, got {params['exponent']}")
        params["max"] = _int(raw["max"], path, minimum=1)
    else:
        p = _number(raw["p"], path)
        if not 0 < p <= 1:
            raise SetupError(f"{path}: p must be in (0, 1], got {p}")
        params["p"] = p
        params["max"] = _int(raw["max"], path, minimum=1)
    return Distribution(kind, params)


def _knob(raw: Any, path: str) -> Knob:
    _mapping(raw, path, required=("probability", "distribution"))
    return Knob(
        _probability(raw["probability"], f"{path}.probability"),
        _distribution(raw["distribution"], f"{path}.distribution"),
    )


def _source_weights(raw: Any, path: str, sources: Collection[str]) -> dict[str, float]:
    if not isinstance(raw, dict):
        raise SetupError(f"{path}: must be a mapping")
    for source in raw:
        if source not in sources:
            raise SetupError(f"{path}: unknown source {source!r}")
    for source in sources:
        if source not in raw:
            raise SetupError(f"{path}: missing source {source!r}")
    weights = {s: _number(raw[s], f"{path}.{s}") for s in sources}
    for source, w in weights.items():
        if w < 0:
            raise SetupError(f"{path}.{source}: must be >= 0, got {w}")
    _weights_sum_to_one(list(weights.values()), path)
    return weights


def _per_source(raw: Any, path: str, sources: Collection[str]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SetupError(f"{path}: must be a mapping")
    for source in raw:
        if source not in sources:
            raise SetupError(f"{path}: unknown source {source!r}")
    for source in sources:
        if source not in raw:
            raise SetupError(f"{path}: missing source {source!r}")
    return raw


# --------------------------------------------------------------------------------------------
# Sections
# --------------------------------------------------------------------------------------------


def _host_port(raw: Any, path: str) -> HostPort:
    _mapping(raw, path, required=("host", "port"))
    return HostPort(_str(raw["host"], f"{path}.host"), _int(raw["port"], f"{path}.port", minimum=1))


def _endpoints(raw: Any, path: str, environ: Mapping[str, str]) -> Endpoints:
    _mapping(raw, path, required=("http", "pgwire", "bolt", "flight", "grpc"))
    http = _mapping(raw["http"], f"{path}.http", required=("base_url",))
    return Endpoints(
        http_base_url=_resolve(http["base_url"], f"{path}.http.base_url", environ),
        pgwire=_host_port(raw["pgwire"], f"{path}.pgwire"),
        bolt=_host_port(raw["bolt"], f"{path}.bolt"),
        flight=_host_port(raw["flight"], f"{path}.flight"),
        grpc=_host_port(raw["grpc"], f"{path}.grpc"),
    )


def _credentials(raw: Any, path: str, environ: Mapping[str, str]) -> Credentials:
    if not isinstance(raw, dict) or raw.get("mode") not in ("none", "env"):
        raise SetupError(f"{path}.mode: required, one of none, env")
    if raw["mode"] == "none":
        _mapping(raw, path, required=("mode",))
        return Credentials("none", None, None, None)
    _mapping(raw, path, required=("mode", "kind", "env"), optional=("user",))
    kind = raw["kind"]
    if kind not in CREDENTIAL_KINDS:
        raise SetupError(f"{path}.kind: must be one of {', '.join(CREDENTIAL_KINDS)}, got {kind!r}")
    if kind == "password" and "user" not in raw:
        raise SetupError(f"{path}.user: required for a password (the principal that logs in)")
    name = _str(raw["env"], f"{path}.env")
    if name not in environ:
        raise SetupError(f"{path}.env: environment variable {name} is not set")
    user = _str(raw["user"], f"{path}.user") if "user" in raw else None
    return Credentials("env", kind, user, name)


def _roles(raw: Any, path: str) -> tuple[Role, ...]:
    entries = _list(raw, path)
    if not entries:
        raise SetupError(f"{path}: at least one role")
    roles = []
    for i, entry in enumerate(entries):
        p = f"{path}[{i}]"
        _mapping(entry, p, required=("role", "weight"))
        weight = _number(entry["weight"], f"{p}.weight")
        if weight < 0:
            raise SetupError(f"{p}.weight: must be >= 0")
        roles.append(Role(_str(entry["role"], f"{p}.role"), weight))
    seen: set[str] = set()
    for r in roles:
        if r.role in seen:
            raise SetupError(f"{path}: duplicate role {r.role!r}")
        seen.add(r.role)
    _weights_sum_to_one([r.weight for r in roles], path)
    return tuple(roles)


def _domain(raw: Any, path: str) -> Domain:
    if not isinstance(raw, dict) or "kind" not in raw:
        raise SetupError(f"{path}.kind: required, one of set, int_range")
    kind = raw["kind"]
    if kind == "set":
        _mapping_keys(raw, path, ("kind", "values"))
        if "values" not in raw:
            raise SetupError(f"{path}: set requires 'values'")
        values = _list(raw["values"], f"{path}.values")
        if not values:
            raise SetupError(f"{path}.values: at least one value")
        seen: list[Any] = []
        for v in values:
            if isinstance(v, bool) or not isinstance(v, (str, int, float)):
                raise SetupError(f"{path}.values: each value must be a string or number, got {v!r}")
            if v in seen:
                raise SetupError(f"{path}.values: duplicate value {v!r}")
            seen.append(v)
        return Domain("set", tuple(values), 0, 0)
    if kind == "int_range":
        _mapping_keys(raw, path, ("kind", "min", "max"))
        for key in ("min", "max"):
            if key not in raw:
                raise SetupError(f"{path}: int_range requires {key!r}")
        low, high = _int(raw["min"], path), _int(raw["max"], path)
        if low > high:
            raise SetupError(f"{path}: min must be <= max ({low} > {high})")
        return Domain("int_range", (), low, high)
    raise SetupError(f"{path}: unknown domain kind {kind!r} (known: set, int_range)")


def _mapping_keys(raw: dict, path: str, allowed: tuple[str, ...]) -> None:
    for key in raw:
        if key not in allowed:
            raise SetupError(f"{path}: unknown key {key!r} (allowed: {', '.join(allowed)})")


def _column(raw: Any, path: str) -> Column:
    _mapping(raw, path, required=("name",), optional=("domain",))
    return Column(
        name=_str(raw["name"], f"{path}.name"),
        domain=_domain(raw["domain"], f"{path}.domain") if "domain" in raw else None,
        names={},
    )


def _optional_section(raw: dict, key: str, path: str, required: tuple[str, ...]) -> dict | None:
    return _mapping(raw[key], f"{path}.{key}", required=required) if key in raw else None


def _table(raw: Any, path: str) -> Table:
    _mapping(
        raw,
        path,
        required=("schema", "table", "weight", "base_column", "columns"),
        optional=("replication",),
    )
    columns = tuple(
        _column(c, f"{path}.columns[{i}]")
        for i, c in enumerate(_list(raw["columns"], f"{path}.columns"))
    )
    seen: set[str] = set()
    for c in columns:
        if c.name in seen:
            raise SetupError(f"{path}.columns: duplicate column {c.name!r}")
        seen.add(c.name)
    base = _str(raw["base_column"], f"{path}.base_column")
    if base not in seen:
        raise SetupError(f"{path}.base_column: base_column {base!r} is not a declared column")
    weight = _number(raw["weight"], f"{path}.weight")
    if weight < 0:
        raise SetupError(f"{path}.weight: must be >= 0")
    return Table(
        name=_str(raw["table"], f"{path}.table"),
        schema=_str(raw["schema"], f"{path}.schema"),
        weight=weight,
        base_column=base,
        sql=None,
        cypher_label=None,
        cypher_var=None,
        graphql_field=None,
        grpc_type_name=None,
        rest_path=None,
        jsonapi_path=None,
        jsonapi_type=None,
        columns=columns,
        replication=_replication(raw["replication"], f"{path}.replication")
        if "replication" in raw
        else None,
    )


def _split_ref(ref: Any, path: str) -> tuple[str, str]:
    text = _str(ref, path)
    table, dot, column = text.partition(".")
    if not dot or not table or not column:
        raise SetupError(f"{path}: must be 'table.column', got {text!r}")
    return table, column


def _check_ref(tables: Mapping[str, Table], table: str, column: str, path: str) -> None:
    if table not in tables:
        raise SetupError(f"{path}: unknown table {table!r}")
    if column not in {c.name for c in tables[table].columns}:
        raise SetupError(f"{path}: unknown column {column!r} of table {table!r}")


def _replication(raw: Any, path: str) -> Replication:
    _mapping(raw, path, required=("setting", "ttl_seconds"))
    setting = raw["setting"]
    if setting not in REPLICATION_SETTINGS:
        raise SetupError(
            f"{path}.setting: must be one of {', '.join(REPLICATION_SETTINGS)}, got {setting!r}"
        )
    ttl = _int(raw["ttl_seconds"], f"{path}.ttl_seconds")
    if setting == "replica" and ttl <= 0:
        raise SetupError(f"{path}: a replica needs its refresh TTL (ttl_seconds > 0)")
    if setting == "live" and ttl != 0:
        raise SetupError(f"{path}: a live read has no replica TTL (ttl_seconds must be 0)")
    return Replication(setting, ttl)


def _source(raw: Any, path: str) -> tuple[tuple[Table, ...], list[dict], Replication, str | None]:
    _mapping(raw, path, required=("tables", "joins", "replication"), optional=("monitor",))
    monitor = _optional_section(raw, "monitor", path, ("container",))
    container = _str(monitor["container"], f"{path}.monitor.container") if monitor else None
    tables = tuple(
        _table(t, f"{path}.tables[{i}]")
        for i, t in enumerate(_list(raw["tables"], f"{path}.tables"))
    )
    names = [t.name for t in tables]
    if len(set(names)) != len(names):
        raise SetupError(f"{path}.tables: duplicate table name")
    if tables:
        _weights_sum_to_one([t.weight for t in tables], f"{path}.tables")
    return (
        tables,
        _list(raw["joins"], f"{path}.joins"),
        _replication(raw["replication"], f"{path}.replication"),
        container,
    )


def _same_source_join(raw: Any, path: str, source: str, tables: Mapping[str, Table]) -> Join:
    _mapping(raw, path, required=("left", "right"))
    lt, lc = _split_ref(raw["left"], f"{path}.left")
    rt, rc = _split_ref(raw["right"], f"{path}.right")
    _check_ref(tables, lt, lc, f"{path}.left")
    _check_ref(tables, rt, rc, f"{path}.right")
    return Join(source, lt, lc, source, rt, rc, None, None)


def _cross_source_join(raw: Any, path: str, sources: Mapping[str, Source]) -> Join:
    _mapping(raw, path, required=("left", "right"))
    ends = []
    for side in ("left", "right"):
        text = _str(raw[side], f"{path}.{side}")
        source, colon, ref = text.partition(":")
        if not colon or source not in sources:
            raise SetupError(
                f"{path}.{side}: must be 'source:table.column' naming a declared source, got {text!r}"
            )
        table, column = _split_ref(ref, f"{path}.{side}")
        _check_ref({t.name: t for t in sources[source].tables}, table, column, f"{path}.{side}")
        ends.append((source, table, column))
    if ends[0][0] == ends[1][0]:
        raise SetupError(
            f"{path}: both ends are in source {ends[0][0]!r}; declare it under that source's joins"
        )
    (ls, lt, lc), (rs, rt, rc) = ends
    return Join(ls, lt, lc, rs, rt, rc, None, None)


def _route_mode(raw: Any, path: str) -> RouteMode:
    _mapping(raw, path, required=("mode", "federated_probability"))
    mode = raw["mode"]
    if mode not in ROUTE_MODES:
        raise SetupError(f"{path}.mode: must be one of {', '.join(ROUTE_MODES)}, got {mode!r}")
    p = _probability(raw["federated_probability"], f"{path}.federated_probability")
    expected = {"auto": 0.0, "direct": 0.0, "federated": 1.0}
    if mode in expected and p != expected[mode]:
        raise SetupError(
            f"{path}: mode {mode} requires federated_probability {expected[mode]:g}, got {p:g}"
        )
    if mode == "mixed" and not 0 < p < 1:
        raise SetupError(
            f"{path}: mode mixed requires federated_probability strictly between 0 and 1, got {p:g}"
        )
    return RouteMode(mode, p)


def _cache_probability(raw: Any, path: str) -> float:
    _mapping(raw, path, required=("probability",))
    return _probability(raw["probability"], f"{path}.probability")


def _knobs(raw: Any, path: str, sources: Collection[str]) -> Knobs:
    keys = (*DISTRIBUTION_KNOBS, "cache", "source_weights", "route")
    _mapping(raw, path, required=keys)
    route_raw = _per_source(raw["route"], f"{path}.route", sources)
    return Knobs(
        distributions={n: _knob(raw[n], f"{path}.{n}") for n in DISTRIBUTION_KNOBS},
        source_weights=_source_weights(raw["source_weights"], f"{path}.source_weights", sources),
        route={s: _route_mode(route_raw[s], f"{path}.route.{s}") for s in sources},
        cache_probability=_cache_probability(raw["cache"], f"{path}.cache"),
    )


def _override_route(raw: Any, path: str, sources: Collection[str]) -> dict[str, RouteMode]:
    modes = _per_source(raw, path, sources)
    return {s: _route_mode(modes[s], f"{path}.{s}") for s in sources}


def _transports(
    raw: Any, path: str, known: Collection[str], sources: Collection[str]
) -> dict[str, TransportOverride]:
    if not isinstance(raw, dict) or not raw:
        raise SetupError(f"{path}: must be a non-empty mapping")
    out: dict[str, TransportOverride] = {}
    for name, entry in raw.items():
        if name not in known:
            raise SetupError(f"{path}: unknown transport {name!r} (known: {', '.join(known)})")
        p = f"{path}.{name}"
        entry = _mapping(entry, p, required=(), optional=("knobs",))
        knobs_raw = entry.get("knobs", {})
        kp = f"{p}.knobs"
        _mapping(
            knobs_raw,
            kp,
            required=(),
            optional=(*DISTRIBUTION_KNOBS, "cache", "source_weights", "route"),
        )
        out[name] = TransportOverride(
            knobs={
                n: _knob(knobs_raw[n], f"{kp}.{n}") for n in DISTRIBUTION_KNOBS if n in knobs_raw
            },
            source_weights=(
                _source_weights(knobs_raw["source_weights"], f"{kp}.source_weights", sources)
                if "source_weights" in knobs_raw
                else None
            ),
            cache_probability=(
                _cache_probability(knobs_raw["cache"], f"{kp}.cache")
                if "cache" in knobs_raw
                else None
            ),
            route=(
                _override_route(knobs_raw["route"], f"{kp}.route", sources)
                if "route" in knobs_raw
                else None
            ),
        )
    return out


def _load_section(raw: Any, path: str) -> Load:
    if not isinstance(raw, dict) or raw.get("mode") not in LOAD_MODES:
        raise SetupError(f"{path}.mode: required, one of {', '.join(LOAD_MODES)}")
    if raw["mode"] == "closed_loop":
        _mapping(
            raw,
            path,
            required=(
                "mode",
                "steps",
                "window_s",
                "processes",
                "profile_requests",
                "idle_window_s",
            ),
        )
        rate = None
        steps = tuple(
            _int(s, f"{path}.steps", minimum=1) for s in _list(raw["steps"], f"{path}.steps")
        )
        if not steps:
            raise SetupError(f"{path}.steps: at least one step")
    else:
        _mapping(
            raw,
            path,
            required=(
                "mode",
                "rate_per_s",
                "connections",
                "window_s",
                "processes",
                "profile_requests",
                "idle_window_s",
            ),
        )
        rate = _number(raw["rate_per_s"], f"{path}.rate_per_s")
        if rate <= 0:
            raise SetupError(f"{path}.rate_per_s: must be > 0")
        # one step: the offered rate over this many persistent connections
        steps = (_int(raw["connections"], f"{path}.connections", minimum=1),)
    window = _number(raw["window_s"], f"{path}.window_s")
    if window <= 0:
        raise SetupError(f"{path}.window_s: must be > 0")
    procs = raw["processes"]
    processes = (
        None
        if procs == "half_cores"
        else _int(procs, f"{path}.processes (an integer or half_cores)", minimum=1)
    )
    profile = _int(raw["profile_requests"], f"{path}.profile_requests")
    idle = _int(raw["idle_window_s"], f"{path}.idle_window_s")
    return Load(raw["mode"], steps, window, processes, rate, profile, idle)
