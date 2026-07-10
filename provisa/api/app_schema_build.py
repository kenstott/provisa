# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Table/column shaping helpers for schema (re)build.

Pure-ish transforms over registered-table dicts: schema-config filtering,
domain uniqueness checks, naming-config resolution, GQL-remote arg injection,
and column-metadata synthesis. Reaches the app state singleton lazily.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml

from provisa.compiler.introspect import ColumnMetadata
from provisa.observability.ops_schema import OPS_TABLES as _OPS_TABLES
from provisa.compiler.type_map import OPS_PG_TO_PHYSICAL as _OPS_PG_TO_PHYSICAL

log = logging.getLogger(__name__)


def _filter_tables_by_schema_cfg(
    tables: list[dict],
    schema_cfg: dict,
    source_allowed_domains: dict[str, list[str]],
) -> list[dict]:
    """Filter registered tables based on schema visibility config and source domain restrictions."""
    if not schema_cfg.get("include_ops", True):
        tables = [t for t in tables if t.get("domain_id") != "ops"]
    elif not schema_cfg.get("include_metrics", True):
        tables = [
            t
            for t in tables
            if not (t.get("domain_id") == "ops" and t.get("table_name") == "metrics")
        ]

    if source_allowed_domains:
        tables = [
            t
            for t in tables
            if not source_allowed_domains.get(t["source_id"])
            or t.get("domain_id", "") in source_allowed_domains[t["source_id"]]
        ]

    return tables


def _assert_domain_table_unique(tables: list[dict]) -> None:
    """Raise if two tables share (domain_id, effective_name) — ambiguous for GraphQL/Cypher."""
    locs: dict[tuple[str, str], list[str]] = {}
    for t in tables:
        effective_name = t.get("alias") or t["table_name"]
        locs.setdefault((t["domain_id"], effective_name), []).append(
            f"{t['source_id']}.{t['schema_name']}"
        )
    dupes = {k: v for k, v in locs.items() if len(v) > 1}
    if dupes:
        detail = "; ".join(
            f"{dom}.{tbl} ← {sorted(srcs)}" for (dom, tbl), srcs in sorted(dupes.items())
        )
        raise RuntimeError(f"Duplicate domain+table registration (must be unique): {detail}")


def _resolve_naming_config(raw_config: dict | None) -> tuple[bool, dict | None]:
    """Load naming config from raw_config or disk. Returns (domain_prefix, resolved_raw_config)."""
    from provisa.api.app import state
    from provisa.compiler import naming as _naming

    domain_prefix = False
    if raw_config:
        domain_prefix = raw_config.get("naming", {}).get("domain_prefix", False)
        if raw_config.get("naming", {}).get("convention"):
            state.global_gql_naming_convention = raw_config["naming"]["convention"]
        if raw_config.get("naming", {}).get("sql_convention"):
            state.global_sql_naming_convention = raw_config["naming"]["sql_convention"]
    else:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                raw_config = yaml.safe_load(f)
            if isinstance(raw_config, dict):
                domain_prefix = raw_config.get("naming", {}).get("domain_prefix", False)
                if raw_config.get("naming", {}).get("convention"):
                    state.global_gql_naming_convention = raw_config["naming"]["convention"]
                if raw_config.get("naming", {}).get("sql_convention"):
                    state.global_sql_naming_convention = raw_config["naming"]["sql_convention"]
    # Single-domain mode: domain prefixing is meaningless with one domain — force it off.
    if raw_config and raw_config.get("naming", {}).get("use_domains") is False:
        domain_prefix = False
    _naming.configure(
        gql=state.global_gql_naming_convention,
        sql=state.global_sql_naming_convention,
    )
    return domain_prefix, raw_config


def _inject_gql_required_args(tables: list[dict], gql_remote_srcs: dict) -> None:
    """Inject required GQL args as native filter columns for graphql_remote tables."""
    if not gql_remote_srcs:
        return
    from provisa.compiler.naming import apply_sql_name as _asn

    _gql_req_args: dict[tuple, list[dict]] = {}
    for _reg in gql_remote_srcs.values():
        _sid = _reg.get("source_id", "")
        for _tbl in _reg.get("tables", []):
            _req = _tbl.get("required_args", [])
            if _req:
                _sql_tbl_name = _tbl.get("sql_name") or _asn(_tbl["name"])
                _gql_req_args[(_sid, _sql_tbl_name)] = _req
    if _gql_req_args:
        for _tbl in tables:
            _key = (_tbl["source_id"], _tbl["table_name"])
            _req = _gql_req_args.get(_key, [])
            for _arg in _req:
                _tbl.setdefault("columns", [])
                _tbl["columns"].append(
                    {
                        "name": _arg["name"],
                        "column_name": _arg["name"],
                        "visible_to": [],
                        "native_filter_type": "query_param",
                        "description": None,
                    }
                )


def _build_gql_object_columns(gql_remote_srcs: dict) -> dict[str, dict[str, list[str]]]:
    """Build gql_object_columns: {table_name: {col_name: [sub_field_names]}} for JSON extraction."""
    _gql_object_cols: dict[str, dict[str, list[str]]] = {}
    for _reg in gql_remote_srcs.values():
        for _tbl in _reg.get("tables", []):
            _tbl_obj: dict[str, list[str]] = {}
            for _col in _tbl.get("columns", []):
                _sub = _col.get("gql_object_fields")
                if _sub:
                    _tbl_obj[_col["name"]] = _sub
            if _tbl_obj:
                _gql_object_cols[_tbl["name"]] = _tbl_obj
    return _gql_object_cols


def _synthesize_column_metadata(
    tables: list[dict],
    col_types_converted: dict[int, list[ColumnMetadata]],
    gql_remote_srcs: dict,
) -> None:
    """Synthesize ColumnMetadata for ops, provisa-admin, graphql_remote, and govdata tables."""
    from provisa.api.app import state
    from provisa.api.startup_seed import _OPS_VIEWS

    # Ops tables: static columns when the engine introspection returns empty
    _ops_static_cols: dict[str, list[ColumnMetadata]] = {
        tbl_name: [
            ColumnMetadata(
                column_name=col_name,
                data_type=_OPS_PG_TO_PHYSICAL.get(pg_type, "VARCHAR").lower(),
                is_nullable=not is_pk,
            )
            for col_name, pg_type, is_pk in cols
        ]
        for tbl_name, cols in _OPS_TABLES.items()
    }
    for view_name, cols, _ in _OPS_VIEWS:
        _ops_static_cols[view_name] = [
            ColumnMetadata(
                column_name=col_name,
                data_type=_OPS_PG_TO_PHYSICAL.get(pg_type, "VARCHAR").lower(),
                is_nullable=not is_pk,
            )
            for col_name, pg_type, is_pk in cols
        ]
    for _tbl in tables:
        if _tbl["source_id"] != "provisa-otel":
            continue
        _vname = _tbl["table_name"]
        if _vname not in _ops_static_cols:
            continue
        _tid = _tbl["id"]
        if not col_types_converted.get(_tid):
            col_types_converted[_tid] = _ops_static_cols[_vname]

    # provisa-admin meta tables (no provisa_admin the engine catalog)
    _pg_to_physical: dict[str, str] = {
        "text": "varchar",
        "character varying": "varchar",
        "varchar": "varchar",
        "integer": "integer",
        "bigint": "bigint",
        "smallint": "smallint",
        "boolean": "boolean",
        "double precision": "double",
        "float8": "double",
        "numeric": "double",
        "date": "date",
        "timestamp": "timestamp",
        "timestamp without time zone": "timestamp",
        "json": "json",
        "jsonb": "json",
    }
    for _tbl in tables:
        if _tbl["source_id"] != "provisa-admin":
            continue
        _tid = _tbl["id"]
        if col_types_converted.get(_tid):
            continue
        _cols = _tbl.get("columns", [])
        if not _cols:
            continue
        col_types_converted[_tid] = [
            ColumnMetadata(
                column_name=c["column_name"],
                data_type=_pg_to_physical.get(c.get("data_type") or "text", "varchar"),
                is_nullable=not c.get("is_primary_key", False),
            )
            for c in _cols
        ]

    # graphql_remote tables (no the engine catalog)
    if gql_remote_srcs:
        _provisa_to_physical = {
            "text": "varchar",
            "integer": "integer",
            "numeric": "double",
            "boolean": "boolean",
            "jsonb": "json",
        }
        _tbl_lookup = {(t["source_id"], t["table_name"]): t["id"] for t in tables}
        for _reg in gql_remote_srcs.values():
            _sid = _reg.get("source_id", "")
            for _tbl in _reg.get("tables", []):
                _tid = _tbl_lookup.get((_sid, _tbl["name"]))
                if _tid is not None and _tid not in col_types_converted:
                    col_types_converted[_tid] = [
                        ColumnMetadata(
                            column_name=c["name"],
                            data_type=_provisa_to_physical.get(c.get("type", "text"), "varchar"),
                            is_nullable=True,
                        )
                        for c in _tbl.get("columns", [])
                    ]

    # govdata tables from registered columns
    for _tbl in tables:
        if state.source_types.get(_tbl["source_id"]) != "govdata":
            continue
        _tid = _tbl["id"]
        if col_types_converted.get(_tid):
            continue
        _cols = _tbl.get("columns", [])
        if not _cols:
            log.warning(
                "govdata table %s.%s has no registered columns — skipping",
                _tbl.get("schema_name", ""),
                _tbl.get("table_name", ""),
            )
            continue
        col_types_converted[_tid] = [
            ColumnMetadata(
                column_name=c["column_name"],
                data_type=c.get("data_type") or "varchar",
                is_nullable=True,
            )
            for c in _cols
        ]
