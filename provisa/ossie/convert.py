# Copyright (c) 2026 Kenneth Stott
# Canary: 19194b89-b45e-479b-9775-12f00f493bbf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1316: Apache Ossie interchange boundary converter.

Bidirectional converter between the Provisa config model and the Apache Ossie
semantic-model interchange format (spec 0.2.0.dev0). Export covers datasets,
fields, relationships, and metrics; governance/RLS/lineage are Provisa-internal
and are NOT exported, except the ``provisa`` custom_extensions slot carrying
modeling_role/modeling_history (REQ-1320). Import produces registration
PROPOSALS only — definitions, never data — which the admin review screen
applies through the existing registration mutations.
"""

from __future__ import annotations

import json
from typing import TypeVar

from dataclasses import dataclass, field

import yaml

from provisa.core.models import ProvisaConfig, Table

OSSIE_VERSION = "0.2.0.dev0"
_ANSI = "ANSI_SQL"
_PROVISA_VENDOR = "provisa"

# Provisa/source column types → Ossie datatype strings. Unknown types pass
# through verbatim — mapping silently to a wrong type would corrupt the model.
_DATATYPE_MAP: dict[str, str] = {
    "varchar": "string",
    "character varying": "string",
    "char": "string",
    "character": "string",
    "text": "string",
    "string": "string",
    "uuid": "string",
    "int": "integer",
    "integer": "integer",
    "int4": "integer",
    "int8": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "tinyint": "integer",
    "numeric": "number",
    "decimal": "number",
    "double": "number",
    "double precision": "number",
    "float": "number",
    "real": "number",
    "bool": "boolean",
    "boolean": "boolean",
    "date": "date",
    "time": "time",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "timestamp with time zone": "timestamp",
    "timestamp without time zone": "timestamp",
    "datetime": "timestamp",
}

_TIME_DATATYPES = {"date", "timestamp"}


def _map_datatype(data_type: str | None) -> str | None:
    if data_type is None:
        return None
    base = data_type.strip().lower()
    # Strip parameterization, e.g. "varchar(255)" / "numeric(10,2)".
    if "(" in base:
        base = base.split("(", 1)[0].strip()
    return _DATATYPE_MAP.get(base, data_type)


def _dataset_name(table: Table) -> str:
    # The VIRTUAL name config relationships reference: alias when set, else table_name
    # (matches table_repo.find_by_table_name, the loader's resolver).
    return table.alias or table.table_name


def _expression(sql: str) -> dict:
    return {"dialects": [{"dialect": _ANSI, "expression": sql}]}


def _table_to_dataset(table: Table) -> dict:
    fields: list[dict] = []
    for col in table.columns:
        f: dict = {"name": col.name, "expression": _expression(col.name)}
        datatype = _map_datatype(col.data_type)
        if datatype is not None:
            f["datatype"] = datatype
        if datatype in _TIME_DATATYPES:
            f["dimension"] = {"is_time": True}
        if col.description:
            f["description"] = col.description
        fields.append(f)

    dataset: dict = {
        "name": _dataset_name(table),
        "source": f"{table.source_id}.{table.schema_name}.{table.table_name}",
    }
    primary_key = [c.name for c in table.columns if c.is_primary_key]
    if primary_key:
        dataset["primary_key"] = primary_key
    if table.unique_constraints:
        dataset["unique_keys"] = [list(uc.columns) for uc in table.unique_constraints]
    if table.description:
        dataset["description"] = table.description
    dataset["fields"] = fields
    if table.modeling_role or table.modeling_history:
        # REQ-1320: star/vault modeling metadata rides the provisa vendor slot so a
        # round-trip through Ossie preserves it.
        dataset["custom_extensions"] = [
            {
                "vendor_name": _PROVISA_VENDOR,
                "data": json.dumps(
                    {
                        "modeling_role": table.modeling_role,
                        "modeling_history": table.modeling_history,
                    },
                    sort_keys=True,
                ),
            }
        ]
    return dataset


def build_ossie_model(config: ProvisaConfig, name: str | None = None) -> dict:
    """REQ-1316: export the semantic slice of a ProvisaConfig as an Ossie document.

    One semantic_model, named ``name`` when given, else the config's first domain
    id, else "provisa" — deterministic for a given config.
    """
    model_name = name or (config.domains[0].id if config.domains else "provisa")

    datasets = [_table_to_dataset(t) for t in config.tables]

    # Config relationships reference tables by VIRTUAL name (alias or table_name);
    # datasets are named the same way, plus accept the raw table_name for aliased tables.
    name_to_dataset: dict[str, str] = {}
    for t in config.tables:
        name_to_dataset[_dataset_name(t)] = _dataset_name(t)
        name_to_dataset.setdefault(t.table_name, _dataset_name(t))

    relationships: list[dict] = []
    for rel in config.relationships:
        if not rel.target_table_id:
            # Computed (function-target) relationships have no dataset target — not
            # representable in Ossie; skipping is the defined export boundary.
            continue
        if rel.source_table_id not in name_to_dataset:
            raise ValueError(
                f"relationship {rel.id!r}: source table {rel.source_table_id!r} "
                "is not in config.tables"
            )
        if rel.target_table_id not in name_to_dataset:
            raise ValueError(
                f"relationship {rel.id!r}: target table {rel.target_table_id!r} "
                "is not in config.tables"
            )
        relationships.append(
            {
                "name": rel.alias or rel.id,
                "from": name_to_dataset[rel.source_table_id],
                "to": name_to_dataset[rel.target_table_id],
                "from_columns": [rel.source_column],
                "to_columns": [rel.target_column],
            }
        )

    metrics: list[dict] = []
    for m in config.metrics:
        metric: dict = {"name": m.name, "expression": _expression(m.expression)}
        if m.datatype is not None:
            metric["datatype"] = m.datatype
        if m.description is not None:
            metric["description"] = m.description
        if m.ai_context is not None:
            # REQ-1319: AI-consumer definition text projects into Ossie ai_context.
            metric["ai_context"] = m.ai_context
        metrics.append(metric)

    return {
        "version": OSSIE_VERSION,
        "semantic_model": [
            {
                "name": model_name,
                "datasets": datasets,
                "relationships": relationships,
                "metrics": metrics,
            }
        ],
    }


def ossie_yaml(config: ProvisaConfig, name: str | None = None) -> str:
    """REQ-1316/REQ-1321: the Ossie document as deterministic YAML (insertion-ordered)."""
    return yaml.safe_dump(build_ossie_model(config, name=name), sort_keys=False)


# ── import (definitions only, never data) ─────────────────────────────────────


@dataclass
class OssieImport:
    """REQ-1316: parsed import PROPOSALS — never applied here; the UI review screen
    registers them through the existing registration mutations."""

    model_name: str
    tables: list[dict] = field(default_factory=list)
    relationships: list[dict] = field(default_factory=list)
    metrics: list[dict] = field(default_factory=list)


_T = TypeVar("_T")


def _require(container: dict, key: str, path: str, typ: type[_T]) -> _T:
    if not isinstance(container, dict):
        raise ValueError(f"ossie import: {path} must be a mapping, got {type(container).__name__}")
    if key not in container:
        raise ValueError(f"ossie import: missing {path}.{key}")
    value = container[key]
    if not isinstance(value, typ):
        raise ValueError(
            f"ossie import: {path}.{key} must be {typ.__name__}, got {type(value).__name__}"
        )
    return value


def _ansi_expression(expr: object, path: str) -> str:
    if not isinstance(expr, dict):
        raise ValueError(f"ossie import: {path} must be a mapping")
    dialects = _require(expr, "dialects", path, list)
    for i, d in enumerate(dialects):
        if isinstance(d, dict) and d.get("dialect") == _ANSI:
            return str(_require(d, "expression", f"{path}.dialects[{i}]", str))
    raise ValueError(f"ossie import: {path}.dialects has no {_ANSI} entry")


def _parse_dataset(ds: object, path: str) -> dict:
    if not isinstance(ds, dict):
        raise ValueError(f"ossie import: {path} must be a mapping")
    name = _require(ds, "name", path, str)
    source = _require(ds, "source", path, str)
    parts = str(source).split(".")
    if len(parts) < 3:
        raise ValueError(
            f"ossie import: {path}.source must be 'database.schema.table', got {source!r}"
        )
    source_id, schema_name, table_name = parts[0], parts[1], ".".join(parts[2:])

    columns: list[dict] = []
    primary_key = ds.get("primary_key") or []
    if not isinstance(primary_key, list):
        raise ValueError(f"ossie import: {path}.primary_key must be a list")
    for i, f in enumerate(ds.get("fields") or []):
        fpath = f"{path}.fields[{i}]"
        if not isinstance(f, dict):
            raise ValueError(f"ossie import: {fpath} must be a mapping")
        col_name = _require(f, "name", fpath, str)
        columns.append(
            {
                "name": col_name,
                "datatype": f.get("datatype"),
                "description": f.get("description"),
                "is_primary_key": col_name in primary_key,
            }
        )

    proposal: dict = {
        "name": name,
        "table_name": table_name,
        "schema_name": schema_name,
        "source_id": source_id,
        "description": ds.get("description"),
        "columns": columns,
        "primary_key": list(primary_key),
        "unique_keys": [list(uk) for uk in (ds.get("unique_keys") or [])],
    }

    for i, ext in enumerate(ds.get("custom_extensions") or []):
        epath = f"{path}.custom_extensions[{i}]"
        if not isinstance(ext, dict):
            raise ValueError(f"ossie import: {epath} must be a mapping")
        if ext.get("vendor_name") != _PROVISA_VENDOR:
            continue
        # REQ-1320: round-trip the provisa modeling metadata slot.
        data = _require(ext, "data", epath, str)
        try:
            payload = json.loads(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"ossie import: {epath}.data is not valid JSON: {e}") from e
        proposal["modeling_role"] = payload.get("modeling_role")
        proposal["modeling_history"] = payload.get("modeling_history")
    return proposal


def parse_ossie_model(doc: object) -> OssieImport:
    """REQ-1316: validate an Ossie document and return registration PROPOSALS.

    Hard errors name the path of the problem. Ingests DEFINITIONS only, never data.
    """
    if not isinstance(doc, dict):
        raise ValueError(f"ossie import: document must be a mapping, got {type(doc).__name__}")
    _require(doc, "version", "$", str)
    models = _require(doc, "semantic_model", "$", list)
    if not models:
        raise ValueError("ossie import: semantic_model must contain at least one model")
    if len(models) > 1:
        raise ValueError(
            f"ossie import: expected exactly one semantic_model entry, got {len(models)}"
        )
    model = models[0]
    mpath = "semantic_model[0]"
    model_name = str(_require(model, "name", mpath, str))

    tables = [
        _parse_dataset(ds, f"{mpath}.datasets[{i}]")
        for i, ds in enumerate(model.get("datasets") or [])
    ]

    relationships: list[dict] = []
    for i, rel in enumerate(model.get("relationships") or []):
        rpath = f"{mpath}.relationships[{i}]"
        relationships.append(
            {
                "name": _require(rel, "name", rpath, str),
                "from": _require(rel, "from", rpath, str),
                "to": _require(rel, "to", rpath, str),
                "from_columns": list(_require(rel, "from_columns", rpath, list)),
                "to_columns": list(_require(rel, "to_columns", rpath, list)),
            }
        )

    metrics: list[dict] = []
    for i, m in enumerate(model.get("metrics") or []):
        kpath = f"{mpath}.metrics[{i}]"
        name = _require(m, "name", kpath, str)
        expression = _ansi_expression(_require(m, "expression", kpath, dict), f"{kpath}.expression")
        metrics.append(
            {
                "name": name,
                "expression": expression,
                "datatype": m.get("datatype"),
                "description": m.get("description"),
                "ai_context": m.get("ai_context"),
            }
        )

    return OssieImport(
        model_name=model_name,
        tables=tables,
        relationships=relationships,
        metrics=metrics,
    )
