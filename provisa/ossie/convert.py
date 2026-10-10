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
import re
from typing import TypeVar

from dataclasses import dataclass, field

import yaml

from provisa.core.models import ProvisaConfig, Table

OSSIE_VERSION = "0.2.0.dev0"
#: The commit of apache/ossie whose core-spec this adapter was written and checked against. The
#: spec's version string did not change across its breaking changes of September 2026 (the
#: ``semantic_model`` wrapper removed, the datatype vocabulary enumerated), so the version alone
#: does not say which shape a document has.
OSSIE_SPEC_COMMIT = "642cb894b453807062ff93de599d0d8edacaad04"
_ANSI = "ANSI_SQL"
_PROVISA_VENDOR = "provisa"

#: Ossie's datatype vocabulary (core-spec/ossie-schema.json, ``DataType``).
OPAQUE = "Opaque"
DATATYPES: tuple[str, ...] = (
    "String",
    "Integer",
    "Decimal",
    "Float",
    "Boolean",
    "Date",
    "Time",
    "DateTime",
    "DateTimeTz",
    OPAQUE,
)

# Provisa/source column types -> Ossie datatype. A type that is not here is a known type outside
# Ossie's vocabulary and is written ``Opaque`` with the type itself in the provisa extension, as
# the spec directs ("use Opaque plus custom_extensions for a known type outside the portable
# vocabulary"); a column with no type recorded carries no datatype ("omit when unknown").
_DATATYPE_MAP: dict[str, str] = {
    "varchar": "String",
    "character varying": "String",
    "char": "String",
    "character": "String",
    "text": "String",
    "string": "String",
    "uuid": "String",
    "int": "Integer",
    "integer": "Integer",
    "int4": "Integer",
    "int8": "Integer",
    "bigint": "Integer",
    "smallint": "Integer",
    "tinyint": "Integer",
    "numeric": "Decimal",
    "decimal": "Decimal",
    "double": "Float",
    "double precision": "Float",
    "float": "Float",
    "real": "Float",
    "bool": "Boolean",
    "boolean": "Boolean",
    "date": "Date",
    "time": "Time",
    "timestamp": "DateTime",
    "timestamp without time zone": "DateTime",
    "datetime": "DateTime",
    "timestamptz": "DateTimeTz",
    "timestamp with time zone": "DateTimeTz",
}

_TIME_DATATYPES = {"Date", "DateTime", "DateTimeTz"}

# Ossie datatype -> the type a registration proposal carries. Decimal and Float carry no
# precision in Ossie, so none is proposed.
_PROPOSED_TYPE: dict[str, str] = {
    "String": "varchar",
    "Integer": "bigint",
    "Decimal": "decimal",
    "Float": "double",
    "Boolean": "boolean",
    "Date": "date",
    "Time": "time",
    "DateTime": "timestamp",
    "DateTimeTz": "timestamptz",
}


# A field's expression that is one column of its source, as written or double-quoted.
_BARE_COLUMN = re.compile(r'"?([A-Za-z_][A-Za-z0-9_$]*)"?')


class OssieExportRefused(ValueError):
    """A model that cannot be written as an Ossie document, said by name."""


def _map_datatype(data_type: str | None) -> str | None:
    """The Ossie datatype of a Provisa/source type: its member of the vocabulary, ``Opaque``
    for a type outside it, None for no type."""
    if data_type is None or not data_type.strip():
        return None
    base = data_type.strip().lower()
    # Strip parameterization, e.g. "varchar(255)" / "numeric(10,2)".
    if "(" in base:
        base = base.split("(", 1)[0].strip()
    return _DATATYPE_MAP.get(base, OPAQUE)


def _typed(entry: dict, data_type: str | None) -> str | None:
    """Write ``data_type`` on a field or metric entry as Ossie states it, and answer the Ossie
    datatype written. An ``Opaque`` entry keeps the type itself in the provisa extension."""
    datatype = _map_datatype(data_type)
    if datatype is None:
        return None
    entry["datatype"] = datatype
    if datatype == OPAQUE:
        entry["custom_extensions"] = [
            {"vendor_name": _PROVISA_VENDOR, "data": json.dumps({"data_type": data_type})}
        ]
    return datatype


def _columns(columns: str) -> list[str]:
    # A composite key's columns are one comma-separated, ordered list (core/models.py
    # Relationship).
    return [name.strip() for name in columns.split(",") if name.strip()]


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
        extension: dict = {}
        datatype = _typed(extension, col.data_type)
        if datatype in _TIME_DATATYPES:
            f["dimension"] = {"is_time": True}
        if col.description:
            f["description"] = col.description
        f.update(extension)  # datatype, and the provisa extension of an Opaque one
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

    One model per document, its properties at the document's root (the shape Ossie has had
    since apache/ossie #383): named ``name`` when given, else the config's first domain id,
    else "provisa" — deterministic for a given config. A model with no table is refused: an
    Ossie document has at least one dataset.
    """
    if not config.tables:
        raise OssieExportRefused(
            "The model has no table, and an Ossie document needs at least one dataset"
        )
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
                "from_columns": _columns(rel.source_column),
                "to_columns": _columns(rel.target_column),
            }
        )

    metrics: list[dict] = []
    for m in config.metrics:
        metric: dict = {"name": m.name, "expression": _expression(m.expression)}
        extension: dict = {}
        _typed(extension, m.datatype)
        if "datatype" in extension:
            metric["datatype"] = extension["datatype"]
        if m.description is not None:
            metric["description"] = m.description
        if m.ai_context is not None:
            # REQ-1319: AI-consumer definition text projects into Ossie ai_context.
            metric["ai_context"] = m.ai_context
        if "custom_extensions" in extension:
            metric["custom_extensions"] = extension["custom_extensions"]
        metrics.append(metric)

    return {
        "version": OSSIE_VERSION,
        "name": model_name,
        "datasets": datasets,
        "relationships": relationships,
        "metrics": metrics,
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


def _provisa_extension(entry: dict, path: str) -> dict:
    """What the provisa vendor slot of ``entry`` carries; empty when it has none."""
    for i, ext in enumerate(entry.get("custom_extensions") or []):
        epath = f"{path}.custom_extensions[{i}]"
        if not isinstance(ext, dict):
            raise ValueError(f"ossie import: {epath} must be a mapping")
        if ext.get("vendor_name") != _PROVISA_VENDOR:
            continue
        data = _require(ext, "data", epath, str)
        try:
            payload = json.loads(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"ossie import: {epath}.data is not valid JSON: {e}") from e
        if not isinstance(payload, dict):
            raise ValueError(f"ossie import: {epath}.data must be a JSON object")
        return payload
    return {}


def _proposed_type(entry: dict, path: str) -> str | None:
    """The type a proposal carries for a field or metric: the one its Ossie datatype stands
    for; for ``Opaque``, the type the provisa extension names, when there is one; None when
    the document states no datatype. A datatype outside Ossie's vocabulary is refused."""
    datatype = entry.get("datatype")
    if datatype is None:
        return None
    if datatype not in DATATYPES:
        raise ValueError(
            f"ossie import: {path}.datatype is {datatype!r}, which is not an Ossie datatype "
            f"({', '.join(DATATYPES)})"
        )
    if datatype == OPAQUE:
        named = _provisa_extension(entry, path).get("data_type")
        return named if isinstance(named, str) else None
    return _PROPOSED_TYPE[datatype]


def _ai_context(entry: dict, path: str) -> str | None:
    """Ossie's ai_context is text, or an object whose ``instructions`` is the text."""
    context = entry.get("ai_context")
    if context is None or isinstance(context, str):
        return context
    if not isinstance(context, dict):
        raise ValueError(f"ossie import: {path}.ai_context must be text or a mapping")
    instructions = context.get("instructions")
    return instructions if isinstance(instructions, str) else None


def _parse_dataset(ds: object, path: str) -> dict:
    if not isinstance(ds, dict):
        raise ValueError(f"ossie import: {path} must be a mapping")
    name = _require(ds, "name", path, str)
    source = _require(ds, "source", path, str)
    parts = str(source).split(".")
    if len(parts) < 3:
        raise ValueError(
            f"ossie import: {path}.source is {source!r}; Provisa registers a table by its "
            "source, schema and table name, so it needs three parts, 'source.schema.table' "
            "(a query, or a name of fewer parts, cannot be registered)"
        )
    source_id, schema_name, table_name = parts[0], parts[1], ".".join(parts[2:])

    columns: list[dict] = []
    not_importable: list[dict] = []
    primary_key = ds.get("primary_key") or []
    if not isinstance(primary_key, list):
        raise ValueError(f"ossie import: {path}.primary_key must be a list")
    for i, f in enumerate(ds.get("fields") or []):
        fpath = f"{path}.fields[{i}]"
        if not isinstance(f, dict):
            raise ValueError(f"ossie import: {fpath} must be a mapping")
        field_name = _require(f, "name", fpath, str)
        expression = _ansi_expression(_require(f, "expression", fpath, dict), f"{fpath}.expression")
        column = _BARE_COLUMN.fullmatch(expression.strip())
        if column is None:
            # A computation is not a column of the source, and a registered table holds
            # columns: it is named to the reviewer and proposed as nothing.
            not_importable.append({"name": field_name, "expression": expression})
            continue
        col_name = column.group(1)
        columns.append(
            {
                "name": col_name,
                # A field that renames its column keeps its own name as the column's alias.
                "alias": field_name if field_name != col_name else None,
                "datatype": _proposed_type(f, fpath),
                "description": f.get("description"),
                "is_primary_key": field_name in primary_key or col_name in primary_key,
            }
        )

    proposal: dict = {
        "name": name,
        "table_name": table_name,
        "schema_name": schema_name,
        "source_id": source_id,
        "description": ds.get("description"),
        "columns": columns,
        "not_importable": not_importable,
        "primary_key": list(primary_key),
        "unique_keys": [list(uk) for uk in (ds.get("unique_keys") or [])],
    }

    # REQ-1320: round-trip the provisa modeling metadata slot.
    slot = _provisa_extension(ds, path)
    if "modeling_role" in slot or "modeling_history" in slot:
        proposal["modeling_role"] = slot.get("modeling_role")
        proposal["modeling_history"] = slot.get("modeling_history")
    return proposal


def parse_ossie_model(doc: object) -> OssieImport:
    """REQ-1316: validate an Ossie document and return registration PROPOSALS.

    Hard errors name the path of the problem. Ingests DEFINITIONS only, never data.
    """
    if not isinstance(doc, dict):
        raise ValueError(f"ossie import: document must be a mapping, got {type(doc).__name__}")
    if "semantic_model" in doc:
        raise ValueError(
            "ossie import: this document wraps its model in $.semantic_model, which predates "
            "Ossie's flat document shape; move the model's name, datasets, relationships and "
            "metrics to the document's root (one model per document)"
        )
    version = _require(doc, "version", "$", str)
    if version != OSSIE_VERSION:
        raise ValueError(
            f"ossie import: $.version is {version!r}; this reads Ossie {OSSIE_VERSION}"
        )
    model = doc
    mpath = "$"
    model_name = str(_require(model, "name", mpath, str))
    datasets = _require(model, "datasets", mpath, list)
    if not datasets:
        raise ValueError("ossie import: $.datasets must contain at least one dataset")

    tables = [_parse_dataset(ds, f"{mpath}.datasets[{i}]") for i, ds in enumerate(datasets)]

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
                "datatype": _proposed_type(m, kpath),
                "description": m.get("description"),
                "ai_context": _ai_context(m, kpath),
            }
        )

    return OssieImport(
        model_name=model_name,
        tables=tables,
        relationships=relationships,
        metrics=metrics,
    )
