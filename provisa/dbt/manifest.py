# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3c1a52-8e0d-4b61-9a2e-5d1f0c7be941
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dbt source's spec: what dbt's manifest says, read into definitions (REQ-2228).

dbt goes on building. Provisa reads what its manifest declares -- the adapter, each model's
database, schema and relation, its description and compiled SQL, and the tests, snapshots, seeds
and macros dbt can run -- and holds that much, never a copy of the project and never a row.
"""

# Requirements: REQ-2228

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

#: dbt's own macro packages: what dbt dispatches to internally, none of it run on its own. A project
#: macro or one from an installed package (``dbt_utils``) is a macro a team calls.
_INTERNAL_PACKAGE = "dbt"


class NotAManifest(ValueError):
    """The document given is not a dbt manifest."""


@dataclass(frozen=True)
class DbtColumn:
    name: str
    description: str | None = None
    data_type: str | None = None


@dataclass(frozen=True)
class DbtModel:
    unique_id: str
    name: str
    database: str
    schema: str
    relation: str
    materialized: str
    description: str | None
    columns: tuple[DbtColumn, ...]
    #: The SQL dbt compiled for the model; None in a manifest written by ``dbt parse``.
    compiled_sql: str | None


@dataclass(frozen=True)
class DbtNode:
    """A test, a snapshot or a seed: what dbt selects by name."""

    unique_id: str
    name: str
    description: str | None


@dataclass(frozen=True)
class DbtMacro:
    name: str
    package: str
    description: str | None


@dataclass(frozen=True)
class DbtManifest:
    project: str
    adapter: str
    generated_at: str | None
    models: tuple[DbtModel, ...]
    tests: tuple[DbtNode, ...]
    snapshots: tuple[DbtNode, ...]
    seeds: tuple[DbtNode, ...]
    macros: tuple[DbtMacro, ...]

    @property
    def databases(self) -> list[str]:
        """Each database the manifest's models are built in, in name order."""
        return sorted({m.database for m in self.models})

    def model_of(self, schema: str, relation: str) -> DbtModel | None:
        """The model built at ``schema``.``relation``."""
        return next((m for m in self.models if m.schema == schema and m.relation == relation), None)


def _text(value: Any) -> str | None:  # any JSON value a manifest holds
    return value if isinstance(value, str) and value else None


def _column(entry: dict) -> DbtColumn:
    return DbtColumn(
        name=entry["name"],
        description=_text(entry.get("description")),
        data_type=_text(entry.get("data_type")),
    )


def _model(node: dict) -> DbtModel:
    return DbtModel(
        unique_id=node["unique_id"],
        name=node["name"],
        database=node["database"],
        schema=node["schema"],
        relation=node.get("alias") or node["name"],
        materialized=(node.get("config") or {}).get("materialized", ""),
        description=_text(node.get("description")),
        columns=tuple(_column(c) for c in (node.get("columns") or {}).values()),
        compiled_sql=_text(node.get("compiled_code")) or _text(node.get("compiled_sql")),
    )


def _node(node: dict) -> DbtNode:
    return DbtNode(
        unique_id=node["unique_id"],
        name=node["name"],
        description=_text(node.get("description")),
    )


def _called_macro(macro: dict) -> bool:
    package = macro.get("package_name", "")
    return package != _INTERNAL_PACKAGE and not package.startswith(f"{_INTERNAL_PACKAGE}_")


def _of_type(nodes: list[dict], kind: str) -> list[dict]:
    return sorted((n for n in nodes if n.get("resource_type") == kind), key=lambda n: n["name"])


def parse_manifest(document: str | bytes | dict) -> DbtManifest:
    """The definitions ``document``, a dbt ``manifest.json``, declares; refused when it is none."""
    try:
        raw = json.loads(document) if isinstance(document, str | bytes) else document
    except ValueError as exc:
        raise NotAManifest(f"not a dbt manifest: {exc}") from exc
    metadata = raw.get("metadata") if isinstance(raw, dict) else None
    if not isinstance(metadata, dict) or "/manifest/" not in str(
        metadata.get("dbt_schema_version", "")
    ):
        raise NotAManifest("not a dbt manifest: it declares no manifest schema version")
    nodes = list((raw.get("nodes") or {}).values())
    return DbtManifest(
        project=metadata.get("project_name") or "",
        adapter=metadata.get("adapter_type") or "",
        generated_at=metadata.get("generated_at"),
        models=tuple(_model(n) for n in _of_type(nodes, "model")),
        tests=tuple(_node(n) for n in _of_type(nodes, "test")),
        snapshots=tuple(_node(n) for n in _of_type(nodes, "snapshot")),
        seeds=tuple(_node(n) for n in _of_type(nodes, "seed")),
        macros=tuple(
            DbtMacro(
                name=m["name"], package=m["package_name"], description=_text(m.get("description"))
            )
            for m in sorted((raw.get("macros") or {}).values(), key=lambda m: m["name"])
            if _called_macro(m)
        ),
    )


def to_definitions(manifest: DbtManifest) -> str:
    """What is held of a manifest, as JSON: its definitions, not the document it came in."""
    return json.dumps(asdict(manifest))


def from_definitions(text: str) -> DbtManifest:
    """The manifest :func:`to_definitions` wrote."""
    held = json.loads(text)
    return DbtManifest(
        project=held["project"],
        adapter=held["adapter"],
        generated_at=held["generated_at"],
        models=tuple(
            DbtModel(**{**m, "columns": tuple(DbtColumn(**c) for c in m["columns"])})
            for m in held["models"]
        ),
        tests=tuple(DbtNode(**n) for n in held["tests"]),
        snapshots=tuple(DbtNode(**n) for n in held["snapshots"]),
        seeds=tuple(DbtNode(**n) for n in held["seeds"]),
        macros=tuple(DbtMacro(**m) for m in held["macros"]),
    )
