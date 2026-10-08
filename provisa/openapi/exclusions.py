# Copyright (c) 2026 Kenneth Stott
# Canary: 598864d3-040d-4447-98c9-11df09722719
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a specification-built source does not offer (REQ-1957).

A published specification describes everything a remote API accepts, including what its vendor
has deprecated or what a deployment must never send. A source built from one carries EXCLUSIONS:

- an **operation**, by path and method: it is not offered for registration as a table or a
  command;
- a **request argument**, by name -- on one operation, or on every operation that has it: it is
  not among the operation's arguments, is not shown, and is never sent.

A brand ships its exclusions as part of its curation; an operator adds their own on any OpenAPI
source. Both are the same list applied the same way: to the specification, before anything reads
it, so every consumer -- the registration listing, the generated commands, the call that is sent
-- sees a specification in which the excluded things do not exist.

An exclusion that names something the specification does not have fails by name: a list that has
gone stale against a newer specification is noticed, not silently ineffective."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

# Requirements: REQ-1957

_METHODS = ("get", "put", "post", "delete", "patch", "head", "options", "trace")


class ExclusionError(ValueError):
    """An exclusion list that cannot be applied: malformed, or naming something the
    specification does not have."""


@dataclass(frozen=True)
class ExcludedOperation:
    method: str  # lower case
    path: str

    def __str__(self) -> str:
        return f"{self.method.upper()} {self.path}"


@dataclass(frozen=True)
class ExcludedArgument:
    name: str
    # None: on every operation that has the argument.
    operation: ExcludedOperation | None = None

    def __str__(self) -> str:
        where = "every operation" if self.operation is None else str(self.operation)
        return f"{self.name} on {where}"


@dataclass(frozen=True)
class Exclusions:
    operations: tuple[ExcludedOperation, ...] = ()
    arguments: tuple[ExcludedArgument, ...] = field(default=())

    def __bool__(self) -> bool:
        return bool(self.operations or self.arguments)

    @classmethod
    def parse(cls, raw: Any, *, who: str) -> "Exclusions":
        """Exclusions as written in a brand's file or a source's config::

            {"operations": [{"method": "POST", "path": "/v1/tokens"}],
             "arguments":  [{"name": "source", "method": "POST", "path": "/v1/customers"},
                            {"name": "card"}]}

        Keys beginning with ``_`` and a ``why`` on any entry are notes to the reader."""
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ExclusionError(f"{who}: exclusions must be a mapping, got {type(raw).__name__}")
        unknown = sorted(
            k for k in raw if not k.startswith("_") and k not in ("operations", "arguments")
        )
        if unknown:
            raise ExclusionError(f"{who}: exclusions has unknown key(s) {', '.join(unknown)}")
        operations = tuple(
            _operation(entry, who=who, what="an excluded operation")
            for entry in raw.get("operations") or ()
        )
        arguments = []
        for entry in raw.get("arguments") or ():
            if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
                raise ExclusionError(f"{who}: an excluded argument needs a name: {entry!r}")
            scoped = "method" in entry or "path" in entry
            arguments.append(
                ExcludedArgument(
                    entry["name"],
                    _operation(entry, who=who, what=f"excluded argument {entry['name']!r}")
                    if scoped
                    else None,
                )
            )
        return cls(operations, tuple(arguments))


def _operation(entry: Any, *, who: str, what: str) -> ExcludedOperation:
    if not isinstance(entry, dict):
        raise ExclusionError(f"{who}: {what} must be a mapping, got {entry!r}")
    method, path = entry.get("method"), entry.get("path")
    if not isinstance(method, str) or not isinstance(path, str):
        raise ExclusionError(f"{who}: {what} needs both a method and a path: {entry!r}")
    if method.lower() not in _METHODS:
        raise ExclusionError(f"{who}: {what} names no HTTP method: {method!r}")
    return ExcludedOperation(method.lower(), path)


def _body_schemas(spec: dict, operation: dict) -> list[dict]:
    """The request-body object schemas of ``operation``, resolved one ``$ref`` deep (where a
    body's properties are declared)."""
    schemas = []
    for body in ((operation.get("requestBody") or {}).get("content") or {}).values():
        schema = body.get("schema") or {}
        ref = schema.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/components/schemas/"):
            schema = spec["components"]["schemas"][ref.rsplit("/", 1)[1]]
        schemas.append(schema)
    return schemas


def _remove_argument(spec: dict, operation: dict, name: str) -> bool:
    """Remove the request argument ``name`` from ``operation``: a body property (with its place
    in ``required``) and a query, header or cookie parameter. True when something was removed.
    A path parameter is part of the operation's address and is never an argument to exclude."""
    removed = False
    for schema in _body_schemas(spec, operation):
        if name in (schema.get("properties") or {}):
            del schema["properties"][name]
            if name in (schema.get("required") or []):
                schema["required"] = [r for r in schema["required"] if r != name]
            removed = True
    parameters = operation.get("parameters") or []
    kept = [
        p
        for p in parameters
        if not (p.get("name") == name and p.get("in") in ("query", "header", "cookie"))
    ]
    if len(kept) != len(parameters):
        operation["parameters"] = kept
        removed = True
    return removed


def apply(spec: dict, exclusions: Exclusions, *, who: str) -> dict:
    """``spec`` without what ``exclusions`` names. ``spec`` itself is not changed.

    Raises :class:`ExclusionError` naming every exclusion the specification has nothing for."""
    if not exclusions:
        return spec
    out = copy.deepcopy(spec)
    paths = out.get("paths") or {}
    stale: list[str] = []

    def _find(op: ExcludedOperation) -> dict | None:
        return (paths.get(op.path) or {}).get(op.method)

    for argument in exclusions.arguments:
        if argument.operation is not None:
            operation = _find(argument.operation)
            if operation is None or not _remove_argument(out, operation, argument.name):
                stale.append(f"argument {argument}")
            continue
        hits = [
            _remove_argument(out, operation, argument.name)
            for item in paths.values()
            for method, operation in item.items()
            if method in _METHODS
        ]
        if not any(hits):
            stale.append(f"argument {argument}")
    for excluded in exclusions.operations:
        item = paths.get(excluded.path) or {}
        if excluded.method not in item:
            stale.append(f"operation {excluded}")
            continue
        del item[excluded.method]
        if not any(method in item for method in _METHODS):
            del paths[excluded.path]
    if stale:
        raise ExclusionError(
            f"{who}: the specification has nothing for these exclusions: {'; '.join(stale)}"
        )
    return out


def excluded_operation_ids(spec: dict, exclusions: Exclusions) -> dict[str, str]:
    """``operationId`` -> "METHOD path" for each operation of ``spec`` that ``exclusions``
    removes: what a deployment compares its registered tables and commands against, so that one
    an exclusion now covers is reported by name."""
    paths = spec.get("paths") or {}
    found = {}
    for excluded in exclusions.operations:
        operation = (paths.get(excluded.path) or {}).get(excluded.method)
        if operation is not None and operation.get("operationId"):
            found[operation["operationId"]] = str(excluded)
    return found


def load_source_spec(spec_path: str, mapping: dict | None, *, source_id: str) -> dict:
    """The specification an OpenAPI source is built from: the one ``spec_path`` names (a brand's
    arrives with the brand's exclusions already applied) without the source's own exclusions,
    which an operator sets under ``exclusions`` in the source's mapping. THE way a registered
    source's specification is loaded, so every consumer sees the same one."""
    from provisa.openapi.loader import load_spec

    who = f"source {source_id}"
    return apply(
        load_spec(spec_path), Exclusions.parse((mapping or {}).get("exclusions"), who=who), who=who
    )


def _normal(operation_id: str) -> str:
    """How an operation's id is compared with a registered name: a table registered from an
    operation is named for it, give or take underscores, hyphens and case
    (``provisa/api_source/openapi_endpoint.py``)."""
    return operation_id.replace("_", "").replace("-", "").lower()


def source_excluded_operations(
    spec_path: str, mapping: dict | None, *, source_id: str
) -> dict[str, str]:
    """``operationId`` -> "METHOD path" for every operation the source of ``spec_path`` does not
    offer: its brand's exclusions (read against the vendor's specification as published) and the
    operator's own."""
    from provisa.openapi.brands import brand_excluded_operations, spec_brand
    from provisa.openapi.loader import load_spec

    brand = spec_brand(spec_path)
    found = dict(brand_excluded_operations(brand)) if brand is not None else {}
    own = Exclusions.parse((mapping or {}).get("exclusions"), who=f"source {source_id}")
    if own:
        found.update(excluded_operation_ids(load_spec(spec_path), own))
    return found


def covered_registrations(
    excluded: dict[str, str], *, tables: list[str], commands: list[str]
) -> list[str]:
    """What a deployment has registered that the source no longer offers, each by name with the
    operation it was made from: a table (named for its operation) or a command (which records
    its operation's id). Registered before the exclusion, it is reported, never silently
    dropped: reading the table or running the command now finds no operation."""
    by_normal = {_normal(operation_id): where for operation_id, where in excluded.items()}
    reported = [
        f"table {name} ({by_normal[_normal(name)]})"
        for name in tables
        if _normal(name) in by_normal
    ]
    reported += [f"command {name} ({excluded[name]})" for name in commands if name in excluded]
    return sorted(reported)
