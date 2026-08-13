# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1c8f0a-3e77-4d2c-9a41-6f0d8e2b7c14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The contract text is the definition; the scanned target is DERIVED from it (REQ-1443, REQ-939).

A checker table carries ONE authored artifact — ``Table.dq_contract`` — and nothing else. What the
checker scans is read back OUT of that artifact rather than declared alongside it, so lineage
("this results table observes that governed table") cannot disagree with what the checker actually
ran against. A second, hand-typed target field would be exactly the drift REQ-939 forbids.

Both dialects name the target the same way — a slash-separated, fully qualified dataset — because
both aim at Provisa's own pgwire endpoint:

    <data source>/<schema>/<table>

Soda v4 spells it as the required top-level ``dataset:`` key (``soda_core.common.yaml``
``read_dataset_identifier``). Great Expectations has no target in an expectation suite at all — a
suite is a bag of expectations and the batch is supplied at runtime — so the same identifier is read
from the suite's ``meta.dataset``. ``meta`` is GX's own user-metadata slot, which keeps the artifact
a VALID GX suite that round-trips through GX unchanged; a Provisa-invented sibling key would not.
"""

from __future__ import annotations

import json
from typing import Any

import yaml

# The checkers REQ-1443 ships. Soda is Elastic License 2.0 (self-hosted and desktop only — the
# hosted plane must never provision it); Great Expectations is Apache 2.0 and carries no such bar.
# The pair is the point: the results schema and the source shape are identical, so the licence tier
# is a property of the CHOICE, not of the feature.
CHECKERS: frozenset[str] = frozenset({"soda", "great_expectations"})

# The dataset identifier's shape. Soda accepts two or more parts; Provisa requires three, because a
# governed table is addressed by (schema, table) and the leading part is the checker's own data
# source name. A two-part identifier cannot name a Provisa table, so it is rejected at parse rather
# than resolved by guessing which half is missing.
_DATASET_PARTS = 3


class ContractError(Exception):
    """A contract's text is unparseable, names no dataset, or names an ungoverned one."""


def _parse_text(text: str, checker: str) -> dict:
    """The contract as a mapping. YAML is the parser for both dialects — YAML is a superset of JSON,
    so a GX suite authored as JSON (the shape ``ExpectationSuite.to_json_dict`` emits) parses here
    unchanged and the operator is never forced to convert it."""
    try:
        parsed: Any = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ContractError(f"{checker} contract is not parseable as YAML/JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ContractError(
            f"{checker} contract must be a mapping at the top level, got "
            f"{type(parsed).__name__}"
        )
    return parsed


def contract_dataset(text: str, checker: str) -> str:
    """The fully qualified dataset the contract scans, verbatim.

    Raises :class:`ContractError` when the checker is unknown, the text is unparseable, the dataset
    key is absent, or it is not the three-part slash form. There is no default target: a contract
    that does not say what it scans is not a contract, and inferring one from the table it happens
    to be registered under would recreate the hand-declared lineage REQ-939 removes.
    """
    if checker not in CHECKERS:
        raise ContractError(
            f"unknown data-quality checker {checker!r}; expected one of {sorted(CHECKERS)}"
        )
    parsed = _parse_text(text, checker)
    if checker == "soda":
        raw = parsed.get("dataset")
        where = "top-level 'dataset'"
    else:
        meta = parsed.get("meta")
        raw = meta.get("dataset") if isinstance(meta, dict) else None
        where = "'meta.dataset'"
    if not isinstance(raw, str) or not raw.strip():
        raise ContractError(
            f"{checker} contract names no dataset: {where} must be a slash-separated, fully "
            f"qualified name '<data source>/<schema>/<table>'"
        )
    dataset = raw.strip()
    parts = dataset.split("/")
    if len(parts) != _DATASET_PARTS or not all(p.strip() for p in parts):
        raise ContractError(
            f"{checker} contract dataset {dataset!r} must have exactly {_DATASET_PARTS} non-empty "
            f"slash-separated parts '<data source>/<schema>/<table>'"
        )
    return dataset


def dataset_parts(dataset: str) -> tuple[str, str, str]:
    """``dataset`` split into (data source name, schema, table). The input is what
    :func:`contract_dataset` returned, so the shape is already guaranteed."""
    data_source, schema, table = dataset.split("/")
    return data_source, schema, table


def resolve_contract_target(dataset: str, tables: list) -> Any:
    """The governed :class:`~provisa.core.models.Table` the contract scans.

    Matching is on (schema, table) — the trailing two parts. The leading part is the checker's data
    source name, which addresses the pgwire ENDPOINT the checker connects through, not a Provisa
    source id; the same governed table is reachable under whatever data source name the operator
    gave that endpoint.

    A dataset that resolves to no governed table raises :class:`ContractError`. A checker may only
    observe what Provisa governs — a contract aimed elsewhere would land rows describing a table
    that has no lineage, no governance and no RLS, which is the opposite of REQ-967's estate.
    """
    _, schema, table_name = dataset_parts(dataset)
    matches = [t for t in tables if t.schema_name == schema and t.table_name == table_name]
    if not matches:
        raise ContractError(
            f"contract dataset {dataset!r} resolves to no governed table "
            f"(schema {schema!r}, table {table_name!r})"
        )
    if len(matches) > 1:
        raise ContractError(
            f"contract dataset {dataset!r} is ambiguous: {len(matches)} governed tables share "
            f"schema {schema!r} and table {table_name!r}"
        )
    return matches[0]


def contract_checks(text: str, checker: str) -> list[dict]:
    """The contract's checks, flattened to ``{column_name, check_type, definition}`` dicts.

    This is what the UI contract builder (REQ-1443 clause 7) renders as its editable rows and what a
    registration-time preview lists without running a scan. It is a READ of the same authored text
    the checker executes — the builder never keeps a parallel model, so a hand edit to the raw
    contract can never be silently discarded by the panel.
    """
    parsed = _parse_text(text, checker)
    if checker == "soda":
        return _soda_checks(parsed)
    if checker == "great_expectations":
        return _gx_checks(parsed)
    raise ContractError(
        f"unknown data-quality checker {checker!r}; expected one of {sorted(CHECKERS)}"
    )


def _soda_checks(parsed: dict) -> list[dict]:
    """Soda v4 shape: dataset-level ``checks:`` plus per-column ``columns: [{name, checks: [...]}]``.
    Every check is a mapping whose required ``type`` names the kind."""
    checks: list[dict] = []
    for check in parsed.get("checks") or []:
        checks.append(_soda_check(check, column_name=""))
    for column in parsed.get("columns") or []:
        if not isinstance(column, dict):
            raise ContractError(f"soda contract 'columns' entry must be a mapping, got {column!r}")
        name = column.get("name")
        if not isinstance(name, str) or not name:
            raise ContractError("soda contract 'columns' entry is missing its 'name'")
        for check in column.get("checks") or []:
            checks.append(_soda_check(check, column_name=name))
    return checks


def _soda_check(check: Any, column_name: str) -> dict:
    if not isinstance(check, dict):
        raise ContractError(f"soda check must be a mapping, got {check!r}")
    check_type = check.get("type")
    if not isinstance(check_type, str) or not check_type:
        raise ContractError(f"soda check is missing its required 'type': {check!r}")
    return {
        "column_name": column_name,
        "check_type": check_type,
        "definition": yaml.safe_dump(check, sort_keys=False, default_flow_style=False).strip(),
    }


def _gx_checks(parsed: dict) -> list[dict]:
    """GX 1.x suite shape: ``expectations: [{type, kwargs}]``. The expectation's own ``type`` is the
    check kind and ``kwargs.column`` the column it applies to (absent for table-level expectations,
    which is the same distinction Soda draws between dataset- and column-level checks."""
    checks: list[dict] = []
    for expectation in parsed.get("expectations") or []:
        if not isinstance(expectation, dict):
            raise ContractError(
                f"great_expectations suite 'expectations' entry must be a mapping, "
                f"got {expectation!r}"
            )
        check_type = expectation.get("type")
        if not isinstance(check_type, str) or not check_type:
            raise ContractError(
                f"great_expectations expectation is missing its required 'type': {expectation!r}"
            )
        kwargs = expectation.get("kwargs")
        column = kwargs.get("column") if isinstance(kwargs, dict) else None
        checks.append(
            {
                "column_name": column if isinstance(column, str) else "",
                "check_type": check_type,
                "definition": json.dumps(expectation, sort_keys=True),
            }
        )
    return checks
