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
            f"{checker} contract must be a mapping at the top level, got {type(parsed).__name__}"
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

    Every check is a SINGLE-KEY mapping whose key is the check type and whose value is that type's
    body (``- missing:`` / ``- row_count: {must_be_greater_than: 0}``). Soda's own parser rejects a
    check with any other number of keys ("Checks require 1 key to be the type",
    contracts/impl/contract_yaml.py), so the same shape is required here — a builder that emitted a
    different one would produce a contract the checker refuses to run."""
    checks: list[dict] = []
    for check in _soda_list(parsed, "checks"):
        checks.append(_soda_check(check, column_name=""))
    for column in _soda_list(parsed, "columns"):
        if not isinstance(column, dict):
            raise ContractError(f"soda contract 'columns' entry must be a mapping, got {column!r}")
        name = column.get("name")
        if not isinstance(name, str) or not name:
            raise ContractError("soda contract 'columns' entry is missing its 'name'")
        for check in _soda_list(column, "checks"):
            checks.append(_soda_check(check, column_name=name))
    return checks


def _soda_list(parsed: dict, key: str) -> list:
    """``parsed[key]`` as a list of entries, absent meaning none.

    A key holding anything else is a malformed contract and says so here: Soda's own parser
    would refuse it too, and letting a scalar reach the loop below would surface as a TypeError
    naming an internal frame rather than the contract that is wrong.
    """
    value = parsed.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ContractError(f"soda contract {key!r} must be a list, got {type(value).__name__}")
    return value


def _soda_check(check: Any, column_name: str) -> dict:
    if not isinstance(check, dict):
        raise ContractError(f"soda check must be a mapping, got {check!r}")
    if len(check) != 1:
        raise ContractError(
            f"soda check must have exactly one key naming its type, got {sorted(check)}"
        )
    check_type = next(iter(check))
    if not isinstance(check_type, str) or not check_type:
        raise ContractError(f"soda check type must be a non-empty string, got {check_type!r}")
    body = check[check_type]
    return {
        "column_name": column_name,
        "check_type": check_type,
        # The row carries the check's ARGS alone (REQ-1443 clause 7). Soda's wrapper — the single
        # key naming the type — is the dialect's, restated by :func:`_check_body` on the way back
        # out, so the editor never asks an operator to retype the type they already picked. A
        # bodiless check (``- missing:``) has no args, hence the empty text.
        "definition": (
            ""
            if body is None
            else yaml.safe_dump(body, sort_keys=False, default_flow_style=False).strip()
        ),
        "extra": "",
    }


def _gx_checks(parsed: dict) -> list[dict]:
    """GX 1.x suite shape: ``expectations: [{type, kwargs}]``. The expectation's own ``type`` is the
    check kind and ``kwargs.column`` the column it applies to (absent for table-level expectations,
    which is the same distinction Soda draws between dataset- and column-level checks."""
    checks: list[dict] = []
    expectations = parsed.get("expectations")
    if expectations is not None and not isinstance(expectations, list):
        raise ContractError(
            "great_expectations suite 'expectations' must be a list, got "
            f"{type(expectations).__name__}"
        )
    for expectation in expectations or []:
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
        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, dict):
            raise ContractError(
                f"great_expectations expectation {check_type!r} 'kwargs' must be a mapping, "
                f"got {type(kwargs).__name__}"
            )
        column = kwargs.get("column")
        checks.append(
            {
                "column_name": column if isinstance(column, str) else "",
                "check_type": check_type,
                # The row carries the expectation's ARGS alone (REQ-1443 clause 7); the
                # ``type``/``kwargs`` envelope is GX's and is restated by :func:`_check_body`.
                "definition": json.dumps(kwargs, sort_keys=True),
                # Anything else the suite authored on the expectation — `id`, `meta`, `notes` as
                # GX's own serializer emits them. Not editable, and kept verbatim rather than
                # dropped, so a pasted suite still round-trips through the panel unchanged.
                "extra": _gx_extra(expectation),
            }
        )
    return checks


def _gx_extra(expectation: dict) -> str:
    """The expectation's keys beside ``type`` and ``kwargs``, as JSON (``""`` when it has none)."""
    extra = {k: v for k, v in expectation.items() if k not in ("type", "kwargs")}
    return json.dumps(extra, sort_keys=True) if extra else ""


def check_definition(check: dict, checker: str) -> str:
    """One check row as its own text in ``checker``'s dialect — args and envelope together.

    The row's ``definition`` is the ARGS alone, because that is all the panel's editor can change
    (REQ-1443 clause 7). A catalog reading a published assertion wants the check as the checker runs
    it, envelope included, so the two are joined back here rather than by each consumer.
    """
    if checker not in CHECKERS:
        raise ContractError(
            f"unknown data-quality checker {checker!r}; expected one of {sorted(CHECKERS)}"
        )
    body = _check_body(check, checker)
    if checker == "soda":
        return yaml.safe_dump(body, sort_keys=False, default_flow_style=False).strip()
    return json.dumps(body, sort_keys=True)


def check_severity(check: dict, checker: str) -> str:
    """``'fail'`` or ``'warn'`` for one check row from :func:`contract_checks` (REQ-1443).

    Soda spells severity as ``threshold.level`` and defaults an unstated level to ``fail``
    (``soda_core`` ``ThresholdYaml.level``). A Great Expectations expectation has no warn level —
    it succeeds or it does not — so every GX check is ``fail``. That asymmetry is the dialects', so
    it is read here once rather than restated by the export builder and the UI panel separately.
    """
    if checker not in CHECKERS:
        raise ContractError(
            f"unknown data-quality checker {checker!r}; expected one of {sorted(CHECKERS)}"
        )
    if checker == "great_expectations":
        return "fail"
    body = _check_body(check, checker).get(check["check_type"])
    threshold = body.get("threshold") if isinstance(body, dict) else None
    level = threshold.get("level") if isinstance(threshold, dict) else None
    return level if level in ("fail", "warn") else "fail"


def build_contract(checker: str, dataset: str, checks: list[dict]) -> str:
    """The contract text for ``checks``, in ``checker``'s dialect — the inverse of
    :func:`contract_checks`.

    The UI builder (REQ-1443 clause 7) edits check rows and serializes them HERE rather than in the
    browser, so the dialect has exactly one implementation and a builder can never emit a shape the
    checker refuses to run. ``checks`` carries the same ``{column_name, check_type, definition}``
    dicts ``contract_checks`` returns, and the two round-trip: a contract pasted into the raw editor,
    parsed to rows and serialized back keeps every check body it arrived with, because ``definition``
    is the check's own text rather than a normalized summary of it.
    """
    if checker not in CHECKERS:
        raise ContractError(
            f"unknown data-quality checker {checker!r}; expected one of {sorted(CHECKERS)}"
        )
    parts = dataset.split("/")
    if len(parts) != _DATASET_PARTS or not all(p.strip() for p in parts):
        raise ContractError(
            f"contract dataset {dataset!r} must have exactly {_DATASET_PARTS} non-empty "
            f"slash-separated parts '<data source>/<schema>/<table>'"
        )
    if checker == "soda":
        return _build_soda(dataset, checks)
    return _build_gx(dataset, checks)


def _check_body(check: dict, checker: str) -> Any:
    """One check's body in the dialect's own shape, rebuilt from the row's args.

    A row holds the check's ARGS and its type separately (REQ-1443 clause 7) — the dialect's
    envelope (soda's single key naming the type, GX's ``type``/``kwargs`` pair) is restated here, on
    the one path that writes contract text, so the editor shows an operator the arguments alone and
    never a wrapper they cannot change.
    """
    check_type = check.get("check_type")
    if not isinstance(check_type, str) or not check_type:
        raise ContractError(f"{checker} check row carries no check_type to serialize: {check!r}")
    definition = check.get("definition")
    if not isinstance(definition, str):
        raise ContractError(
            f"{checker} check {check_type!r} carries no definition text to serialize"
        )
    args = _parse_args(definition, check_type, checker)
    if checker == "soda":
        return {check_type: args}
    if args is None:
        args = {}
    if not isinstance(args, dict):
        raise ContractError(
            f"great_expectations check {check_type!r} args must be a mapping, "
            f"got {type(args).__name__}"
        )
    extra = check.get("extra")
    envelope = json.loads(extra) if isinstance(extra, str) and extra.strip() else {}
    return {**envelope, "type": check_type, "kwargs": args}


def _parse_args(definition: str, check_type: str, checker: str) -> Any:
    """A row's args text as data. Empty text means the check takes no args (soda's ``- missing:``)."""
    if not definition.strip():
        return None
    try:
        return yaml.safe_load(definition)
    except yaml.YAMLError as exc:
        raise ContractError(
            f"{checker} check {check_type!r} args are not parseable as YAML/JSON: {exc}"
        ) from exc


def _build_soda(dataset: str, checks: list[dict]) -> str:
    """Soda v4: dataset-level ``checks:`` and per-column ``columns: [{name, checks: [...]}]``, each
    check a single-key mapping whose key is its type."""
    dataset_checks: list[dict] = []
    by_column: dict[str, list[dict]] = {}
    for check in checks:
        body = _check_body(check, "soda")
        column = check.get("column_name") or ""
        if column:
            by_column.setdefault(column, []).append(body)
        else:
            dataset_checks.append(body)
    contract: dict[str, Any] = {"dataset": dataset}
    if by_column:
        contract["columns"] = [
            {"name": name, "checks": bodies} for name, bodies in by_column.items()
        ]
    if dataset_checks:
        contract["checks"] = dataset_checks
    return yaml.safe_dump(contract, sort_keys=False, default_flow_style=False)


def _build_gx(dataset: str, checks: list[dict]) -> str:
    """GX 1.x: an expectation suite whose target rides in ``meta.dataset`` — the artifact stays a
    valid GX suite, so it round-trips through GX itself unchanged."""
    suite = {
        "meta": {"dataset": dataset},
        "expectations": [_check_body(check, "great_expectations") for check in checks],
    }
    return json.dumps(suite, indent=2, sort_keys=False)
