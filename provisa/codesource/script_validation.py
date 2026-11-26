# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1e3c58-d2f7-4b90-8e14-5c09b7a2f3d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Validating a Spark source's script before it is used (REQ-2225), in three steps.

The script -- the one the source serves, or one its form holds -- is validated by the source's own
host, which imports pyspark and provisa_services (provisa/codehost/front.py VALIDATE), on the
cluster ``spark_cluster.resolved`` gives the source:

1. STATIC. The host reads the script's tags and type hints as at pairing; its document is held to
   the manifest's rules (provisa/codesource/manifest.py): every tag known, every declared row type
   and parameter type mappable, every table argument a ``TableRef`` of a declared row type on a
   ``@query`` or ``@command``, and nothing required that the connector does not offer. It needs no
   cluster.
2. DRY RUN. Each ``@table`` and ``@query`` runs once on the cluster, on a sample of its governed
   inputs: each table argument is a governed table the caller names, read over Arrow
   Flight as the caller's role (provisa/codesource/table_reads.py), at most :data:`SAMPLE_ROWS`
   of it, and its rows are compared with its declared row type. Row counts and errors are
   reported. A function whose table arguments are not named is reported as needing them. A
   ``@command``, which may write or call out, is not run: it is checked statically and reported
   as not run by design (REQ-2225, amended 2026-11-24).
3. KEYS. A two-table ``@query`` used to match rows (a code resolver, REQ-2249) carries the key of
   each side in every row: the keys the caller names are checked against its declared columns and
   against its dry-run rows.

Each step's status is ``ok``, ``failed``, ``incomplete`` (an input not named) or ``not_run`` (an
earlier step failed, or there is no cluster), with each function's own result."""

# Requirements: REQ-2225, REQ-2249, REQ-2270
from __future__ import annotations

import asyncio
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import COMMAND, COMMANDS, TABLE, Export, Manifest, TableOf

if TYPE_CHECKING:
    from provisa.codesource.lifecycle import Setup
    from provisa.codesource.remote import RemoteWorker

#: How many rows of each table argument, and of each function's result, a dry run takes.
SAMPLE_ROWS = 100

OK, FAILED, INCOMPLETE, NOT_RUN = "ok", "failed", "incomplete", "not_run"
#: A function's own results beside the validator's (connectors/spark/provisa_spark/validate.py).
NEEDS_INPUT, MISMATCH, ERROR = "needs_input", "mismatch", "error"
#: A ``@command``'s dry-run result: it is checked statically and never run.
BY_DESIGN = "not_run_by_design"


class ScriptValidationRefused(BundleRefused):
    """A source whose script cannot be validated at all, said by name."""

    def __init__(self, code: str, message: str, **params: str) -> None:
        super().__init__(f"script_validation_{code}", message, **params)


@dataclass(frozen=True)
class Asked:
    """What the caller gives a validation: the script (None: the one the source serves), each
    function's inputs by parameter (a table argument's value is a governed table's name), and each
    code resolver's two keys, ``(source key, target key)``."""

    script: str | None = None
    inputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    keys: dict[str, tuple[str, str]] = field(default_factory=dict)


def _step(status: str, functions: list[dict], **extra: Any) -> dict:
    return {"status": status, "functions": functions, **extra}


def _status_of(functions: list[dict]) -> str:
    found = {f["status"] for f in functions}
    if found & {MISMATCH, ERROR}:
        return FAILED
    return INCOMPLETE if found & {NEEDS_INPUT, NOT_RUN} else OK


# -- step 1: static ---------------------------------------------------------------------------------


def _columns(manifest: Manifest, export: Export) -> list[str]:
    from provisa.codesource.commands import contract

    if export.kind == TABLE:
        from provisa.codesource.arrow_rows import row_schema

        return list(row_schema(manifest, export).names)
    return [c["name"] for c in contract(manifest, export)[1]]


def _command_of(manifest: Manifest, export: Export) -> dict:
    """``export`` as the command registering it (actions_router saves one so)."""
    from provisa.codesource.commands import CODE_COMMAND, command_kind, contract

    arguments, outputs = contract(manifest, export)
    return {
        "name": export.name,
        "function_name": export.name,
        "impl_kind": CODE_COMMAND,
        "kind": command_kind(export),
        "arguments": arguments,
        "output_columns": outputs,
    }


def _is_resolver(manifest: Manifest, export: Export) -> bool:
    from provisa.codesource.resolver import is_resolver

    return export.kind in COMMANDS and is_resolver(_command_of(manifest, export))


def _validated_exports(manifest: Manifest) -> list[Export]:
    """The functions validation checks: the script's tables, queries and commands."""
    return [e for e in manifest.exports if e.kind == TABLE or e.kind in COMMANDS]


def _not_run(export: Export) -> dict:
    """A ``@command``'s dry-run result: it may write or call out, so it is not run."""
    return {
        "function": export.name,
        "status": BY_DESIGN,
        "reason": "a @command may write or call out: it is checked statically and not run",
    }


def _described(manifest: Manifest, export: Export) -> dict:
    tables = [p.name for p in export.parameters if isinstance(p.shape, TableOf)]
    return {
        "name": export.name,
        "kind": export.kind,
        "tables": tables,
        "arguments": [p.name for p in export.parameters if not isinstance(p.shape, TableOf)],
        "columns": _columns(manifest, export),
        "resolver": _is_resolver(manifest, export),
    }


def static(answer: dict, offered: set[str]) -> tuple[dict, Manifest | None]:
    """Step 1 from the host's ``static`` answer: its report, and the script's manifest when the
    step passed. ``offered`` names the values the connector offers its code."""
    from provisa.codesource.manifest import PYTHON_RULES, ManifestRefused, read_manifest

    if answer.get("failed"):
        return _step(FAILED, [], problems=[answer["failed"]]), None
    try:
        manifest = read_manifest(answer["document"], PYTHON_RULES)
    except ManifestRefused as refused:
        return _step(FAILED, [], problems=refused.problems, hash=answer.get("hash")), None
    problems = [
        f"the script requires {r.name}, which the connector does not offer"
        for r in manifest.requires
        if r.name not in offered
    ]
    functions = [_described(manifest, e) for e in _validated_exports(manifest)]
    status = FAILED if problems else OK
    report = _step(status, functions, problems=problems, hash=answer.get("hash"))
    return report, (None if problems else manifest)


# -- step 2: the dry run ----------------------------------------------------------------------------


def _row_schema(manifest: Manifest, export: Export) -> str | None:
    """The declared rows' schema as a call carries it; None for a single value."""
    from provisa.codesource.arrow_rows import row_schema, schema_text
    from provisa.codesource.commands import _fields_of  # pyright: ignore[reportPrivateUsage]

    if export.kind == TABLE:
        return schema_text(row_schema(manifest, export))
    if export.many and _fields_of(manifest, export.returns) is not None:
        return schema_text(row_schema(manifest, export))
    return None


def _table_columns(manifest: Manifest, shape: TableOf) -> list[str]:
    from provisa.codesource.commands import _fields_of  # pyright: ignore[reportPrivateUsage]

    fields = _fields_of(manifest, shape.row)
    assert fields is not None  # the manifest admits no other row type for a TableRef
    return [f.name for f in fields]


def _call(manifest: Manifest, export: Export, given: dict[str, Any]) -> tuple[dict, dict, list]:
    """The validator's call of ``export`` with ``given``, its arguments not yet encoded (a table
    reference is made readable first), the table references in it (by parameter) and the table
    parameters ``given`` does not name."""
    from provisa.codesource.worker import ABSENT, encode

    names = [p.name for p in export.parameters]
    references: dict[str, dict] = {}
    missing: list[str] = []
    args: list[Any] = []
    for p in export.parameters:
        if isinstance(p.shape, TableOf):
            table = given.get(p.name)
            if not table:
                missing.append(p.name)
                args.append(ABSENT)
                continue
            references[p.name] = {
                "kind": "table_ref",
                "ref": str(table),
                "columns": _table_columns(manifest, p.shape),
            }
            args.append(references[p.name])
        else:
            args.append(given[p.name] if p.name in given else ABSENT)
    schema = _row_schema(manifest, export)
    call: dict[str, Any] = {
        "function": export.name,
        "kind": "table" if export.kind == TABLE else "command",
        "args": args,
        "names": names,
        "schema": schema,
        "sample": SAMPLE_ROWS,
    }
    if export.kind == TABLE:
        call["takes"] = list(export.takes)
        call["context"] = {
            "filters": [],
            "order_by": [],
            "limit": SAMPLE_ROWS,
            "columns": _columns(manifest, export),
        }
        call["context"] = encode(call["context"])
    return call, references, missing


def _sampled(reference: dict) -> None:
    """A table argument's statement, read at most :data:`SAMPLE_ROWS` rows of."""
    reference["sql"] = f"{reference['sql']} LIMIT {SAMPLE_ROWS}"


# -- step 3: the keys -------------------------------------------------------------------------------


def _declared_keys(manifest: Manifest, export: Export, keys: tuple[str, str]) -> str | None:
    """Why ``export``'s declared columns do not carry both ``keys``, or None."""
    from provisa.codesource.resolver import ResolverRefused, check_resolver

    try:
        check_resolver(_command_of(manifest, export), *keys)
    except ResolverRefused as refused:
        return refused.params["reason"]
    return None


def _key_checks(manifest: Manifest, asked: Asked) -> tuple[list[dict], list[dict]]:
    """The validator's key checks of the script's resolvers, and the results already known
    without a dry run: a resolver whose keys are not named, or whose declared columns lack one."""
    checks, known = [], []
    for export in _validated_exports(manifest):
        if not _is_resolver(manifest, export):
            continue
        keys = asked.keys.get(export.name)
        if keys is None:
            known.append({"function": export.name, "status": NEEDS_INPUT, "missing": ["keys"]})
            continue
        source_key, target_key = keys
        declared = _declared_keys(manifest, export, keys)
        if declared is not None:
            known.append(
                {
                    "function": export.name,
                    "status": MISMATCH,
                    "source_key": source_key,
                    "target_key": target_key,
                    "problems": [declared],
                }
            )
            continue
        checks.append({"function": export.name, "source_key": source_key, "target_key": target_key})
    return checks, known


# -- the three --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Target:
    """Where a source's script is validated: its host, its kept settings, and what is needed to
    give the host its setup values and the cluster."""

    worker: "RemoteWorker"
    remote: dict
    setup: Setup  # lifecycle.setup_values: what gives the host its setup values
    own_cluster: str | None
    offered: set[str]
    script_name: str


async def target_of(state: Any, source_id: str) -> Target:
    """Where ``source_id``'s script is validated; refused by name for a source that is not a Spark
    script connector's, or whose host is not connected."""
    from provisa.codesource import remote_sources
    from provisa.codesource.remote import RemoteWorker
    from provisa.codesource.source import context_for, prepare

    source = await remote_sources._source(state, source_id)  # pyright: ignore[reportPrivateUsage]
    mapping = source.mapping
    if not mapping.get("script_hash") or not mapping.get("connector"):
        raise ScriptValidationRefused(
            "not_script",
            f"{source_id!r} serves no script: only a Spark source's script is validated",
            source=source_id,
        )
    row = await remote_sources._connector(mapping["connector"])  # pyright: ignore[reportPrivateUsage]
    script_name = next(r["name"] for r in row["requires"] if r.get("script"))
    stood_in = [n for n, s in (mapping.get("org_defaults") or {}).items() if s == "spark_cluster"]
    if len(stood_in) != 1:
        raise ScriptValidationRefused(
            "no_cluster_value",
            f"{source_id!r} names no Spark cluster value: its connector is not the Spark one",
            source=source_id,
        )
    prepared = await prepare(source.id, source.type.value, mapping, await context_for(state))
    worker = prepared.worker()
    if not isinstance(worker, RemoteWorker) or prepared.remote is None or prepared.setup is None:
        raise ScriptValidationRefused(
            "not_connected", f"{source_id!r} has no connected host", source=source_id
        )
    settings = mapping.get("settings") or {}
    return Target(
        worker=worker,
        remote=prepared.remote,
        setup=prepared.setup,
        own_cluster=settings.get(stood_in[0]),
        offered={r.name for r in prepared.manifest.requires},
        script_name=script_name,
    )


def _shown_cluster(address: str) -> str:
    """A cluster's address as a report shows it: its host and port, never a token."""
    from provisa.core.spark_cluster import parse

    endpoint = parse(address)
    return f"sc://{endpoint.host}:{endpoint.port}"


async def _cluster(state: Any, target: Target) -> tuple[dict | None, str | None]:
    """The cluster ``spark_cluster.resolved`` gives the source, as the report shows it, or why
    there is none."""
    from provisa.core import spark_cluster
    from provisa.core.request_context import require_current_org

    try:
        resolved = await spark_cluster.resolved(
            state.admin_db, require_current_org(), target.own_cluster
        )
    except spark_cluster.SparkClusterRefused as refused:
        return None, str(refused)
    return {"address": _shown_cluster(resolved.address), "origin": resolved.origin}, None


async def validate(state: Any, target: Target, asked: Asked, role_id: str) -> dict:
    """The three steps' report on ``asked``'s script, run by ``target``'s host as ``role_id``."""
    from provisa.codesource.table_reads import TableNotReadable, readable
    from provisa.codesource.worker import encode
    from provisa.otel_compat import mint_udf_session

    worker = target.worker
    answer = await asyncio.to_thread(worker.validate, "static", asked.script)
    first, manifest = static(answer, target.offered)
    cluster, no_cluster = await _cluster(state, target)
    report: dict[str, Any] = {"static": first, "cluster": cluster}
    if manifest is None:
        reason = "the static check failed"
        report["dry_run"] = _step(NOT_RUN, [], reason=reason)
        report["keys"] = _step(NOT_RUN, [], reason=reason)
        return report
    if no_cluster is not None:
        report["dry_run"] = _step(NOT_RUN, [], reason=no_cluster)
        report["keys"] = _step(NOT_RUN, [], reason=no_cluster)
        return report
    values = await target.setup.read()
    if asked.script is not None:
        values[target.script_name] = asked.script
    known: list[dict] = []
    calls: list[dict] = []
    async with AsyncExitStack() as held:
        for export in _validated_exports(manifest):
            if export.kind == COMMAND:
                known.append(_not_run(export))
                continue
            call, references, missing = _call(manifest, export, asked.inputs.get(export.name, {}))
            if missing:
                known.append({"function": export.name, "status": NEEDS_INPUT, "missing": missing})
                continue
            session = mint_udf_session(str(uuid.uuid4()), "invoker", role_id)
            try:
                await held.enter_async_context(
                    readable(state, session, dict(references), remote=target.remote, local=None)
                )
            except TableNotReadable as unreadable:
                known.append({"function": export.name, "status": ERROR, "error": str(unreadable)})
                continue
            for reference in references.values():
                _sampled(reference)
            calls.append({**call, "args": encode(call["args"])})
        checks, known_keys = _key_checks(manifest, asked)
        request = {"values": values, "calls": calls, "match": checks}
        ran = await asyncio.to_thread(worker.validate, "run", asked.script, request)
    if ran.get("failed"):
        report["dry_run"] = _step(FAILED, known, reason=ran["failed"])
        report["keys"] = _step(NOT_RUN, known_keys, reason="the dry run could not run")
        return report
    dry = [*ran["report"]["dry_run"], *known]
    keyed = [*ran["report"]["match"], *known_keys]
    report["dry_run"] = _step(_status_of(dry), dry)
    report["keys"] = _step(_status_of(keyed), keyed)
    return report
