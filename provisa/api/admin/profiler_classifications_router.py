# Copyright (c) 2026 Kenneth Stott
# Canary: 85ea3ee2-3220-4afb-9cdc-f55adb2d3f0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes of a profile's classification suggestions and of the classification kinds
(REQ-2147).

* ``GET    /admin/tables/{table_id}/profile-classifications`` -- the steward's decisions.
* ``POST   /admin/tables/{table_id}/profile-classifications`` -- accept or reject a suggestion.
  Accepting links the column to the kind's Property term and, for a sensitive kind, marks it pii;
  the answer names the mask the kind offers.
* ``DELETE /admin/tables/{table_id}/profile-classifications`` -- withdraw a decision.
* ``GET    /admin/classification-kinds`` -- every kind, each with its test, threshold, whether it is
  sensitive and whether the org has it enabled.
* ``PUT    /admin/classification-kinds`` -- the kinds the org has enabled.
* ``POST   /admin/classification-kinds`` -- add a custom kind (amended 2026-11-25).
* ``PATCH  /admin/classification-kinds/{kind_id}`` -- change a kind: a shipped kind's share that
  must pass, sensitivity and mask, a custom kind's any field, and whether either is enabled.
* ``DELETE /admin/classification-kinds/{kind_id}`` -- delete a custom kind.
* ``POST   /admin/tables/{table_id}/sensitive-scans`` -- scan the table's text columns for
  sensitive kinds held inside their values, in the background (KINDS ARE PATTERN RULES; CONTAINED
  VALUES ARE HINTS). Answers at once with the scan, running.
* ``GET    /admin/tables/{table_id}/sensitive-scans/latest`` -- the latest scan and the hints it
  left: per column, the share of sampled values each kind was found inside, never the text.

A decision is the steward's: it needs ``sensitive_data`` in the table's own domain (data_steward,
in its domains; org_admin, in every one). The kinds are listed for a holder of ``sensitive_data``
or ``org_settings``, and enabled or disabled by a holder of ``org_settings`` alone. A scan is run
and read by those who may register tables in the table's own domain -- it is asked for while
registering a table, a withdrawn one as well as one in service -- and shows hints only on the columns the
caller is served.
"""

# Requirements: REQ-2147, REQ-1159, REQ-2278

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import (
    allowed_domains_for_capability_request,
    require_capability_request,
)
from provisa.api.admin.profiler_router import _db, _jsonable, _member
from provisa.api.admin.column_rules_router import keyed_by
from provisa.api.admin.table_read import refuse_unparameterized, table_for_steward
from provisa.api.errors import ApiError
from provisa.api.engine_error_names import client_value

router = APIRouter(prefix="/admin", tags=["admin", "profiler"])

_RIGHT = "sensitive_data"


class ClassificationDecision(BaseModel):
    column: str  # as the org admin reads it (the result row's column_name)
    kind: str
    status: str  # accepted | rejected
    evidence: str
    runId: str
    # REQ-2145: what to do with a tag the column inherits from the term it leaves, as a term move
    # in the glossary takes them.
    confirmTagLoss: list[str] = []
    carryTags: bool = False


class ForgetClassification(BaseModel):
    physicalColumn: str  # the decision's column, by its registered name
    kind: str


class EnabledKinds(BaseModel):
    enabled: list[str]


def _caller(request: Request) -> str | None:
    identity = getattr(request.state, "identity", None)
    return None if identity is None else getattr(identity, "user_id", None)


async def _steward_of(request: Request, conn: Any, table_id: int) -> None:
    """Refuse a caller without ``sensitive_data`` in the table's own domain."""
    from provisa.api.admin.domain_guard import table_domain

    require_capability_request(request, _RIGHT)
    allowed = allowed_domains_for_capability_request(request, _RIGHT)
    domain = await table_domain(conn, table_id)
    if allowed is not None and domain not in allowed:
        raise ApiError(
            403,
            "auth.domain_denied",
            f"No {_RIGHT} right in domain {domain!r}",
            domain=domain,
        )


def _physical(table_id: int, column: str, served: set[str] | None) -> str:
    """``column``'s registered name; a column the caller is not served (``served``, from
    ``_member``) is answered as one the table does not have (REQ-2152)."""
    from provisa.api.admin.profiler_checks_router import _physical_names

    physical = _physical_names(table_id).get(column)
    if physical is None or (served is not None and physical not in served):
        raise ApiError(
            404,
            "profile.column_not_found",
            f"Column {column!r} is not a column of table {table_id} the org admin reads",
            column=column,
        )
    return physical


def _unknown_kind(exc: KeyError, kind_id: str) -> ApiError:
    return ApiError(422, "profile.classification_kind_unknown", exc.args[0], kind=kind_id)


async def _kind(kind_id: str) -> Any:
    """The org's kind ``kind_id`` (a shipped kind as the org changed it, or its own)."""
    from provisa.profiler import classifications
    from provisa.profiler.classify import kind_in

    try:
        return kind_in(await classifications.org_kinds(_db()), kind_id)
    except KeyError as exc:
        raise _unknown_kind(exc, kind_id) from exc


@router.get("/tables/{table_id}/profile-classifications")
async def list_classifications(request: Request, table_id: int) -> dict:
    from provisa.profiler import classifications

    async with _db().acquire() as conn:
        member = await _member(conn, table_id, request)
        await _steward_of(request, conn, table_id)
        decided = await classifications.decisions(conn, table_id)
    served = member["served"]
    # REQ-2152: only the decisions on columns the caller is served.
    return {
        "decisions": [
            _jsonable(d)
            for (physical, _kind_id), d in decided.items()
            if served is None or physical in served
        ]
    }


@router.post("/tables/{table_id}/profile-classifications")
async def decide_classification(
    request: Request, table_id: int, body: ClassificationDecision
) -> dict:
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.term_tags import TagLossRefused, TermTagRefused
    from provisa.profiler import classifications

    if body.status not in classifications.DECISION_STATUSES:
        raise ApiError(
            422,
            "profile.classification_status_invalid",
            f"a decision is one of {classifications.DECISION_STATUSES}, got {body.status!r}",
            status=body.status,
        )
    kind = await _kind(body.kind)
    accepted = None
    async with _db().acquire() as conn:
        member = await _member(conn, table_id, request)
        await _steward_of(request, conn, table_id)
        physical = _physical(table_id, body.column, member["served"])
        try:
            async with conn.transaction():
                if body.status == "accepted":
                    accepted = await classifications.accept_effects(
                        conn,
                        table_id,
                        physical,
                        kind,
                        body.evidence,
                        confirm_tag_loss=tuple(body.confirmTagLoss),
                        carry_tags=body.carryTags,
                    )
                await classifications.record(
                    conn,
                    table_id,
                    physical,
                    kind,
                    status=body.status,
                    evidence=body.evidence,
                    run_id=body.runId,
                    by=_caller(request),
                )
        except TagLossRefused as exc:
            raise ApiError(409, exc.code, str(exc), columns=exc.columns) from exc
        except TermTagRefused as exc:
            raise ApiError(409, exc.code, str(exc), **exc.params) from exc
        except glossary_repo.TermIsClass as exc:
            from provisa.api.admin.read_rights import reader_for_request, told_glossary_refusal

            # REQ-2152: only the tables the caller reads are named.
            said, tables = await told_glossary_refusal(conn, reader_for_request(request), exc)
            raise ApiError(
                409, "glossary.term_is_class", said, term=exc.term, tables=tables
            ) from exc
        except classifications.PiiRefused as exc:
            raise ApiError(
                409,
                exc.result.code or "profile.classification_pii_refused",
                exc.result.message,
                **(exc.result.params or {}),
            ) from exc
        # REQ-2278: a column classified (and, for a sensitive kind, marked pii) comes under a column
        # rule keyed on either at once.
        ruled = await keyed_by(conn, tags=("pii",), kinds=(kind.id,))
    if ruled:
        from provisa.api.app import _rebuild_schemas

        await _rebuild_schemas()
    if accepted is None:
        return {"status": body.status}
    return {
        "status": body.status,
        "term": accepted.term,
        "termNote": accepted.term_note,
        "pii": accepted.pii,
        "maskPreset": accepted.mask_preset,
        "tagChanges": accepted.tag_changes,
    }


@router.delete("/tables/{table_id}/profile-classifications")
async def forget_classification(
    request: Request, table_id: int, body: ForgetClassification
) -> dict:
    from provisa.profiler import classifications

    kind = await _kind(body.kind)
    async with _db().acquire() as conn:
        member = await _member(conn, table_id, request)
        await _steward_of(request, conn, table_id)
        # REQ-2152: a decision on a column the caller is not served is answered as missing.
        served = member["served"]
        readable = served is None or body.physicalColumn in served
        if not readable or not await classifications.forget(
            conn, table_id, body.physicalColumn, kind.id
        ):
            raise ApiError(
                404,
                "profile.classification_decision_not_found",
                f"No decision on {body.physicalColumn!r} as {kind.id!r}",
                column=body.physicalColumn,
                kind=kind.id,
            )
        ruled = await keyed_by(conn, kinds=(kind.id,))  # REQ-2278: the column is released at once
    if ruled:
        from provisa.api.app import _rebuild_schemas

        await _rebuild_schemas()
    return {"ok": True}


# -- a command's output column (REQ-1159, amended 2026-11-25) ---------------------------------------


class CommandClassification(BaseModel):
    column: str  # a declared output column of the command
    kind: str
    status: str  # accepted | rejected
    evidence: str  # the steward's: no profile run reads a command's output


async def _command_steward(request: Request, conn: Any, name: str) -> Any:
    """The command, for a caller holding ``sensitive_data`` in its domain who reads it."""
    from provisa.api.admin.command_columns import command_output
    from provisa.api.admin.read_rights import reader_for_request

    require_capability_request(request, _RIGHT)
    command = await command_output(conn, name)
    if not reader_for_request(request).command(name):
        raise ApiError(404, "actions.command_not_found", f"No command {name!r}", command=name)
    allowed = allowed_domains_for_capability_request(request, _RIGHT)
    if allowed is not None and command.domain_id not in allowed:
        raise ApiError(
            403,
            "auth.domain_denied",
            f"No {_RIGHT} right in domain {command.domain_id!r}",
            domain=command.domain_id,
        )
    return command


@router.get("/commands/{name}/profile-classifications")
async def list_command_classifications(request: Request, name: str) -> dict:
    from provisa.profiler import classifications

    async with _db().acquire() as conn:
        await _command_steward(request, conn, name)
        decided = await classifications.command_decisions(conn, name)
    return {"decisions": [_jsonable(d) for d in decided.values()]}


@router.post("/commands/{name}/profile-classifications")
async def decide_command_classification(
    request: Request, name: str, body: CommandClassification
) -> dict:
    """A steward's classification of a command's output column, as of a table column's: accepted,
    the column means the kind's Property term and, for a sensitive kind, is marked pii."""
    from provisa.api.admin.command_columns import require_output_column
    from provisa.profiler import classifications

    if body.status not in classifications.DECISION_STATUSES:
        raise ApiError(
            422,
            "profile.classification_status_invalid",
            f"a decision is one of {classifications.DECISION_STATUSES}, got {body.status!r}",
            status=body.status,
        )
    kind = await _kind(body.kind)
    async with _db().acquire() as conn:
        command = await _command_steward(request, conn, name)
        require_output_column(command, body.column)
        async with conn.transaction():
            if body.status == "accepted":
                try:
                    await classifications.accept_command_column_effects(
                        conn, command.name, command.domain_id, body.column, kind, body.evidence
                    )
                except classifications.PiiRefused as exc:
                    raise ApiError(
                        409,
                        exc.result.code or "profile.classification_pii_refused",
                        exc.result.message,
                        **(exc.result.params or {}),
                    ) from exc
            await classifications.record_command_column(
                conn,
                command.name,
                body.column,
                kind.id,
                status=body.status,
                evidence=body.evidence,
                by=_caller(request),
            )
        ruled = await keyed_by(conn, tags=("pii",), kinds=(kind.id,))  # REQ-2278
    if ruled:
        from provisa.api.app import _rebuild_schemas

        await _rebuild_schemas()
    return {"status": body.status}


@router.delete("/commands/{name}/profile-classifications")
async def forget_command_classification(
    request: Request, name: str, body: ForgetClassification
) -> dict:
    from provisa.profiler import classifications

    kind = await _kind(body.kind)
    async with _db().acquire() as conn:
        await _command_steward(request, conn, name)
        if not await classifications.forget_command_column(
            conn, name, body.physicalColumn, kind.id
        ):
            raise ApiError(
                404,
                "profile.classification_decision_not_found",
                f"No decision on {body.physicalColumn!r} as {kind.id!r}",
                column=body.physicalColumn,
                kind=kind.id,
            )
        ruled = await keyed_by(conn, kinds=(kind.id,))  # REQ-2278: the column is released at once
    if ruled:
        from provisa.api.app import _rebuild_schemas

        await _rebuild_schemas()
    return {"ok": True}


class CustomKind(BaseModel):
    """A custom kind as the form and Polly send it (REQ-2147, amended 2026-11-25)."""

    label: str
    pattern: str
    check: str | None
    ignore: str | None
    threshold: float
    sensitive: bool
    maskPreset: str | None
    term: str


class KindChange(BaseModel):
    """A change to one kind: of a shipped kind only ``threshold``, ``sensitive`` and
    ``maskPreset``; of a custom kind any of its fields; of either whether it is enabled. A field
    left out is unchanged."""

    enabled: bool | None = None
    threshold: float | None = None
    sensitive: bool | None = None
    maskPreset: str | None = None
    label: str | None = None
    pattern: str | None = None
    check: str | None = None
    ignore: str | None = None
    term: str | None = None


_STORED = {"maskPreset": "mask_preset"}  # the stored name of a field the API spells otherwise


def _stored(fields: dict[str, Any]) -> dict[str, Any]:
    return {_STORED.get(k, k): v for k, v in fields.items()}


def _refused(exc: ValueError, kind_id: str | None) -> ApiError:
    return ApiError(422, "profile.classification_kind_invalid", str(exc), kind=kind_id or "")


async def _kinds_doc() -> dict:
    from provisa.profiler import classifications

    settings = await classifications.read_settings(_db())
    return {
        "kinds": [
            classifications.kind_doc(k, k.id not in settings.disabled) for k in settings.kinds
        ],
        **classifications.kind_choices(),
    }


async def _save(request: Request, settings: Any) -> None:
    from provisa.core.org_settings import write_org_overrides
    from provisa.profiler import classifications

    await write_org_overrides(
        _db(),
        {classifications.SETTING_KEY: classifications.setting_of(settings)},
        updated_by=_caller(request) or "unknown",
    )


@router.get("/classification-kinds")
async def list_kinds(request: Request) -> dict:
    # REQ-1327: classification is the steward's -- the kinds a column is tested for are what its
    # decisions are made on, so the kinds' page and their changes are the decisions' own right.
    require_capability_request(request, _RIGHT)
    return await _kinds_doc()


@router.put("/classification-kinds")
async def set_enabled_kinds(request: Request, body: EnabledKinds) -> dict:
    from provisa.profiler import classifications

    require_capability_request(request, _RIGHT)
    settings = await classifications.read_settings(_db())
    try:
        changed = classifications.with_enabled(settings, set(body.enabled))
    except KeyError as exc:
        unknown = sorted(set(body.enabled) - {k.id for k in settings.kinds})
        raise ApiError(
            422, "profile.classification_kind_unknown", exc.args[0], kind=", ".join(unknown)
        ) from exc
    await _save(request, changed)
    return await _kinds_doc()


@router.post("/classification-kinds")
async def create_kind(request: Request, body: CustomKind) -> dict:
    """Add a custom kind (REQ-2147, amended 2026-11-25). The answer is every kind, and the new
    kind's id."""
    from provisa.profiler import classifications

    require_capability_request(request, _RIGHT)
    settings = await classifications.read_settings(_db())
    try:
        changed, kind_id = classifications.with_custom(settings, _stored(body.model_dump()))
    except ValueError as exc:
        raise _refused(exc, None) from exc
    await _save(request, changed)
    return {"id": kind_id, **await _kinds_doc()}


@router.patch("/classification-kinds/{kind_id}")
async def change_kind(request: Request, kind_id: str, body: KindChange) -> dict:
    """Change one kind: a shipped kind's share, sensitivity and mask, a custom kind's any field,
    and whether either is enabled."""
    from provisa.profiler import classifications
    from provisa.profiler.classify import CHANGEABLE, kind_in

    require_capability_request(request, _RIGHT)
    settings = await classifications.read_settings(_db())
    try:
        kind = kind_in(settings.kinds, kind_id)
    except KeyError as exc:
        raise _unknown_kind(exc, kind_id) from exc
    given = body.model_dump(exclude_unset=True)
    enabled = given.pop("enabled", None)
    change = _stored(given)
    try:
        if kind.custom:
            settings = classifications.with_custom_change(settings, kind_id, change)
        else:
            refused = sorted(set(change) - set(CHANGEABLE))
            if refused:
                raise ValueError(
                    f"classification kind {kind_id!r} is shipped: its {', '.join(refused)} "
                    "cannot change; copy it into a custom kind to change its test"
                )
            settings = classifications.with_shipped_change(settings, kind_id, change)
        if enabled is not None:
            ids = {k.id for k in settings.kinds} - settings.disabled
            settings = classifications.with_enabled(
                settings, ids | {kind_id} if enabled else ids - {kind_id}
            )
    except ValueError as exc:
        raise _refused(exc, kind_id) from exc
    await _save(request, settings)
    return await _kinds_doc()


@router.delete("/classification-kinds/{kind_id}")
async def delete_kind(request: Request, kind_id: str) -> dict:
    """Delete a custom kind, with the decisions on it and the hints of it; a shipped kind is
    disabled, never deleted."""
    from provisa.profiler import classifications

    require_capability_request(request, _RIGHT)
    settings = await classifications.read_settings(_db())
    try:
        changed = classifications.without_custom(settings, kind_id)
    except KeyError as exc:
        raise _unknown_kind(exc, kind_id) from exc
    except ValueError as exc:
        raise _refused(exc, kind_id) from exc
    async with _db().acquire() as conn:
        async with conn.transaction():
            await classifications.forget_kind(conn, kind_id)
    await _save(request, changed)
    return await _kinds_doc()


# -- contained values: hints -----------------------------------------------------------------------


def _scan_doc(scan: dict) -> dict:
    return {
        "scanId": scan["scan_id"],
        "status": scan["status"],
        "startedAt": scan["started_at"].isoformat(),
        "finishedAt": None if scan["finished_at"] is None else scan["finished_at"].isoformat(),
        "sampledRows": scan["sampled_rows"],
        "scannedColumns": scan["scanned_columns"],
        "error": client_value(scan["error"]),
    }


@router.post("/tables/{table_id}/sensitive-scans")
async def start_sensitive_scan(request: Request, table_id: int) -> dict:
    from provisa.api.app import state
    from provisa.core.connection_loop import spawn_background
    from provisa.profiler import hints

    # REQ-1986: a table that requires parameters has no rows to scan; refused from its metadata
    # before a scan is recorded.
    refuse_unparameterized(table_for_steward(request, state, table_id))
    async with _db().acquire() as conn:
        scan_id = await hints.start(conn, table_id, _caller(request))
        scan = await hints.latest_scan(conn, table_id)
    assert scan is not None and scan["scan_id"] == scan_id, "the scan was just recorded"
    # A scan outlives its request: it runs on a background worker (provisa.core.connection_loop).
    spawn_background(hints.run(state, table_id, scan_id), name="sensitive-value-scan")
    return _scan_doc(scan)


@router.get("/tables/{table_id}/sensitive-scans/latest")
async def latest_sensitive_scan(request: Request, table_id: int) -> dict:
    from provisa.api.admin.read_rights import served_columns_request
    from provisa.api.app import state
    from provisa.profiler import classifications, hints

    table_for_steward(request, state, table_id)
    kinds = {k.id: k for k in await classifications.org_kinds(_db())}
    async with _db().acquire() as conn:
        scan = await hints.latest_scan(conn, table_id)
        found = await hints.hints_of(conn, table_id)
        served = await served_columns_request(conn, request, table_id)
    return {
        "scan": None if scan is None else _scan_doc(scan),
        # REQ-2152: only the hints on columns the caller is served.
        "hints": [
            {
                "column": h["physical_column"],
                "kind": h["kind"],
                "label": kinds[h["kind"]].label,
                "custom": kinds[h["kind"]].custom,
                "sensitive": kinds[h["kind"]].sensitive,
                "valuesTested": h["values_tested"],
                "valuesMatched": h["values_matched"],
                "share": h["share"],
                "evidence": h["evidence"],
            }
            for h in found
            if served is None or h["physical_column"] in served
        ],
    }
