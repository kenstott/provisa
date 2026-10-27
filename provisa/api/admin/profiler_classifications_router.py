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

A decision is the steward's: it needs ``sensitive_data`` in the table's own domain (data_steward,
in its domains; org_admin, in every one). The kinds are listed for a holder of ``sensitive_data``
or ``org_settings``, and enabled or disabled by a holder of ``org_settings`` alone.
"""

# Requirements: REQ-2147

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import (
    allowed_domains_for_capability_request,
    has_capability_request,
    require_capability_request,
)
from provisa.api.admin.profiler_router import _db, _jsonable, _member
from provisa.api.errors import ApiError

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


def _kind(kind_id: str) -> Any:
    from provisa.profiler.classify import kind_of

    try:
        return kind_of(kind_id)
    except KeyError as exc:
        raise ApiError(422, "profile.classification_kind_unknown", str(exc), kind=kind_id) from exc


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
    kind = _kind(body.kind)
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
                    kind.id,
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

    kind = _kind(body.kind)
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
    return {"ok": True}


def _kind_doc(kind: Any, enabled: bool) -> dict:
    return {
        "id": kind.id,
        "label": kind.label,
        "sensitive": kind.sensitive,
        "test": kind.test,
        "families": list(kind.families),
        "threshold": kind.threshold,
        "term": kind.term,
        "maskPreset": kind.mask_preset,
        "enabled": enabled,
    }


@router.get("/classification-kinds")
async def list_kinds(request: Request) -> dict:
    from provisa.core.org_settings import read_org_overrides
    from provisa.profiler import classifications
    from provisa.profiler.classify import catalogue

    if not (
        has_capability_request(request, _RIGHT) or has_capability_request(request, "org_settings")
    ):
        raise ApiError(
            403,
            "auth.missing_capability",
            f"Listing classification kinds needs {_RIGHT} or org_settings",
        )
    disabled = classifications.disabled_of(await read_org_overrides(_db()))
    return {
        "kinds": [_kind_doc(k, k.id not in disabled) for k in catalogue()],
        "canEdit": has_capability_request(request, "org_settings"),
    }


@router.put("/classification-kinds")
async def set_enabled_kinds(request: Request, body: EnabledKinds) -> dict:
    from provisa.api.admin._platform_guard import require_org_settings
    from provisa.core.org_settings import write_org_overrides
    from provisa.profiler import classifications
    from provisa.profiler.classify import catalogue

    require_org_settings(request)
    ids = {k.id for k in catalogue()}
    unknown = set(body.enabled) - ids
    if unknown:
        raise ApiError(
            422,
            "profile.classification_kind_unknown",
            f"unknown classification kinds {sorted(unknown)}",
            kind=", ".join(sorted(unknown)),
        )
    setting = classifications.setting_for(ids - set(body.enabled))
    await write_org_overrides(
        _db(), {classifications.SETTING_KEY: setting}, updated_by=_caller(request) or "unknown"
    )
    return await list_kinds(request)
