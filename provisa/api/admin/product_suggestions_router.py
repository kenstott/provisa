# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7f3d18-2e95-4c6a-9d41-5a8c1e7f2b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Suggesting a data product from listed tables, over HTTP (REQ-2180, amended 2026-10-28,
SUGGEST A PRODUCT FROM A LISTED TABLE).

* A signed-in reader suggests a product from tables of one domain the catalog lists to them,
  stating why (provisa.core.repositories.product_suggestion); a table they do not see is refused
  by name.
* The suggestion goes to the domain's data product owners and data stewards, as their entitlement
  rows name them (provisa.entitlements.owners), and each with an address is emailed through the
  org mail channel. A domain with no owner or steward has no one to receive it, and is refused.
* Suggestions are counted per table (amended 2026-11-24): the counts, and each table's
  suggestions, answer the domains whose suggestions go to the caller -- those they steward and
  those holding a data product they own. The Tables page shows a count as a badge on its table,
  which opens that table's suggestions.
"""

# Requirements: REQ-2180, REQ-2153, REQ-2219

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.capabilities import allowed_domains_request
from provisa.api.errors import ApiError
from provisa.core.repositories import product_suggestion as suggestions
from provisa.core.repositories.product_suggestion import SuggestionRefused

router = APIRouter(prefix="/admin/product-suggestions", tags=["admin"])

_ANONYMOUS = "anonymous"
#: The mail kind a suggestion's notices are recorded under (provisa.core.mail_stats).
MAIL_KIND = "product_suggestion"


class SuggestionBody(BaseModel):
    """Tables of one domain, by the names the catalog lists them under, and why a product of
    them is wanted."""

    domain_id: str
    tables: list[str]
    reason: str


def _state() -> Any:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "product_suggestion.database_not_connected", "Database not connected")
    return state


def _suggester(request: Request) -> str:
    user_id = getattr(getattr(request.state, "identity", None), "user_id", _ANONYMOUS)
    if user_id == _ANONYMOUS:
        raise ApiError(
            401,
            "product_suggestion.sign_in_required",
            "a product is suggested by a signed-in reader",
        )
    return str(user_id)


def _refused(refused: SuggestionRefused) -> ApiError:
    error = ApiError(refused.status, refused.code, str(refused))
    error.params = dict(refused.params)
    return error


async def _receivers(conn: Any) -> dict[str, set[str]]:
    """domain id -> its data stewards and the owners of its data products, by user id."""
    from provisa.core.schema_org import data_products
    from provisa.entitlements.owners import DOMAINS, holders, product_owners

    out: dict[str, set[str]] = {
        d: set(people) for d, people in (await holders(conn, DOMAINS)).items()
    }
    owners = await product_owners(conn)
    products = (
        await conn.execute_core(select(data_products.c.id, data_products.c.domain_id))
    ).fetchall()
    for product in products:
        out.setdefault(product.domain_id, set()).update(owners.get(product.id, ()))
    return out


async def recipients_of(conn: Any, domain_id: str) -> list[str]:
    """The domain's data stewards and the owners of its data products, by user id, sorted."""
    return sorted((await _receivers(conn)).get(domain_id, ()))


async def received_domains(conn: Any, user_id: str) -> frozenset[str]:
    """The domains whose suggestions go to ``user_id``: those they steward, and those holding a
    data product they own."""
    return frozenset(d for d, people in (await _receivers(conn)).items() if user_id in people)


def notice_text(org_id: str, suggestion: dict) -> tuple[str, str]:
    """The subject and body of the email a suggestion sends its recipients."""
    tables = ", ".join(f"{suggestion['domain_id']}.{t}" for t in suggestion["tables"])
    subject = f"[Provisa] A data product is suggested from {tables}"
    body = (
        f"{suggestion['suggested_by']} suggests a data product in domain "
        f"{suggestion['domain_id']} of org {org_id}, from the listed tables {tables}.\n\n"
        f"Why: {suggestion['reason']}\n\n"
        "Build a product from it, add the tables to an existing product, or decline it with a "
        "reason.\n"
    )
    return subject, body


async def _tell(conn: Any, org_id: str, suggestion: dict) -> None:
    """Email each recipient with an address but the suggester; a recipient without one has the
    stored suggestion as their record, as every notice does."""
    from provisa.auth.role_grants import emails_of, send_notice

    emails = await emails_of(conn, suggestion["recipients"])
    subject, body = notice_text(org_id, suggestion)
    for person in suggestion["recipients"]:
        if person in emails and person != suggestion["suggested_by"]:
            await send_notice(org_id, emails[person], subject, body, kind=MAIL_KIND)


def _view(row: dict) -> dict:
    return {**row, "created_at": row["created_at"].isoformat()}


@router.post("")
async def suggest(body: SuggestionBody, request: Request) -> dict:
    """Suggest a data product from listed tables of one domain; it goes to the domain's owners
    and stewards, by email."""
    suggester = _suggester(request)
    org_id = require_active_org_id(request)
    reach = allowed_domains_request(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        try:
            tables = await suggestions.seen_tables(conn, body.domain_id, body.tables, reach)
            recipients = await recipients_of(conn, body.domain_id)
            if not recipients:
                raise SuggestionRefused(
                    "product_suggestion.no_recipient",
                    f"domain {body.domain_id} has no data product owner or data steward to "
                    "receive a suggestion",
                    status=409,
                    domain=body.domain_id,
                )
            async with conn.transaction():
                stored = await suggestions.record(
                    conn,
                    domain_id=body.domain_id,
                    tables=tables,
                    suggested_by=suggester,
                    reason=body.reason,
                    recipients=recipients,
                )
        except SuggestionRefused as refused:
            raise _refused(refused) from refused
        await _tell(conn, org_id, stored)
    return _view(stored)


@router.get("/counts")
async def suggestion_counts(request: Request) -> list[dict]:
    """How many suggestions name each table of the domains whose suggestions go to the caller,
    most wanted first."""
    reader = _suggester(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        return await suggestions.counts(conn, await received_domains(conn, reader))


@router.get("")
async def table_suggestions(domain_id: str, table_name: str, request: Request) -> list[dict]:
    """The suggestions naming one table, newest first, for its domain's stewards and data
    product owners; anyone else is refused."""
    reader = _suggester(request)
    state = _state()
    async with state.model_db.acquire() as conn:
        if domain_id not in await received_domains(conn, reader):
            raise ApiError(
                403,
                "product_suggestion.not_a_recipient",
                f"the suggestions of domain {domain_id} are read by its data stewards and data "
                "product owners",
                domain=domain_id,
            )
        rows = await suggestions.of_table(conn, domain_id, table_name)
    return [_view(r) for r in rows]
