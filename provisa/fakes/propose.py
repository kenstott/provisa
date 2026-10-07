# Copyright (c) 2026 Kenneth Stott
# Canary: 7e1b8fff-61e2-477d-9378-804687c638d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fill from profile (REQ-1494): a fake or a synthetic rule proposed for each column from the
table's latest profile run -- its plausible types, its value frequencies and lengths -- and the
column's name and tags.

Fakes are proposed for identifying columns only: a column tagged pii, or one whose plausible type
is a person's name, an email address, a phone number or an address, or whose name says a
government identifier or an account number. A pii column with no confident match is proposed
nothing and named, for the operator to decide. Every other column may be proposed a synthetic
rule: the profile's own distribution for a number or a time, pinned to the run; its categories
where the profile shows real repetition; bool() for a boolean; text to the profiled lengths for
free text. Nothing is saved: the proposals fill the editor's fields.
"""

# Requirements: REQ-1494

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

#: How confident a plausible type must be to propose from it.
CONFIDENT = 0.7

#: Real repetition: a column of few distinct values, each held by at least this many rows.
REPEATED_ROWS = 5

_FAKES_BY_TYPE = {
    "email": "email()",
    "person_name_first": "first_name()",
    "person_name_last": "last_name()",
    "person_name_full": "name()",
    "phone": "phone_number()",
    "address_street": "street_address()",
    "address_city": "city()",
    "address_region": "state()",
    "address_postal_code": "postcode()",
    "address_country": "country()",
}

# Government identifiers and account numbers, by the column's name.
_FAKES_BY_NAME: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ssn()", ("ssn", "social_security", "social_security_number", "national_id", "nin")),
    ("ein()", ("ein", "tax_id", "taxid", "tin", "itin")),
    ("passport_number()", ("passport", "passport_number", "passport_no")),
    ("iban()", ("iban",)),
    ("bban()", ("account", "account_number", "account_no", "acct", "acct_no", "bank_account")),
    ("credit_card_number()", ("card_number", "credit_card", "cc_number", "pan")),
    ("license_plate()", ("license_plate", "plate", "registration_plate")),
)


@dataclass(frozen=True)
class ProfiledColumnFacts:
    """What a column's latest profile run says about it."""

    name: str
    family: str
    length_max: int | None  # text columns only
    plausible_type: str
    confidence: float
    frequencies: tuple[int, ...]  # the full value-frequency table's counts, when recorded


@dataclass
class Proposals:
    run_id: str
    columns: dict[str, dict[str, str]] = field(default_factory=dict)
    unmatched_pii: list[str] = field(default_factory=list)


def _tokens(name: str) -> set[str]:
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()
    parts = [p for p in re.split(r"[^a-z0-9]+", snake) if p]
    return set(parts) | {"_".join(parts)}


def _identifying(c: ProfiledColumnFacts) -> str | None:
    """The fake for an identifying column, or None."""
    tokens = _tokens(c.name)
    for fake, names in _FAKES_BY_NAME:
        if tokens & set(names):
            return fake
    if c.confidence >= CONFIDENT:
        return _FAKES_BY_TYPE.get(c.plausible_type)
    return None


def _rule(c: ProfiledColumnFacts, run_id: str) -> str | None:
    """A synthetic rule for a column that is not identifying, or None."""
    if c.family == "boolean" or c.plausible_type == "boolean":
        return "bool()"
    if c.family in ("numeric", "temporal") and c.plausible_type != "identifier":
        return f"profile(run={run_id!r})"
    repeated = bool(c.frequencies) and min(c.frequencies) >= REPEATED_ROWS
    if c.plausible_type == "category" and repeated:
        return "categories()"
    if c.plausible_type == "free_text" and c.length_max:
        return f"text(max_nb_chars={max(c.length_max, 5)})"
    return None


def propose(
    run_id: str, columns: list[ProfiledColumnFacts], pii: set[str], declared: set[str]
) -> Proposals:
    """Proposals for each column not in ``declared`` (a column with a fake or rule already)."""
    out = Proposals(run_id)
    for c in columns:
        if c.name in declared:
            continue
        fake = _identifying(c)
        if fake is not None:
            out.columns[c.name] = {"fake": fake}
        elif c.name in pii:
            out.unmatched_pii.append(c.name)
        else:
            rule = _rule(c, run_id)
            if rule is not None:
                out.columns[c.name] = {"syntheticRule": rule}
    return out


async def latest_facts(
    conn: Any, *, org_id: str, env: str | None, reg: dict
) -> tuple[str, list[ProfiledColumnFacts]] | None:
    """The latest succeeded profile run of ``reg`` in ``env`` and its columns' facts, by
    registered column name; None when the table has none."""
    from sqlalchemy import select

    from provisa.core.environments import org_schema
    from provisa.profiler.schema import result_sa_table
    from provisa.synthetic.run import _profile_table_id, _qualified

    schema = org_schema(org_id, env)
    tid = await _profile_table_id(conn, schema, reg)

    def rel(kind: str) -> Any:
        return _qualified(result_sa_table(reg["table_name"], tid, kind), schema)

    runs = rel("runs")
    from provisa.profiler.declared import measured

    row = (
        await conn.execute_core(
            select(runs.c.run_id)
            .where(runs.c.status == "succeeded", measured(runs))  # REQ-1942: what the data holds
            .order_by(runs.c.run_time.desc())
            .limit(1)
        )
    ).fetchone()
    if row is None:
        return None
    run_id = row[0]

    async def rows(kind: str) -> list[dict]:
        r = rel(kind)
        result = await conn.execute_core(select(r).where(r.c.run_id == run_id))
        return [dict(x._mapping) for x in result.fetchall()]

    # A run labels every column it profiles (provisa.profiler.run.result_rows).
    plausible = {p["column_name"]: p for p in await rows("plausible_type")}
    freq: dict[str, list[int]] = {}
    for v in await rows("top_values"):
        if v["kind"] != "top" and v["value"] is not None:
            freq.setdefault(v["column_name"], []).append(v["row_count"])
    facts = []
    for c in await rows("columns"):
        p = plausible[c["column_name"]]
        facts.append(
            ProfiledColumnFacts(
                name=c["physical_column"],
                family=c["family"],
                length_max=c["length_max"],
                plausible_type=p["plausible_type"],
                confidence=float(p["confidence"]),
                frequencies=tuple(freq.get(c["column_name"], ())),  # empty: no full table recorded
            )
        )
    return run_id, facts
