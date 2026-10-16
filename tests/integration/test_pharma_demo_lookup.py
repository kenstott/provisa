# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-a1b2-c3d4e5f6a7b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The real products the pharma demo's formulary is drawn from (REQ-2038).

The demo's private formulary must name products that are present in the AskAmerica health
tables, or every join to them is empty. This reads the live tables with the warehouse lane's
``ASKAMERICA_API_KEY`` and prints three lists to the log:

- products in a current shortage that have Medicaid utilization (shortage risk);
- products whose labeler has an Ongoing Class I or II recall, with Medicaid utilization (recall
  risk; a recall names its firm, not its NDC, so it matches by labeler name);
- products with Medicaid utilization in neither list (the formulary's unflagged lines).

It asserts only that each list is long enough to draw a formulary from.
"""

from __future__ import annotations

import os
import re

import pytest

from tests.conftest import as_deployment_org, deployment_org_on_this_node

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_SOURCE_ID = "askamerica-pharma-lookup"
_SCHEMAS = "health,ref,geo"
_SCHEMA = "health"
_COLD_START_SECONDS = 600
_YEAR = "2023"
#: How many rows of each list the log carries, and the fewest each must have.
_PRINT = 40
_ENOUGH = 10


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the pharma lookup needs a real key "
            "(a secret of the warehouse lane)"
        )
    return key


def _source():
    from provisa.core.models import Source, SourceType

    return Source(id=_SOURCE_ID, type=SourceType.govdata, username=_api_key(), database=_SCHEMAS)


@pytest.fixture(scope="module")
def endpoint(tmp_path_factory):
    from provisa.federation import pgwire_replica as pr

    source = _source()
    previous = os.environ.get("PROVISA_DATA_DIR")
    os.environ["PROVISA_DATA_DIR"] = str(tmp_path_factory.mktemp("askamerica-state"))
    try:
        with deployment_org_on_this_node():
            served = pr.ensure_endpoint(source, timeout=_COLD_START_SECONDS)
        yield served
    finally:
        with as_deployment_org():
            pr.stop_endpoint(source.id)
        if previous is None:
            del os.environ["PROVISA_DATA_DIR"]
        else:
            os.environ["PROVISA_DATA_DIR"] = previous


def _firm(name: str) -> str:
    """A firm name reduced to the words that identify it, for matching a recall to a labeler."""
    words = re.sub(r"[^a-z0-9 ]", " ", name.lower()).split()
    noise = {"inc", "llc", "ltd", "co", "corp", "corporation", "company", "usa", "us", "the", "lp"}
    return " ".join(w for w in words if w not in noise)


def _ndc_list(ndcs) -> str:
    return ", ".join("'" + n.replace("'", "''") + "'" for n in sorted(ndcs))


def _print(title: str, rows: list[dict]) -> None:
    print(f"\n=== {title}: {len(rows)} rows (first {_PRINT}) ===")
    for row in rows[:_PRINT]:
        print(" | ".join(f"{k}={v}" for k, v in row.items()))


@pytest.mark.asyncio(loop_scope="session")
async def test_the_formulary_can_be_drawn_from_real_products(endpoint):
    from provisa.federation import pgwire_replica as pr

    schema = pr.remote_schema(_source(), _SCHEMA)
    conn = await pr._pg_connect(endpoint.pgwire_host, endpoint.pgwire_port)
    try:
        shortages = await conn.fetch(
            f"SELECT product_ndc, generic_name_normalized, company_name, therapeutic_category, "
            f'availability, status FROM "{schema}"."fda_drug_shortages" '
            "WHERE status = 'Current' AND product_ndc IS NOT NULL"
        )
        recalls = await conn.fetch(
            f"SELECT recall_number, classification, recalling_firm, reason_for_recall, "
            f'recall_initiation_date FROM "{schema}"."fda_drug_recalls" '
            "WHERE status = 'Ongoing' AND classification IN ('Class I', 'Class II')"
        )
        recall_firms = {_firm(r["recalling_firm"]): r for r in recalls if r["recalling_firm"]}

        short_ndcs = {r["product_ndc"] for r in shortages}
        products = await conn.fetch(
            f"SELECT product_ndc, product_ndc9, generic_name_normalized, labeler_name, "
            f'dosage_form, route FROM "{schema}"."fda_ndc_products" '
            "WHERE product_type = 'HUMAN PRESCRIPTION DRUG' AND product_ndc9 IS NOT NULL"
        )
        by_ndc = {p["product_ndc"]: p for p in products}
        recall_ndcs = {
            p["product_ndc"]
            for p in products
            if p["labeler_name"] and _firm(p["labeler_name"]) in recall_firms
        }
        # Unflagged lines: a bounded sample of products in neither list.
        plain_ndcs = {n for n in sorted(by_ndc) if n not in short_ndcs and n not in recall_ndcs}
        plain_ndcs = set(sorted(plain_ndcs)[:: max(1, len(plain_ndcs) // 400)][:400])

        wanted = {
            by_ndc[n]["product_ndc9"]
            for n in (short_ndcs | recall_ndcs | plain_ndcs)
            if n in by_ndc
        }
        utilization = await conn.fetch(
            "SELECT product_ndc9, sum(CAST(number_of_prescriptions AS DOUBLE)) AS rx, "
            "sum(CAST(total_amount_reimbursed AS DOUBLE)) AS spend "
            f'FROM "{schema}"."medicaid_drug_utilization" '
            f"WHERE year = '{_YEAR}' AND state <> 'XX' AND product_ndc9 IN ({_ndc_list(wanted)}) "
            "GROUP BY product_ndc9"
        )
    finally:
        await conn.close()

    rx = {u["product_ndc9"]: u for u in utilization if u["rx"]}

    def rows(ndcs, extra):
        out = []
        for n in sorted(ndcs):
            p = by_ndc.get(n)
            if p is None or p["product_ndc9"] not in rx:
                continue
            u = rx[p["product_ndc9"]]
            out.append(
                {
                    "product_ndc": n,
                    "product_ndc9": p["product_ndc9"],
                    "generic": p["generic_name_normalized"],
                    "labeler": p["labeler_name"],
                    "form": p["dosage_form"],
                    "route": p["route"],
                    "rx_2023": int(u["rx"]),
                    "spend_2023": round(float(u["spend"] or 0)),
                    **extra(n, p),
                }
            )
        return sorted(out, key=lambda r: -r["rx_2023"])

    short_by_ndc = {r["product_ndc"]: r for r in shortages}
    shortage_rows = rows(
        short_ndcs,
        lambda n, p: {
            "shortage_category": short_by_ndc[n]["therapeutic_category"],
            "availability": short_by_ndc[n]["availability"],
        },
    )
    recall_rows = rows(
        recall_ndcs,
        lambda n, p: {
            "recall": recall_firms[_firm(p["labeler_name"])]["recall_number"],
            "class": recall_firms[_firm(p["labeler_name"])]["classification"],
            "recalling_firm": recall_firms[_firm(p["labeler_name"])]["recalling_firm"],
        },
    )
    plain_rows = rows(plain_ndcs, lambda n, p: {})

    print(
        f"\ncurrent shortages={len(shortages)} ongoing class I/II recalls={len(recalls)} "
        f"rx products={len(products)} recall-matched products={len(recall_ndcs)}"
    )
    _print("shortage products with Medicaid utilization", shortage_rows)
    _print("recall-firm products with Medicaid utilization", recall_rows)
    _print("unflagged products with Medicaid utilization", plain_rows)
    print("\n=== ongoing class I/II recalls (first 40) ===")
    for r in recalls[:_PRINT]:
        print(
            f"{r['recall_number']} | {r['classification']} | {r['recalling_firm']} | "
            f"{r['recall_initiation_date']} | {(r['reason_for_recall'] or '')[:120]}"
        )

    assert len(shortage_rows) >= _ENOUGH, len(shortage_rows)
    assert len(recall_rows) >= _ENOUGH, len(recall_rows)
    assert len(plain_rows) >= _ENOUGH, len(plain_rows)
