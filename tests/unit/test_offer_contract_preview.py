# Copyright (c) 2026 Kenneth Stott
# Canary: 12838658-d78f-497a-b6c9-ebb0e16e76bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (A PRODUCT PAGE PREVIEWS CONTRACTS AS THE ACTING ROLE): the product page previews each
offer's contract as the caller's acting role -- a meta-role when acting as a set -- while the
contract offered stays the offer's role's.

Over the demo model (config/provisa.yaml), through the real rights: the runtime's tables are the
config's with their grants folded down the role chains as the build folds them, and the meta-role
is built by ``ensure_meta_role``. Only the surface registration, which compiles a schema, is
replaced; the grants, chains and column rule are not."""

from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from provisa.api import offer_derivation
from provisa.compiler.rls import RLSContext
from provisa.core.db import standard_role_dicts
from provisa.security.inheritance import (
    expand_column_grants,
    narrow_own_domain_grants,
    role_chains,
    with_own_domains,
)
from tests.role_state import RoleState

_CONFIG = Path(__file__).resolve().parents[2] / "config" / "provisa.yaml"
_META = "meta:data_steward+pet_analyst"


@pytest.fixture
def demo(monkeypatch) -> tuple[RoleState, dict, dict[str, dict]]:
    """The demo model's runtime, pet_health and its tables by name."""

    def _register(state, role, rls):
        state.rls_contexts[role["id"]] = rls

    monkeypatch.setattr("provisa.api.app_loaders.register_role_surface", _register)
    config = yaml.safe_load(_CONFIG.read_text())
    own = [
        {
            "id": r["id"],
            "capabilities": r.get("capabilities") or [],
            "parents": r.get("parents") or [],
            "domain_access": r.get("domain_access") or [],
        }
        for r in config.get("roles") or []
    ]
    roles = [
        {**r, "session_vars": {}, "max_rows": None, "rate_limit": None, "description": r["id"]}
        for r in with_own_domains([*standard_role_dicts(), *own])
    ]
    chains = role_chains(roles)
    tables = [
        {
            "id": i,
            "source_id": t["source_id"],
            "domain_id": t["domain_id"],
            "table_name": t["table"],
            "alias": t.get("alias"),
            "columns": [
                {
                    "column_name": c["name"],
                    "data_type": c["data_type"],
                    "description": c.get("description"),
                    "visible_to": list(c.get("visible_to") or []),
                    "scope": c.get("scope"),
                }
                for c in t.get("columns") or []
            ],
            "write_grants": [],
        }
        for i, t in enumerate(copy.deepcopy(config["tables"]), start=1)
    ]
    expand_column_grants(tables, chains)
    narrow_own_domain_grants(tables, roles)
    members = ("pet_analyst", "data_steward")
    state = RoleState(
        roles={r["id"]: r for r in roles},
        rls_contexts={m: RLSContext(rules={}) for m in members},
        contexts={
            m: SimpleNamespace(
                tables={str(t["id"]): SimpleNamespace(table_id=t["id"]) for t in tables}
            )
            for m in members
        },
        masking_rules={},
        masked_columns=[],
        meta_resolved_at={},
        role_chains=chains,
        role_build_inputs={
            "role_chains": chains,
            "tables": tables,
            "withdrawn_tables": [],
            "metrics": [],
            "functions": [],
            "webhooks": [],
        },
    )
    state.tables = state.role_build_inputs["tables"]
    product = next(p for p in config["data_products"] if p["id"] == "pet_health")
    by_name = {t["alias"] or t["table_name"]: t for t in state.tables}
    return state, product, by_name


def test_previewed_as_the_meta_role_the_pets_and_vets_columns_appear(demo):
    state, product, by_name = demo
    assert product["tables"] == ["pets", "vets"]
    for name in product["tables"]:
        table = by_name[name]
        fields = {f.name for f in offer_derivation.preview(state, _META, table)}
        assert fields == {c["column_name"] for c in table["columns"]}, name
    assert state.meta_roles[_META] == ("data_steward", "pet_analyst")


def test_the_contract_offered_to_viewer_still_lists_no_column(demo):
    state, product, by_name = demo
    for name in product["tables"]:
        assert offer_derivation.preview(state, "viewer", by_name[name]) == [], name
        seen = offer_derivation.seen_columns(state, "viewer", by_name[name])
        assert seen and not any(c.read for c in seen), name


def test_a_column_is_previewed_by_its_sql_alias_where_one_is_set(demo):
    # REQ-2158 (amended 2026-11-25): consumers see each column by the name they query it by.
    state, _, by_name = demo
    pets = by_name["pets"]
    aliased, *rest = pets["columns"]
    aliased["alias"] = "pet_ref"
    fields = [f.name for f in offer_derivation.preview(state, _META, pets)]
    assert fields == ["pet_ref", *(c["column_name"] for c in rest)]
    assert aliased["column_name"] not in fields
