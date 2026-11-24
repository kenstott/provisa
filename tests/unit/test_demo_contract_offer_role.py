# Copyright (c) 2026 Kenneth Stott
# Canary: f679a0d0-5359-4707-bbec-2ab74d9a4c05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180: the demo products' default contracts list their columns.

A contract lists the columns its offer's role reads (offer_derivation.seen_columns, through the
real rights: effective_rights.data_view_from_state -> rights.column_served). The built-in default
offer acts as ``viewer``; every demo pets/vets/shelter column is granted to ``pet_analyst`` and
``data_steward`` only, so each demo product declares its ``default`` offer as ``pet_analyst``.
Nothing here is mocked: the runtime's tables are the config's, with their grants folded down the
role chains as the build folds them."""

from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from provisa.api.offer_derivation import seen_columns
from provisa.core.db import standard_role_dicts
from provisa.core.models import DEFAULT_OFFER
from provisa.security.inheritance import (
    expand_column_grants,
    narrow_own_domain_grants,
    role_chains,
    with_own_domains,
)

_ROOT = Path(__file__).resolve().parents[2]
_CASES = [
    ("config/provisa.yaml", "pet_health"),
    ("config/provisa.yaml", "workforce_ops"),
    ("config/provisa-install.yaml", "pet_health"),
    ("config/provisa-install.yaml", "workforce_ops"),
    ("demo/named/perf/config.yaml", "pet_health"),
    ("demo/named/perf/config.yaml", "workforce_ops"),
    ("demo/named/starthere/config.yaml", "pet_health"),
]


def _runtime(config: dict) -> SimpleNamespace:
    """What a build of ``config`` serves, as far as a contract's columns read it."""
    own = [
        {
            "id": r["id"],
            "capabilities": r.get("capabilities") or [],
            "parents": r.get("parents") or [],
            "domain_access": r.get("domain_access") or [],
        }
        for r in config.get("roles") or []
    ]
    roles = with_own_domains([*standard_role_dicts(), *own])
    chains = role_chains(roles)
    tables = [
        {
            "id": i,
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
    return SimpleNamespace(
        tables=tables,
        roles={r["id"]: r for r in roles},
        role_chains=chains,
        meta_roles={},
        meta_qualifiers={},
        rls_contexts={},
        masking_rules={},
    )


@pytest.mark.parametrize(("path", "product_id"), _CASES)
def test_the_demo_products_default_contract_lists_the_columns_its_role_is_granted(path, product_id):
    config = yaml.safe_load((_ROOT / path).read_text())
    product = next(p for p in config["data_products"] if p["id"] == product_id)
    declared = {o["name"]: o for o in product.get("offers") or []}
    role = declared["default"]["role_id"] if "default" in declared else DEFAULT_OFFER.role_id
    state = _runtime(config)
    by_name = {t["alias"] or t["table_name"]: t for t in state.tables}
    declared_columns = {
        t.get("alias") or t["table"]: t.get("columns") or [] for t in config["tables"]
    }
    assert product["tables"], product_id
    for name in product["tables"]:
        table = by_name[name]
        granted = {
            c["name"]
            for c in declared_columns[name]
            if set(c.get("visible_to") or ()) & set(state.role_chains[role])
        }
        read = {c.name for c in seen_columns(state, role, table) if c.read}
        assert granted, f"{path}: {role} is granted no column of {product_id}.{name}"
        assert read == granted, f"{path}: {role} reads {sorted(read)} of {product_id}.{name}"
