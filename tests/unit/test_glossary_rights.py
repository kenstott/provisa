# Copyright (c) 2026 Kenneth Stott
# Canary: b41f7d02-9c58-4a6e-90d3-27ee5c1a8f74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1590: the glossary's two rights, and which endpoint each one gates.

Reading a term to find out what a column means is not administering the org, so the surface came
off ``org_settings`` — under which an analyst was told they had no permission to view the page —
and onto ``glossary_read``. ``glossary_rw`` is curation on top of that. Asserted here:

* both rights exist, are distinct, and neither is platform bypass;
* every read endpoint names READ and every mutating one names RW, read from the router source so
  the assertion is about the wiring rather than about a mock;
* ``GET /ref`` stays on ``table_registration`` — it serves the Tables surface's hover popup;
* the gates admit a holder, admit platform bypass, and reject the other glossary right;
* the two role seeds (schema.sql and db.py) agree, analyst reads, and no seeded role curates
  without also reading — a role granted RW alone could not open the page.
"""

# Requirements: REQ-1590

from __future__ import annotations

import json
import re
import types
from pathlib import Path

import pytest

from provisa.api.errors import ApiError
from provisa.security.rights import Capability, has_platform_bypass

_ROUTER = Path(__file__).resolve().parents[2] / "provisa/api/admin/glossary_router.py"
_SCHEMA_SQL = Path(__file__).resolve().parents[2] / "provisa/core/schema.sql"


def test_the_glossary_rights_exist_and_are_not_platform_bypass():
    assert Capability.GLOSSARY_READ.value == "glossary_read"
    assert Capability.GLOSSARY_RW.value == "glossary_rw"
    assert Capability.GLOSSARY_READ.value != Capability.GLOSSARY_RW.value
    assert not has_platform_bypass({"glossary_read", "glossary_rw"})


# --- which right each endpoint names -------------------------------------------------------------


def _endpoint_gates() -> dict[str, str]:
    """method+path → the gate helper the handler's first statement calls."""
    source = _ROUTER.read_text()
    gates: dict[str, str] = {}
    for block in re.split(r"^@router\.", source, flags=re.M)[1:]:
        route = re.match(r'(\w+)\("([^"]*)"\)', block)
        assert route, block[:80]
        call = re.search(
            r"_require_(glossary_read|glossary_rw|table_registration)\(request\)", block
        )
        assert call, f"{route.group(1)} {route.group(2)} names no gate"
        gates[f"{route.group(1).upper()} {route.group(2)}"] = call.group(1)
    return gates


def test_reads_are_gated_on_glossary_read():
    gates = _endpoint_gates()
    assert gates["GET /terms"] == "glossary_read"
    assert gates["GET /terms/{term_id}"] == "glossary_read"


def test_the_ref_lookup_stays_on_table_registration():
    # It is the Tables surface's hover popup, and Tables already requires that right — moving it
    # onto a glossary right would take the popup away from a table curator who holds neither.
    assert _endpoint_gates()["GET /ref"] == "table_registration"


def test_every_other_endpoint_is_gated_on_glossary_rw():
    # Includes the three generation endpoints: they draft with an LLM, but what they write is the
    # org's vocabulary, so they are curation and not a read.
    curating = {k: v for k, v in _endpoint_gates().items() if not k.startswith("GET ")}
    assert curating, "the router exposes no mutating endpoints — the parse is wrong"
    assert set(curating.values()) == {"glossary_rw"}
    for name in ("generate", "definitions/generate", "relationships/generate"):
        assert any(name in k for k in curating), name


# --- the gates themselves ------------------------------------------------------------------------


def _request(caps: set[str] | None, *, user_id: str = "alice"):
    identity = None if caps is None else types.SimpleNamespace(user_id=user_id, roles=[])
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


@pytest.fixture
def resolve_caps(monkeypatch):
    def _install(caps: set[str]):
        import provisa.api.admin.capabilities as capmod

        monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: caps)

    return _install


@pytest.mark.parametrize(
    "gate_name,right,other",
    [
        ("_require_glossary_read", "glossary_read", "glossary_rw"),
        ("_require_glossary_rw", "glossary_rw", "glossary_read"),
    ],
)
class TestGlossaryGates:
    def _gate(self, gate_name):
        from provisa.api.admin import glossary_router

        return getattr(glossary_router, gate_name)

    def test_admits_the_holder(self, gate_name, right, other, resolve_caps):
        resolve_caps({right})
        self._gate(gate_name)(_request({right}))

    def test_admits_platform_bypass(self, gate_name, right, other, resolve_caps):
        resolve_caps({"admin"})
        self._gate(gate_name)(_request({"admin"}))

    def test_rejects_the_other_glossary_right(self, gate_name, right, other, resolve_caps):
        # The two are checked independently: neither implies the other. Holding RW alone leaves the
        # page unreachable, which is why a curator is seeded both.
        resolve_caps({other})
        with pytest.raises(ApiError) as err:
            self._gate(gate_name)(_request({other}))
        assert err.value.status_code == 403

    def test_rejects_org_settings(self, gate_name, right, other, resolve_caps):
        # The right the surface used to be gated on no longer opens it.
        resolve_caps({"org_settings"})
        with pytest.raises(ApiError):
            self._gate(gate_name)(_request({"org_settings"}))


# --- the seeds -----------------------------------------------------------------------------------


def _schema_sql_seed(role_id: str) -> set[str]:
    sql = _SCHEMA_SQL.read_text()
    seed = re.search(rf"'{role_id}',\s*'(\[[\s\S]*?\])'::jsonb", sql)
    assert seed, f"schema.sql: {role_id} seed not found"
    return set(json.loads(seed.group(1)))


def _db_seed(role_id: str) -> set[str]:
    from provisa.core.db import _SEED_ROLES

    for seeded_id, caps in _SEED_ROLES:
        if seeded_id == role_id:
            return set(caps)
    raise AssertionError(f"db.py _SEED_ROLES: {role_id} not found")


@pytest.mark.parametrize("role_id", ["org_admin", "analyst", "developer", "modeler"])
def test_the_two_seeds_agree_on_the_glossary_rights(role_id):
    glossary = {"glossary_read", "glossary_rw", "org_glossary_rw"}
    assert _schema_sql_seed(role_id) & glossary == _db_seed(role_id) & glossary


def test_an_analyst_reads_the_glossary_but_does_not_curate_it():
    caps = _db_seed("analyst")
    assert "glossary_read" in caps
    assert "glossary_rw" not in caps


@pytest.mark.parametrize("role_id", ["org_admin", "modeler"])
def test_the_curating_roles_hold_both(role_id):
    assert {"glossary_read", "glossary_rw"} <= _db_seed(role_id)


@pytest.mark.parametrize("role_id", ["org_admin", "analyst", "developer", "modeler"])
def test_no_seeded_role_curates_without_reading(role_id):
    caps = _db_seed(role_id)
    if "glossary_rw" in caps:
        assert "glossary_read" in caps


# --- REQ-1592: the org's glossary owner ----------------------------------------------------------


def test_the_org_glossary_right_is_a_third_and_distinct_right():
    assert Capability.ORG_GLOSSARY_RW.value == "org_glossary_rw"
    assert Capability.ORG_GLOSSARY_RW.value != Capability.GLOSSARY_RW.value
    assert not has_platform_bypass({"org_glossary_rw"})


@pytest.mark.parametrize("role_id", ["analyst", "developer", "modeler"])
def test_org_admin_alone_owns_the_org_glossary(role_id):
    # The override goes past every domain and every authorship claim, so it is seeded to the role
    # that already owns the org's data plane and to nothing else — modeler included.
    assert "org_glossary_rw" in _db_seed("org_admin")
    assert "org_glossary_rw" not in _db_seed(role_id), role_id


def test_the_org_owner_also_holds_the_ordinary_rights():
    # It widens what a curator may do; it does not replace the right that opens the surface.
    assert {"glossary_read", "glossary_rw"} <= _db_seed("org_admin")
