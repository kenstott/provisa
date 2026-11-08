# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7c1a9e-52d4-4b86-a0e7-9d2c6e418b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role narrows its parents (REQ-2155): what it holds is what it is given intersected with the
union of what they hold, for capabilities, domains and data_residency's regions alike. A right no
parent holds is refused by name, at save and at config load. A role with no capabilities can do
nothing, but may carry a name or session values, run a command that lists it, or be a building
block whose grants others inherit. A role's rows are its own rule AND its parents' ORed (REQ-1677).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError

from provisa.api.admin._platform_guard import role_narrowing_problem
from provisa.core.db import STANDARD_ROLE_RIGHTS, standard_role_dicts
from provisa.core.models import ProvisaConfig
from provisa.security import domain_tree
from provisa.security.effective_rights import capabilities_view, data_view_from_state
from provisa.security.inheritance import (
    effective_capabilities,
    expand_column_grants,
    expand_meta_grants,
    flatten_role_dicts,
    materialize_rls,
    narrowed,
    parent_map,
    right_not_in_parents,
    role_chains,
)
from provisa.security.mutation_authz import admit_command, command_reachable
from provisa.security.rights import (
    META_DOMAIN_ID,
    VIEWER_ROLE,
    role_domain_problem,
    served_roles,
)
from provisa.security.row_filters import with_owner_grant
from tests.rules import rule_of

REPO = Path(__file__).resolve().parents[2]


def _role(rid, caps=(), domains=("*",), parents=(), regions=()):
    return {
        "id": rid,
        "capabilities": list(caps),
        "domain_access": list(domains),
        "parents": list(parents),
        "residency_values": list(regions),
    }


def _flat(roles: list[dict]) -> dict[str, dict]:
    return {r["id"]: r for r in flatten_role_dicts(roles)}


def _rows(*own: dict) -> list[dict]:
    return [
        {**r, "domain_access": ["*"], "residency_values": []} for r in standard_role_dicts()
    ] + [*own]


class TestTheOneRule:
    def test_a_role_with_no_parents_holds_what_it_is_given(self):
        assert _flat([_role("solo", ["usage"], ["a"])])["solo"]["capabilities"] == ["usage"]

    def test_a_role_holds_what_it_is_given_of_what_its_parents_hold(self):
        roles = [
            _role("p", ["usage", "write"], ["a", "b"]),
            _role("c", ["usage", "full_results"], ["a", "x"], ["p"]),
        ]
        child = _flat(roles)["c"]
        assert child["capabilities"] == ["usage"]
        assert child["domain_access"] == ["a"]

    def test_the_union_of_every_parent_is_what_is_narrowed(self):
        roles = [
            _role("p", ["usage"], ["a"]),
            _role("q", ["write"], ["b"]),
            _role("c", ["usage", "write"], ["a", "b"], ["p", "q"]),
        ]
        child = _flat(roles)["c"]
        assert child["capabilities"] == ["usage", "write"]
        assert child["domain_access"] == ["a", "b"]

    def test_a_parents_narrowing_reaches_every_descendant(self):
        roles = [
            _role("g", ["usage", "write"], ["a", "b"]),
            _role("p", ["usage"], ["a"], ["g"]),
            _role("c", ["usage", "write"], ["a", "b"], ["p"]),
        ]
        child = _flat(roles)["c"]
        assert child["capabilities"] == ["usage"]
        assert child["domain_access"] == ["a"]

    def test_a_capability_a_standard_role_gains_reaches_a_child_only_when_given(self):
        rows = _rows(_role("c", ["usage"], ["a"], ["analyst"]))
        assert effective_capabilities("c", rows) == ["usage"]

    def test_a_standard_role_is_the_palette_and_adds(self):
        rows = _rows()
        assert set(effective_capabilities("developer", rows)) == STANDARD_ROLE_RIGHTS["developer"]

    @pytest.mark.parametrize(
        ("given", "held", "want"),
        [
            (["*"], [["*"]], {"*"}),
            (["a"], [["*"]], {"a"}),
            (["*"], [["a", "b"]], {"a", "b"}),
            (["a", "b"], [["b"], ["c"]], {"b"}),
            (["a"], [[]], set()),
        ],
    )
    def test_star_is_every_data_domain(self, given, held, want):
        assert narrowed(given, held) == want

    def test_a_system_domain_comes_only_with_the_capability_still_held(self):
        roles = [
            _role("p", ["catalog_read", "glossary_read"], ["*"]),
            _role("c", ["catalog_read"], ["a"], ["p"]),
        ]
        assert _flat(roles)["c"]["domain_access"] == ["a", "meta"]

    def test_a_parent_listing_a_business_unit_holds_the_domains_under_it(self):
        tree = {"bu": None, "bu_east": "bu", "other": None}
        roles = [_role("p", ["usage"], ["bu"]), _role("c", ["usage"], ["bu_east"], ["p"])]
        flat = {
            r["id"]: r for r in flatten_role_dicts(domain_tree.flatten_role_domains(roles, tree))
        }
        assert flat["c"]["domain_access"] == ["bu_east"]


class TestRegions:
    RESIDENCY = "data_residency"

    def test_a_child_covers_what_it_is_given_of_what_its_parents_cover(self):
        roles = [
            _role("p", [self.RESIDENCY], regions=["eu", "us"]),
            _role("c", [self.RESIDENCY], parents=["p"], regions=["eu", "apac"]),
        ]
        assert _flat(roles)["c"]["residency_values"] == ["eu"]

    def test_a_role_without_the_right_covers_nothing(self):
        roles = [
            _role("p", [self.RESIDENCY, "usage"], regions=["eu"]),
            _role("c", ["usage"], parents=["p"], regions=["eu"]),
        ]
        assert _flat(roles)["c"]["residency_values"] == []

    def test_a_parents_regions_narrow_every_descendant(self):
        roles = [
            _role("g", [self.RESIDENCY], regions=["eu", "us"]),
            _role("p", [self.RESIDENCY], parents=["g"], regions=["eu"]),
            _role("c", [self.RESIDENCY], parents=["p"], regions=["eu", "us"]),
        ]
        assert _flat(roles)["c"]["residency_values"] == ["eu"]


class TestRefusedByName:
    def _refused(self, *own, role="c", **given):
        row = next(r for r in own if r["id"] == role)
        return right_not_in_parents(
            role,
            row["parents"],
            given.get("capabilities", row["capabilities"]),
            given.get("domains", row["domain_access"]),
            given.get("regions", row["residency_values"]),
            _rows(*own),
            given.get("tree", {}),
        )

    def test_a_capability_no_parent_holds(self):
        child = _role("c", ["write"], ["a"], ["analyst"])
        assert self._refused(child) == ("capability", "write", "no parent holds it (analyst)")

    def test_a_domain_no_parent_reaches(self):
        parent, child = _role("p", ["usage"], ["a"]), _role("c", ["usage"], ["b"], ["p"])
        assert self._refused(parent, child) == ("domain", "b", "no parent reaches it (p)")

    def test_star_under_a_parent_that_lists_domains(self):
        parent, child = _role("p", ["usage"], ["a"]), _role("c", ["usage"], ["*"], ["p"])
        assert self._refused(parent, child) == ("domain", "*", "no parent reaches it (p)")

    def test_a_region_no_parent_covers(self):
        parent = _role("p", ["data_residency"], ["*"], regions=["eu"])
        child = _role("c", ["data_residency"], ["a"], ["p"], ["us"])
        assert self._refused(parent, child) == ("region", "us", "no parent covers it (p)")

    def test_a_domain_under_one_its_parent_lists_is_held(self):
        parent = _role("p", ["usage"], ["bu"])
        child = _role("c", ["usage"], ["bu_east"], ["p"])
        assert self._refused(parent, child, tree={"bu": None, "bu_east": "bu"}) is None

    def test_a_subset_is_not_refused(self):
        child = _role("c", ["usage", "glossary_read"], ["a"], ["analyst"])
        assert self._refused(child) is None

    def test_a_role_with_no_parents_narrows_nothing(self):
        assert self._refused(_role("c", ["write"], ["a"])) is None


def _config(*roles: dict, domains=("a",)) -> dict:
    return {
        "sources": [],
        "domains": [{"id": d, "description": f"Domain {d}."} for d in domains],
        "tables": [],
        "relationships": [],
        "roles": [{"description": "A role.", **r} for r in roles],
        "rls_rules": [],
    }


class TestSaveRefusal:
    def test_the_refusal_names_the_role_the_kind_and_the_right(self):
        problem = role_narrowing_problem(
            "c", ["analyst"], ["write"], ["a"], [], _rows(_role("c", ["write"], ["a"])), {}
        )
        assert problem is not None and problem.code == "roles.right_not_in_parents"
        assert problem.params == {"role": "c", "kind": "capability", "right": "write"}
        assert "Role 'c' cannot hold capability 'write'" in str(problem.detail)

    def test_a_subset_is_saved(self):
        assert (
            role_narrowing_problem(
                "c", ["analyst"], ["usage"], ["a"], [], _rows(_role("c", ["usage"], ["a"])), {}
            )
            is None
        )


class TestAnOwnerFieldStands:
    def test_rw_from_an_owner_field_is_held_though_no_parent_holds_it(self):
        rows = with_owner_grant(_rows(_role("team", ["usage"], ["a"], ["analyst"])), {"team"})
        assert "data_product_rw" in _flat(rows)["team"]["capabilities"]
        assert "data_product_rw" in effective_capabilities("team", rows)

    def test_a_role_it_does_not_name_gains_nothing(self):
        rows = with_owner_grant(_rows(_role("other", ["usage"], ["a"], ["analyst"])), {"team"})
        assert _flat(rows)["other"]["capabilities"] == ["usage"]


class TestConfigLoad:
    def test_a_hand_coded_right_no_parent_holds_fails_the_load_by_name(self):
        child = {
            "id": "c",
            "capabilities": ["write"],
            "domain_access": ["a"],
            "parents": ["analyst"],
        }
        with pytest.raises(ValidationError, match=r"Role 'c' cannot hold capability 'write'"):
            ProvisaConfig.model_validate(_config(child))

    def test_a_hand_coded_domain_no_parent_reaches_fails_the_load_by_name(self):
        parent = {"id": "p", "capabilities": ["usage"], "domain_access": ["a"]}
        child = {
            "id": "c",
            "capabilities": ["usage"],
            "domain_access": ["b"],
            "parents": ["p"],
        }
        with pytest.raises(ValidationError, match=r"Role 'c' cannot hold domain 'b'"):
            ProvisaConfig.model_validate(_config(parent, child, domains=("a", "b")))

    def test_a_subset_loads(self):
        child = {
            "id": "c",
            "capabilities": ["usage"],
            "domain_access": ["a"],
            "parents": ["analyst"],
        }
        assert ProvisaConfig.model_validate(_config(child)).roles[0].id == "c"

    def test_a_role_with_no_capabilities_and_no_domain_loads(self):
        role = {"id": "name_only", "capabilities": [], "domain_access": []}
        assert ProvisaConfig.model_validate(_config(role)).roles[0].domain_access == []


def _role_files() -> list[Path]:
    named = sorted((REPO / "demo" / "named").glob("*/config.yaml"))
    return [*sorted((REPO / "config").glob("provisa*.yaml")), *named]


@pytest.mark.parametrize("path", _role_files(), ids=lambda p: str(p.relative_to(REPO)))
def test_every_demo_and_config_role_is_a_subset_of_its_parents(path: Path):
    raw = yaml.safe_load(path.read_text())
    own = [
        {"residency_values": [], **r}
        for r in (raw or {}).get("roles") or []
        if isinstance(r, dict) and "id" in r
    ]
    rows = _rows(*own)
    ids = {r["id"] for r in rows}
    for role in own:
        parents = role.get("parents") or []
        if not set(parents) <= ids:
            continue
        refused = right_not_in_parents(
            role["id"],
            parents,
            role.get("capabilities") or [],
            role.get("domain_access") or [],
            role["residency_values"],
            rows,
            {},
        )
        assert refused is None, f"{role['id']}: {refused}"


def test_pet_analyst_holds_what_it_lists():
    """pet_analyst, the role the maintainer reported: what narrowing leaves it is what it lists."""
    raw = yaml.safe_load((REPO / "config" / "provisa.yaml").read_text())
    pet = next(r for r in raw["roles"] if r["id"] == "pet_analyst")
    assert set(effective_capabilities("pet_analyst", _rows(pet))) == STANDARD_ROLE_RIGHTS["analyst"]
    assert "catalog_read" not in pet["capabilities"]


class TestViewerHoldsNoCatalogRead:
    """REQ-1132: catalog_read is held only by a role that names it."""

    def test_viewer_and_a_role_built_on_it_do_not_hold_it(self):
        assert "catalog_read" not in STANDARD_ROLE_RIGHTS["viewer"]
        assert "catalog_read" not in STANDARD_ROLE_RIGHTS["analyst"]

    @pytest.mark.parametrize("role_id", ["dq_reader", "dq_steward", "data_product_owner"])
    def test_a_role_whose_job_is_its_own_tables_does_not_hold_it(self, role_id):
        assert "catalog_read" not in STANDARD_ROLE_RIGHTS[role_id]

    @pytest.mark.parametrize(
        "role_id",
        [
            "developer",
            "modeler",
            "source_admin",
            "security_admin",
            "data_steward",
            "org_operator",
            "org_admin",
            "sandbox",
        ],
    )
    def test_a_role_working_across_the_model_holds_it(self, role_id):
        assert "catalog_read" in STANDARD_ROLE_RIGHTS[role_id]


class TestDataAccessShowsTheDefaultTierAndNoMore:
    """dq_reader's Data access tab: no meta table beyond the default tier."""

    def _view(self) -> set[str]:
        rows = [{**r, "residency_values": [], "max_rows": None} for r in standard_role_dicts()]
        flat = {r["id"]: r for r in flatten_role_dicts(rows)}
        chains = role_chains(rows)

        def meta(tid: int, name: str, *columns: str) -> dict:
            return {
                "id": tid,
                "domain_id": META_DOMAIN_ID,
                "table_name": name,
                "columns": [
                    {"column_name": c, "visible_to": [VIEWER_ROLE], "scope": "domain"}
                    for c in columns
                ],
                "write_grants": [],
            }

        tables = [
            meta(1, "registered_tables_meta", "id", "visible_to"),
            meta(2, "table_keys", "table_id"),
            meta(3, "rls_rules", "id", "filter_expr"),
            meta(4, "roles", "id", "capabilities"),
            meta(5, "tracked_webhooks", "name", "url"),
        ]
        expand_column_grants(tables, chains)
        expand_meta_grants(tables, flat.values())
        state = SimpleNamespace(
            roles=flat,
            tables=tables,
            rls_contexts={},
            masking_rules={},
            meta_roles={},
            role_chains=chains,
        )
        return {
            t["table_name"]: {c["name"] for c in t["columns"]}
            for t in data_view_from_state(
                state,
                "dq_reader",
                parents=parent_map(rows),
                stored={},
                readable=lambda *_: True,
                governance=True,
            )
        }

    def test_nothing_of_meta_beyond_the_default_tier(self):
        shown = self._view()
        assert not {"rls_rules", "roles", "tracked_webhooks"} & set(shown)

    def test_the_default_tier_is_there_core_columns_alone(self):
        assert self._view() == {"registered_tables_meta": {"id"}, "table_keys": {"table_id"}}


class TestNoCapabilities:
    def test_a_role_with_no_capabilities_may_list_no_domain(self):
        assert role_domain_problem("name_only", [], []) is None

    def test_a_role_holding_a_capability_must_still_list_one(self):
        assert role_domain_problem("r", ["usage"], []) is not None

    def test_it_holds_no_domain_whatever_it_lists(self):
        assert _flat([_role("empty", [], ["*"])])["empty"]["domain_access"] == []

    def test_unselecting_every_capability_takes_the_reach_with_it(self):
        rows = _rows(_role("c", [], ["*"], ["analyst"]))
        assert _flat(rows)["c"]["domain_access"] == []
        assert _flat(rows)["c"]["capabilities"] == []

    def test_a_parent_with_no_capabilities_hands_down_no_domain(self):
        roles = [
            _role("block", [], ["a"]),
            _role("p", ["usage"], ["b"]),
            _role("c", ["usage"], ["a", "b"], ["block", "p"]),
        ]
        assert _flat(roles)["c"]["domain_access"] == ["b"]

    def test_it_keeps_its_name_and_session_values(self):
        role = {**_role("empty", [], []), "session_vars": {"region": "eu"}}
        flat = _flat([role])["empty"]
        assert flat["id"] == "empty" and flat["session_vars"] == {"region": "eu"}

    def test_it_is_served_no_public_column(self):
        column = {"column_name": "id", "visible_to": ["empty", "p"], "scope": "public"}
        roles = {r["id"]: r for r in flatten_role_dicts([_role("empty"), _role("p", ["usage"])])}
        assert served_roles(roles, {"empty": ["empty"], "p": ["p"]}, "a", column) == {"p"}

    def test_its_data_access_is_empty(self):
        table = {
            "id": 1,
            "domain_id": "a",
            "table_name": "t",
            "columns": [{"column_name": "id", "visible_to": ["empty"], "scope": "public"}],
        }
        state = type(
            "S",
            (),
            {
                "roles": {"empty": _role("empty", [], [])},
                "tables": [table],
                "rls_contexts": {},
                "masking_rules": {},
            },
        )()
        assert (
            data_view_from_state(
                state,
                "empty",
                parents={"empty": []},
                stored={},
                readable=lambda *_: True,
                governance=True,
            )
            == []
        )


class TestAroleJustToRunACommand:
    COMMAND = {"name": "archive", "domain_id": "a", "kind": "mutation", "visible_to": ["runner"]}

    def test_a_command_naming_it_is_reachable_with_no_domain(self):
        assert command_reachable(self.COMMAND, _role("runner", [], []))

    def test_a_command_naming_it_is_admitted_with_no_write_right(self):
        admit_command(self.COMMAND, _role("runner", [], []), "archive")

    def test_a_command_not_naming_it_is_not(self):
        assert not command_reachable(
            {**self.COMMAND, "visible_to": ["someone"]}, _role("runner", [], [])
        )

    def test_a_role_holding_a_capability_still_needs_the_domain_and_the_right(self):
        role = _role("runner", ["usage"], ["b"])
        assert not command_reachable(self.COMMAND, role)


class TestABuildingBlock:
    def test_a_role_including_it_gets_its_grants_and_capabilities_from_its_other_parents(self):
        roles = [
            _role("block", [], []),
            _role("p", ["usage"], ["a"]),
            _role("c", ["usage"], ["a"], ["block", "p"]),
        ]
        column = {"column_name": "id", "visible_to": ["block"]}
        expand_column_grants([{"columns": [column]}], role_chains(roles))
        flat = _flat(roles)
        served = served_roles(flat, role_chains(roles), "a", column)
        assert served == {"c"}
        assert flat["c"]["capabilities"] == ["usage"]


class TestTheLineageSaysWhatIsHeld:
    def test_a_right_unselected_is_no_longer_held_or_listed(self):
        rows = _rows(_role("c", ["usage"], ["a"], ["analyst"]))
        view = capabilities_view("c", rows, standard={r["id"] for r in standard_role_dicts()})
        assert [c["right"] for c in view["capabilities"]] == ["usage"]
        assert view["capabilities"][0]["from"] == ["analyst"]
        assert view["adds"]["c"] == []


TABLES = [{"id": 10, "domain_id": "sales"}, {"id": 11, "domain_id": "sales"}]


def _rule(role, expr, table_id=None, domain_id=None, rights="R"):
    return {
        "id": 1,
        "role_id": role,
        "table_id": table_id,
        "domain_id": domain_id,
        "filter_expr": expr,
        "rights": rights,
    }


def _resolved(rules, parents, role, table=10) -> str | None:
    table_rules = {
        r["role_id"]: r["filter_expr"]
        for r in materialize_rls(rules, TABLES, parents)
        if r["table_id"] == table
    }
    return table_rules.get(role)


class TestRowRules:
    def test_a_childs_rule_is_anded_with_its_parents(self):
        rules = [_rule("p", "region = 'east'", 10), _rule("c", "tier = 1", 10)]
        assert _resolved(rules, {"p": [], "c": ["p"]}, "c") == rule_of(
            "(tier = 1) AND (region = 'east')"
        )

    def test_a_role_with_no_rule_reads_under_its_parents(self):
        rules = [_rule("p", "region = 'east'", 10)]
        assert _resolved(rules, {"p": [], "c": ["p"]}, "c") == rule_of("region = 'east'")

    def test_its_parents_are_ored(self):
        rules = [_rule("p", "a = 1", 10), _rule("q", "b = 2", 10), _rule("c", "x = 3", 10)]
        assert _resolved(rules, {"p": [], "q": [], "c": ["p", "q"]}, "c") == rule_of(
            "(x = 3) AND ((a = 1) OR (b = 2))"
        )

    def test_a_parent_with_no_rule_stands_for_every_row(self):
        rules = [_rule("p", "a = 1", 10), _rule("c", "x = 3", 10)]
        assert _resolved(rules, {"p": [], "q": [], "c": ["p", "q"]}, "c") == rule_of("x = 3")

    def test_no_rule_anywhere_is_every_row(self):
        assert _resolved([], {"p": [], "c": ["p"]}, "c") is None

    def test_it_is_one_rule_for_one_role_and_target(self):
        rules = [_rule("p", "a = 1", 10), _rule("c", "x = 3", 10)]
        out = materialize_rls(rules, TABLES, {"p": [], "c": ["p"]})
        assert len([r for r in out if r["role_id"] == "c" and r["table_id"] == 10]) == 1

    def test_a_childs_domain_rule_is_anded_with_its_parents_table_rule(self):
        rules = [_rule("p", "a = 1", 10), _rule("c", "x = 3", domain_id="sales")]
        assert _resolved(rules, {"p": [], "c": ["p"]}, "c") == rule_of("(x = 3) AND (a = 1)")

    def test_narrowing_reaches_the_grandchild(self):
        rules = [_rule("g", "a = 1", 10), _rule("p", "b = 2", 10), _rule("c", "x = 3", 10)]
        assert _resolved(rules, {"g": [], "p": ["g"], "c": ["p"]}, "c") == rule_of(
            "(x = 3) AND ((b = 2) AND (a = 1))"
        )

    def test_a_parents_own_rule_is_not_changed(self):
        rules = [_rule("p", "a = 1", 10), _rule("c", "x = 3", 10)]
        assert _resolved(rules, {"p": [], "c": ["p"]}, "p") == rule_of("a = 1")

    def test_a_cycle_is_refused(self):
        with pytest.raises(ValueError, match="cycle"):
            materialize_rls([_rule("a", "x = 1", 10)], TABLES, {"a": ["b"], "b": ["a"]})


def test_parent_map_reads_a_role_row():
    assert parent_map([_role("c", parents=["p"])]) == {"c": ["p"]}
