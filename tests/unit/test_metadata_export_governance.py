# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9e5b81-73a4-4f60-9d18-6ab204f7e3c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1071: masked / RLS / visibility facts project outward as tags, never as rule bodies."""

# Requirements: REQ-039, REQ-040, REQ-041, REQ-1071

from __future__ import annotations

import dataclasses

import pytest

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.governance import build_governance_tags
from provisa.api.metadata_export.model import GovernanceSignal, MetadataSnapshot
from provisa.api.metadata_export.refs import UnknownTableError
from provisa.core.models import (
    Column,
    Domain,
    ProvisaConfig,
    RLSRule,
    Role,
    Source,
    SourceType,
    Table,
    TagAssignment,
)

MASK_PATTERN = r"(\d{3})-(\d{2})-(\d{4})"
MASK_REPLACE = "XXX-XX-\\3"
RLS_FILTER = "region_id = current_setting('provisa.region')"


def _table(name: str, columns: list[Column], domain_id: str = "sales") -> Table:
    return Table(
        source_id="wh",
        domain_id=domain_id,
        schema_name="public",
        table_name=name,
        columns=columns,
        # Marked so the table publishes: only Data Product tables enter the snapshot.
        product_id="prod",
    )


def _config(*, tables: list[Table], rls: list[RLSRule] | None = None) -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[Domain(id="sales", description="Sales", steward="s")],
        tables=tables,
        relationships=[],
        roles=[
            Role(id="analyst", description="Test role.", capabilities=[], domain_access=["*"]),
            Role(id="admin", description="Test role.", capabilities=[], domain_access=["*"]),
        ],
        rls_rules=rls or [],
    )


def _tags(config: ProvisaConfig, signal: GovernanceSignal):
    return [t for t in build_governance_tags(config, "acme") if t.signal is signal]


def test_masked_column_tagged_with_kind_and_exempt_roles():
    config = _config(
        tables=[
            _table(
                "customers",
                [
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["analyst", "admin"],
                        masks=[
                            {"to": ["admin"], "mask": None},
                            {
                                "mask": {
                                    "type": "regex",
                                    "pattern": MASK_PATTERN,
                                    "replace": MASK_REPLACE,
                                },
                            },
                        ],
                    )
                ],
            )
        ]
    )
    tags = _tags(config, GovernanceSignal.MASKED)
    assert len(tags) == 1
    tag = tags[0]
    assert tag.asset.fqn() == "wh.public.customers.ssn"
    # REQ-2059: the entry (its place in the list) and its kind.
    assert tag.rule_id == "mask:provisa://acme/sales/tables/customers#field:ssn:1:regex"
    assert tag.exempt_roles == ("admin",)
    assert tag.restricted_roles == ("analyst",)


def test_unmasked_column_produces_no_mask_tag():
    config = _config(
        tables=[
            _table(
                "customers", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])]
            )
        ]
    )
    assert _tags(config, GovernanceSignal.MASKED) == []


def test_visibility_restriction_names_the_roles_that_cannot_see_the_column():
    config = _config(
        tables=[
            _table(
                "customers",
                [
                    Column(name="id", data_type="int", visible_to=["analyst", "admin"]),
                    Column(name="salary", data_type="numeric", visible_to=["admin"]),
                ],
            )
        ]
    )
    tags = _tags(config, GovernanceSignal.VISIBILITY_RESTRICTED)
    assert [t.asset.fqn() for t in tags] == ["wh.public.customers.salary"]
    assert tags[0].restricted_roles == ("analyst",)
    assert tags[0].exempt_roles == ("admin",)


def test_column_visible_to_every_role_is_not_tagged():
    """Tagging a universally visible column would report a restriction the engine never applies."""
    config = _config(
        tables=[
            _table(
                "customers", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])]
            )
        ]
    )
    assert _tags(config, GovernanceSignal.VISIBILITY_RESTRICTED) == []


def test_table_scoped_rls_rule_tags_only_that_table():
    config = _config(
        tables=[
            _table("orders", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])]),
            _table(
                "regions", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])]
            ),
        ],
        rls=[
            RLSRule(
                table_id="orders", role_id="analyst", filter=RLS_FILTER, description="Test rule."
            )
        ],
    )
    tags = _tags(config, GovernanceSignal.RLS_RESTRICTED)
    assert [t.asset.fqn() for t in tags] == ["wh.public.orders"]
    assert tags[0].rule_id == "rls:provisa://acme/sales/tables/orders:analyst"
    assert tags[0].restricted_roles == ("analyst",)
    assert tags[0].exempt_roles == ("admin",)


def test_domain_scoped_rls_rule_tags_every_table_in_the_domain():
    config = _config(
        tables=[
            _table("orders", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])]),
            _table(
                "regions", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])]
            ),
            _table(
                "lab_runs",
                [Column(name="id", data_type="int", visible_to=["analyst", "admin"])],
                domain_id="lab",
            ),
        ],
        rls=[
            RLSRule(
                domain_id="sales", role_id="analyst", filter=RLS_FILTER, description="Test rule."
            )
        ],
    )
    tags = _tags(config, GovernanceSignal.RLS_RESTRICTED)
    assert {t.asset.fqn() for t in tags} == {"wh.public.orders", "wh.public.regions"}
    assert all(t.rule_id == "rls:provisa://acme/sales:analyst" for t in tags)


def test_tag_scoped_rls_rule_tags_every_table_carrying_the_tag():
    # REQ-1384: the rule's scope is the tag; a column-level assignment does not tag its table.
    readable = [Column(name="id", data_type="int", visible_to=["analyst", "admin"])]
    config = _config(
        tables=[_table("orders", readable), _table("regions", readable), _table("staff", readable)],
        rls=[
            RLSRule(
                tag_id="regional", role_id="analyst", filter=RLS_FILTER, description="Test rule."
            )
        ],
    )
    config.tag_assignments = [
        TagAssignment(tag_id="regional", object_type="table", table_ref="wh.public.orders"),
        TagAssignment(tag_id="regional:emea", object_type="table", table_ref="wh.public.regions"),
        TagAssignment(
            tag_id="regional", object_type="column", table_ref="wh.public.staff", column_name="id"
        ),
    ]
    tags = _tags(config, GovernanceSignal.RLS_RESTRICTED)
    assert {t.asset.fqn() for t in tags} == {"wh.public.orders", "wh.public.regions"}
    assert all(t.rule_id == "rls:provisa://acme/tags/regional:analyst" for t in tags)


def test_rls_rule_naming_an_unknown_table_is_refused():
    config = _config(
        tables=[
            _table("orders", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])])
        ],
        rls=[
            RLSRule(
                table_id="ghost", role_id="analyst", filter=RLS_FILTER, description="Test rule."
            )
        ],
    )
    with pytest.raises(UnknownTableError):
        build_governance_tags(config, "acme")


def test_rls_rule_with_no_scope_is_refused():
    config = _config(
        tables=[
            _table("orders", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])])
        ],
        rls=[RLSRule(role_id="analyst", filter=RLS_FILTER, description="Test rule.")],
    )
    with pytest.raises(ValueError, match="neither a table, a domain nor a tag"):
        build_governance_tags(config, "acme")


def _snapshot_text(snapshot: MetadataSnapshot) -> str:
    """Every string anywhere in the snapshot, including nested dataclasses."""

    def walk(value) -> list[str]:
        if isinstance(value, str):
            return [value]
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return [s for f in dataclasses.fields(value) for s in walk(getattr(value, f.name))]
        if isinstance(value, (list, tuple, set)):
            return [s for item in value for s in walk(item)]
        return []

    return "\n".join(walk(snapshot))


def test_rule_bodies_never_appear_anywhere_in_a_published_snapshot():
    """The mask pattern and the RLS predicate are the policy; publishing them next to the
    restricted asset would hand a catalog reader the shape of the withheld data."""
    config = _config(
        tables=[
            _table(
                "customers",
                [
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["admin"],
                        masks=[
                            {"to": ["admin"], "mask": None},
                            {
                                "mask": {
                                    "type": "regex",
                                    "pattern": MASK_PATTERN,
                                    "replace": MASK_REPLACE,
                                },
                            },
                        ],
                    ),
                    Column(
                        name="tier",
                        data_type="text",
                        visible_to=["analyst", "admin"],
                        masks=[
                            {"mask": {"type": "constant", "value": "REDACTED-TIER"}},
                        ],
                    ),
                ],
            )
        ],
        rls=[
            RLSRule(
                table_id="customers", role_id="analyst", filter=RLS_FILTER, description="Test rule."
            )
        ],
    )
    text = _snapshot_text(build_snapshot(config, org_id="acme", dialect="postgres", contexts={}))
    assert MASK_PATTERN not in text
    assert MASK_REPLACE not in text
    assert "REDACTED-TIER" not in text
    assert RLS_FILTER not in text
    assert "current_setting" not in text


def test_build_snapshot_attaches_governance_tags():
    """Assets published without their restrictions would read as unrestricted."""
    config = _config(
        tables=[
            _table(
                "customers",
                [
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["admin"],
                        masks=[
                            {"to": ["admin"], "mask": None},
                            {
                                "mask": {
                                    "type": "regex",
                                    "pattern": MASK_PATTERN,
                                    "replace": MASK_REPLACE,
                                },
                            },
                        ],
                    )
                ],
            )
        ],
        rls=[
            RLSRule(
                table_id="customers", role_id="analyst", filter=RLS_FILTER, description="Test rule."
            )
        ],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres", contexts={})
    signals = {t.signal for t in snapshot.governance_tags}
    assert signals == {
        GovernanceSignal.MASKED,
        GovernanceSignal.RLS_RESTRICTED,
        GovernanceSignal.VISIBILITY_RESTRICTED,
    }
    assert all(t.asset.fqn().startswith("wh.public.customers") for t in snapshot.governance_tags)


def test_ungoverned_config_publishes_no_tags():
    config = _config(
        tables=[
            _table("orders", [Column(name="id", data_type="int", visible_to=["analyst", "admin"])])
        ]
    )
    assert (
        build_snapshot(config, org_id="acme", dialect="postgres", contexts={}).governance_tags == []
    )
