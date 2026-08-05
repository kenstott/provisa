# Copyright (c) 2026 Kenneth Stott
# Canary: 3a70d6c2-58bf-4e13-9c44-8ed1a2f05b97
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-1071 — enforcement facts projected as catalog tags."""

from __future__ import annotations

import dataclasses

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.api.metadata_egress import build_snapshot
from provisa.api.metadata_egress.model import GovernanceSignal
from provisa.core.models import (
    Column,
    Domain,
    ProvisaConfig,
    RLSRule,
    Role,
    Source,
    SourceType,
    Table,
)

scenarios("../features/REQ-1071.feature")

MASK_PATTERN = r"(\d{3})-(\d{2})-(\d{4})"
RLS_FILTER = "region_id = current_setting('provisa.region')"


@pytest.fixture
def shared_data() -> dict:
    return {}


@given("a governed config with a masked column, a column hidden from one role, and an RLS rule")
def _governed_config(shared_data):
    shared_data["config"] = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[Domain(id="sales", description="Sales", steward="data-steward")],
        tables=[
            Table(
                source_id="wh",
                domain_id="sales",
                schema_name="public",
                table_name="customers",
                columns=[
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["analyst", "admin"],
                        mask_type="regex",
                        mask_pattern=MASK_PATTERN,
                        mask_replace="XXX-XX-\\3",
                        unmasked_to=["admin"],
                    ),
                    Column(name="salary", data_type="numeric", visible_to=["admin"]),
                ],
            )
        ],
        relationships=[],
        roles=[
            Role(id="analyst", capabilities=[], domain_access=["*"]),
            Role(id="admin", capabilities=[], domain_access=["*"]),
        ],
        rls_rules=[RLSRule(table_id="customers", role_id="analyst", filter=RLS_FILTER)],
    )


@when("a metadata snapshot is built for the org")
def _build(shared_data):
    shared_data["snapshot"] = build_snapshot(
        shared_data["config"], org_id="acme", dialect="postgres"
    )


@then("each restricted asset carries a governance tag naming the signal and the rule")
def _tags_published(shared_data):
    tags = shared_data["snapshot"].governance_tags
    by_signal = {t.signal: t for t in tags}
    assert set(by_signal) == {
        GovernanceSignal.MASKED,
        GovernanceSignal.RLS_RESTRICTED,
        GovernanceSignal.VISIBILITY_RESTRICTED,
    }
    assert by_signal[GovernanceSignal.MASKED].asset.fqn() == "wh.public.customers.ssn"
    assert by_signal[GovernanceSignal.MASKED].rule_id == "mask:wh.public.customers.ssn:regex"
    assert by_signal[GovernanceSignal.RLS_RESTRICTED].asset.fqn() == "wh.public.customers"
    assert by_signal[GovernanceSignal.RLS_RESTRICTED].rule_id == "rls:customers:analyst"
    assert (
        by_signal[GovernanceSignal.VISIBILITY_RESTRICTED].asset.fqn()
        == "wh.public.customers.salary"
    )


@then("every tag names the roles the restriction applies to and the roles exempt from it")
def _roles_named(shared_data):
    for tag in shared_data["snapshot"].governance_tags:
        assert tag.restricted_roles == ("analyst",)
        assert tag.exempt_roles == ("admin",)


@then("no mask pattern or RLS predicate appears anywhere in the snapshot")
def _no_rule_bodies(shared_data):
    def walk(value) -> list[str]:
        if isinstance(value, str):
            return [value]
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return [s for f in dataclasses.fields(value) for s in walk(getattr(value, f.name))]
        if isinstance(value, (list, tuple, set)):
            return [s for item in value for s in walk(item)]
        return []

    text = "\n".join(walk(shared_data["snapshot"]))
    assert MASK_PATTERN not in text
    assert RLS_FILTER not in text
    assert "current_setting" not in text
