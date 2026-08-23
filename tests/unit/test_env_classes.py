# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e0d17-9c34-4f2a-b7e6-1a0d3c85f942
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1489: the classification is exhaustive, disjoint, and checked against the real schema."""

import pytest

from provisa.core import env_classes as ec
from provisa.core.schema_org import metadata as org_metadata

_CLASSES = {
    "CARRIED": ec.CARRIED,
    "IDENTITY_ONLY": ec.IDENTITY_ONLY,
    "NEVER_SENSITIVE": ec.NEVER_SENSITIVE,
    "NEVER_RUNTIME": ec.NEVER_RUNTIME,
    "PARTIAL": ec.PARTIAL,
}


def test_every_table_in_the_org_schema_is_classified():
    # REQ-1489: the failure mode for a table somebody added and forgot is THIS test, not a copied
    # secret. A new name here is a decision to be made, not a default to be inherited.
    declared = set(org_metadata.tables)
    assert declared - ec.CLASSIFIED == set(), (
        "unclassified org-schema tables — add each to a class in provisa/core/env_classes.py"
    )


def test_the_classification_names_no_table_the_schema_does_not_have():
    assert ec.CLASSIFIED - set(org_metadata.tables) == set()


def test_a_table_falls_in_exactly_one_class():
    seen: dict[str, str] = {}
    for name, members in _CLASSES.items():
        for table in members:
            assert table not in seen, f"{table} is in both {seen[table]} and {name}"
            seen[table] = name


def test_no_credential_table_travels():
    # REQ-1489, REQ-1492: default-deny for secrets and for the directory of who exists.
    for table in ("org_secrets", "user_directory"):
        assert table not in ec.CARRIED
        assert table not in ec.IDENTITY_ONLY


def test_who_holds_which_role_travels_with_the_roles_themselves():
    # REQ-1539: data rights are the roles' answer, and an environment gets its own copy of `roles`
    # — so `developer` can be unrestricted in dev and hold nothing in prod. The assignments have to
    # travel alongside, or a member would arrive in the new environment holding no role at all and
    # the per-environment role definitions would have nobody to apply to.
    assert {"roles", "user_role_assignments"} <= ec.CARRIED


def test_the_roles_are_seeded_by_a_creation_and_carried_by_nothing_else():
    # REQ-1539: a new environment needs roles to be usable at all, so a creation seeds them. After
    # that they are the environment's own answer — a merge from an unrestricted dev must not be
    # able to overwrite the restricted `developer` row in the base it merges into.
    assert ec.SEEDED_AT_CREATION == {"roles", "user_role_assignments"}
    assert ec.SEEDED_AT_CREATION < ec.CARRIED


def test_sources_travel_stripped_rather_than_excluded():
    # REQ-1491: registered_tables references a source, so dropping the row would cascade the model
    # away — the row travels without the connection values.
    assert "sources" in ec.IDENTITY_ONLY
    assert "sources" not in ec.CARRIED


def test_catalog_bindings_are_not_part_of_the_model():
    # Their vendor_ref and physical_key address another environment's external catalog.
    assert "catalog_bindings" in ec.NEVER_RUNTIME


@pytest.mark.parametrize(
    "key", ["metadata_export.endpoint", "redirect.base_url", "cache.redis_url", "cache"]
)
def test_settings_naming_an_external_target_or_a_runtime_stay_behind(key):
    assert not ec.carries_setting(key)


@pytest.mark.parametrize("key", ["naming.style", "glossary.require_definition", "ui.theme"])
def test_governance_settings_travel(key):
    assert ec.carries_setting(key)


class TestBindingColumns:
    """REQ-1491: what an IDENTITY_ONLY row leaves behind, checked against the real columns."""

    @pytest.mark.parametrize("table", sorted(ec.IDENTITY_ONLY))
    def test_every_binding_column_is_a_column_that_exists(self, table):
        # A name misspelled here would be silently ignored by the copy, which would then carry the
        # connection value it was written down to hold back.
        real = set(org_metadata.tables[table].columns.keys())
        assert ec.binding_columns(table) - real == set()

    @pytest.mark.parametrize("table", sorted(ec.IDENTITY_ONLY))
    def test_the_identity_a_source_is_known_by_always_travels(self, table):
        # The row exists in the target so registered_tables still resolves; its key must come with
        # it, or the copy would produce a row addressing nothing.
        pk = {c.name for c in org_metadata.tables[table].primary_key.columns}
        assert pk and pk & ec.binding_columns(table) == set()

    @pytest.mark.parametrize("table", sorted(ec.IDENTITY_ONLY))
    def test_boundness_is_marked_rather_than_inferred(self, table):
        # REQ-1491: an empty host is not an absent one — the connection builder reads it as
        # localhost:5432 — so the copy marks the row instead of blanking it.
        assert ec.BOUND_COLUMN in ec.binding_columns(table)
        assert ec.BOUND_COLUMN in org_metadata.tables[table].columns

    @pytest.mark.parametrize("table", sorted(ec.CARRIED | ec.NEVER_RUNTIME))
    def test_asking_a_non_identity_table_for_its_bindings_is_refused(self, table):
        # An empty set would read as "this table carries no connection values", which is a claim
        # nobody checked; the answer is that the question does not apply.
        with pytest.raises(KeyError):
            ec.binding_columns(table)

    def test_where_a_source_points_never_travels(self):
        assert {"host", "port", "database", "username"} <= ec.binding_columns("sources")
        assert ec.binding_columns("api_sources") >= {"base_url", "auth"}
        assert ec.binding_columns("kafka_sources") >= {"bootstrap_servers"}

    def test_governance_on_a_source_row_still_travels(self):
        # The point of IDENTITY_ONLY is that the row is not simply dropped: everything about the
        # source that is model rather than address comes with it.
        carried = set(org_metadata.tables["sources"].columns.keys()) - ec.binding_columns("sources")
        assert {"id", "type", "description", "cache_enabled", "load_protected"} <= carried
