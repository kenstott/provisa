# Copyright (c) 2026 Kenneth Stott
# Canary: 3a1c7e52-8d64-4f09-b2ae-5d70c9184bb6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The serialized model's DIRECTORY LAYOUT and its determinism (REQ-1526).

What is under test is the tree, not the database: that a file's path is the entity's REQ-1385
address, that nothing storage wrote (a surrogate id, a tenancy, a stamp) and nothing a binding holds
(a host, a credential) reaches a file, and that the same model renders byte-identically twice.
"""

import pytest
import yaml

from provisa.api.metadata_export.refs import RESERVED_KIND_KEYWORDS, table_uri
from provisa.core import env_files as ef
from provisa.core.models import Table

ORG = "acme"


def _table(**over) -> Table:
    fields = {
        "id": 41,
        "source_id": "pg_main",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "alias": "Order",
        "columns": [],
    }
    fields.update(over)
    return Table(**fields)


class TestPathIsTheAddress:
    def test_a_table_path_is_its_uri_with_scheme_and_org_stripped(self):
        table = _table()
        uri = table_uri(ORG, table)
        assert ef.table_path(table.domain_id, table.alias) == "sales/tables/Order.yaml"
        assert uri == "provisa://acme/sales/tables/Order"
        assert ef.path_of(uri) == ef.table_path(table.domain_id, table.alias)

    def test_a_hierarchical_domain_nests(self):
        assert ef.table_path("sales/eu", "Order") == "sales/eu/tables/Order.yaml"

    def test_a_table_in_no_domain_sits_at_the_root(self):
        assert ef.table_path(None, "Order") == "tables/Order.yaml"

    def test_a_domains_own_attributes_live_inside_its_directory(self):
        assert ef.domain_path("sales") == "sales/domain.yaml"
        assert ef.domain_path("sales/eu") == "sales/eu/domain.yaml"

    def test_a_name_carrying_a_slash_stays_one_file(self):
        assert ef.table_path("sales", "Order/Line") == "sales/tables/Order%2FLine.yaml"

    def test_root_kinds_address_by_id(self):
        assert ef.kind_path("sources", "pg_main") == "sources/pg_main.yaml"
        assert ef.kind_path("commands", "refund") == "commands/refund.yaml"
        assert ef.kind_path("calendars", "fiscal", "2026") == "calendars/fiscal/2026.yaml"

    def test_the_address_round_trips_through_the_org(self):
        path = ef.table_path("sales", "Order")
        assert ef.address_of(path, ORG) == "provisa://acme/sales/tables/Order"
        assert ef.path_of(ef.address_of(path, ORG)) == path

    def test_every_directory_the_tree_uses_is_a_reserved_domain_name(self):
        used = {"tables", "sources", "commands", "calendars", "terms", "domain"}
        assert used <= set(RESERVED_KIND_KEYWORDS)


class TestFragmentsAreNotPaths:
    def test_a_column_has_no_file_of_its_own(self):
        with pytest.raises(ef.FileLayoutError, match="inside its table's file"):
            ef.path_of("provisa://acme/sales/tables/Order#field:total")

    def test_a_relationship_has_no_file_of_its_own(self):
        with pytest.raises(ef.FileLayoutError, match="inside its table's file"):
            ef.path_of("provisa://acme/sales/tables/Order#rel:customer")

    def test_an_organization_is_the_tree_not_a_file(self):
        with pytest.raises(ef.FileLayoutError, match="the tree itself"):
            ef.path_of("provisa://acme")

    def test_a_foreign_uri_is_refused(self):
        with pytest.raises(ef.FileLayoutError, match="not a provisa URI"):
            ef.path_of("https://acme/sales/tables/Order")


class TestWhatNeverReachesAFile:
    def test_the_surrogate_key_and_the_tenancy_are_dropped(self):
        body = ef.entity(
            "registered_tables",
            {"id": 41, "tenant_id": "t-1", "org_id": ORG, "alias": "Order", "table_name": "orders"},
        )
        assert body == {"alias": "Order", "table_name": "orders"}

    def test_the_clustering_run_is_dropped(self):
        body = ef.entity(
            "registered_tables",
            {"table_name": "orders", "l1_cluster": 3, "clusters_computed_at": "2026-08-21"},
        )
        assert body == {"table_name": "orders"}

    def test_a_sources_binding_never_reaches_the_tree(self):
        body = ef.entity(
            "sources",
            {
                "id": "pg_main",
                "type": "postgres",
                "host": "db.internal",
                "port": 5432,
                "database": "sales",
                "username": "svc",
                "mapping": {"schema": "public"},
                "bound": True,
                "description": "the sales database",
            },
        )
        assert body == {"type": "postgres", "description": "the sales database"}

    def test_an_unset_column_is_absent_rather_than_null(self):
        body = ef.entity("registered_tables", {"table_name": "orders", "description": None})
        assert body == {"table_name": "orders"}


class TestChildrenAreWrittenInsideTheirParent:
    def test_columns_nest_under_the_table_sorted_by_name(self):
        body = ef.entity(
            "registered_tables",
            {"id": 41, "table_name": "orders"},
            columns=ef.Child(
                "column_name",
                "table_id",
                [
                    {"id": 9, "table_id": 41, "column_name": "total"},
                    {"id": 7, "table_id": 41, "column_name": "customer_id"},
                ],
            ),
        )
        assert [c["column_name"] for c in body["columns"]] == ["customer_id", "total"]

    def test_the_joining_surrogate_is_gone_from_the_child(self):
        body = ef.entity(
            "registered_tables",
            {"id": 41, "table_name": "orders"},
            columns=ef.Child(
                "column_name", "table_id", [{"id": 9, "table_id": 41, "column_name": "total"}]
            ),
        )
        assert body["columns"] == [{"column_name": "total"}]

    def test_an_empty_collection_is_absent_rather_than_an_empty_list(self):
        body = ef.entity(
            "registered_tables",
            {"table_name": "orders"},
            columns=ef.Child("column_name", "table_id", []),
        )
        assert "columns" not in body

    def test_a_child_without_the_sort_key_is_refused(self):
        with pytest.raises(ef.FileLayoutError, match="sort key is the child's address"):
            ef.entity(
                "registered_tables",
                {"table_name": "orders"},
                columns=ef.Child("column_name", "table_id", [{"description": "no name"}]),
            )


class TestDeterminism:
    def test_keys_are_emitted_in_one_fixed_order_whatever_order_they_arrive_in(self):
        forward = ef.dump({"a.yaml": {"alpha": 1, "beta": 2, "gamma": 3}})
        backward = ef.dump({"a.yaml": {"gamma": 3, "beta": 2, "alpha": 1}})
        assert forward == backward
        assert forward["a.yaml"] == "alpha: 1\nbeta: 2\ngamma: 3\n"

    def test_two_environments_holding_the_same_model_render_identically(self):
        left = ef.entity(
            "registered_tables",
            {"id": 41, "tenant_id": "t-left", "table_name": "orders"},
            columns=ef.Child(
                "column_name",
                "table_id",
                [
                    {"id": 1, "table_id": 41, "column_name": "total"},
                    {"id": 2, "table_id": 41, "column_name": "customer_id"},
                ],
            ),
        )
        right = ef.entity(
            "registered_tables",
            {"id": 907, "tenant_id": "t-right", "table_name": "orders"},
            columns=ef.Child(
                "column_name",
                "table_id",
                [
                    {"id": 550, "table_id": 907, "column_name": "customer_id"},
                    {"id": 551, "table_id": 907, "column_name": "total"},
                ],
            ),
        )
        path = ef.table_path("sales", "Order")
        assert ef.dump({path: left}) == ef.dump({path: right})

    def test_a_model_ordered_list_is_left_alone(self):
        # column_presets is ordered BY THE MODEL; sorting it would change what it says.
        body = ef.entity(
            "registered_tables", {"table_name": "orders", "column_presets": ["z", "a"]}
        )
        assert yaml.safe_load(ef.dump({"t.yaml": body})["t.yaml"])["column_presets"] == ["z", "a"]

    def test_files_render_one_scalar_per_line(self):
        text = ef.dump({"t.yaml": {"visible_to": ["admin", "analyst"]}})["t.yaml"]
        assert text == "visible_to:\n- admin\n- analyst\n"

    def test_unicode_survives_rather_than_being_escaped(self):
        text = ef.dump({"t.yaml": {"description": "año"}})["t.yaml"]
        assert text == "description: año\n"


class TestLoad:
    def test_a_tree_round_trips_through_text(self):
        tree = {
            "sales/domain.yaml": {"description": "sales"},
            "sales/tables/Order.yaml": {
                "columns": [{"column_name": "total"}],
                "table_name": "orders",
            },
            "sources/pg_main.yaml": {"type": "postgres"},
        }
        assert ef.load(ef.dump(tree)) == tree

    def test_a_file_that_is_not_an_entity_is_refused_rather_than_coerced(self):
        with pytest.raises(ef.FileLayoutError, match="describes exactly one entity"):
            ef.load({"sales/tables/Order.yaml": "- a\n- b\n"})

    def test_a_file_outside_the_layout_is_refused(self):
        with pytest.raises(ef.FileLayoutError, match="not a model file"):
            ef.load({"README.md": "notes"})
