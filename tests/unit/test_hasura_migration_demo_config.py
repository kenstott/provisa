# Copyright (c) 2026 Kenneth Stott
# Canary: 06abf80c-16d5-4e78-a62e-a1804aa2d44e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The hasuramigration demo's model is what importing its Hasura export produces (REQ-417).

The demo's config.yaml holds the import's output -- tables, custom names, column grants, row
rules, write grants, relationships, roles, the command and the triggers -- plus what the demo adds:
column types and descriptions (a preview reads them from the database), a default for each handler
URL's variable, the Salesforce source, its table and relationship, and the persona role. This
keeps the two from drifting: everything the import writes is in the config as the import wrote it.
"""

# Requirements: REQ-417, REQ-2074

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from provisa.core.config_loader import parse_config_dict
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.parser import parse_metadata_dir
from provisa.import_shared.warnings import WarningCollector

DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "hasuramigration"
# What the demo adds to the import's output.
DEMO_ONLY_RELATIONSHIPS = {"customers.account"}
DEMO_ONLY_ROLES = {"ana"}
DEMO_ONLY_SOURCES = {"crm"}


@pytest.fixture(scope="module")
def imported():
    collector = WarningCollector()
    metadata = parse_metadata_dir(DEMO / "hasura" / "metadata", collector)
    return convert_metadata(
        metadata, collector, domain_map={"public": "store"}, source_overrides={"outfitters": {}}
    )


@pytest.fixture(scope="module")
def demo():
    return parse_config_dict(yaml.safe_load((DEMO / "config.yaml").read_text()))


def _tables(config):
    return {t.table_name: t for t in config.tables if t.source_id not in DEMO_ONLY_SOURCES}


def _without_default(url: str | None) -> str | None:
    """A handler URL with the default the demo gives its variable taken off."""
    return (
        None
        if url is None
        else re.sub(r"\$\{env:([A-Za-z0-9_]+):-[^}]*\}", lambda m: "${env:" + m[1] + "}", url)
    )


def test_the_same_tables_with_the_same_names(imported, demo):
    got, want = _tables(demo), _tables(imported)
    assert got.keys() == want.keys()
    for name, table in want.items():
        assert (got[name].alias, got[name].domain_id) == (table.alias, table.domain_id), name
        assert {c.name: (c.alias, sorted(c.visible_to)) for c in got[name].columns} == {
            c.name: (c.alias, sorted(c.visible_to)) for c in table.columns
        }, name


def test_the_same_write_grants(imported, demo):
    got, want = _tables(demo), _tables(imported)
    for name, table in want.items():
        assert [g.model_dump() for g in got[name].write_grants] == [
            g.model_dump() for g in table.write_grants
        ], name


def test_the_same_row_rules(imported, demo):
    def rules(config):
        return sorted((r.table_id, r.role_id, r.filter) for r in config.rls_rules)

    assert rules(demo) == rules(imported)


def test_the_same_relationships_and_one_more(imported, demo):
    def rels(config):
        return {
            r.id: (
                r.source_table_id,
                r.target_table_id,
                r.source_column,
                r.target_column,
                r.cardinality,
                r.graphql_alias,
            )
            for r in config.relationships
            if r.id not in DEMO_ONLY_RELATIONSHIPS
        }

    assert rels(demo) == rels(imported)
    assert DEMO_ONLY_RELATIONSHIPS <= {r.id for r in demo.relationships}


def test_the_same_roles(imported, demo):
    def roles(config):
        return {
            r.id: (sorted(r.capabilities), r.domain_access, r.parent_role_id)
            for r in config.roles
            if r.id not in DEMO_ONLY_ROLES
        }

    assert roles(demo) == roles(imported)


def test_the_same_command_and_triggers(imported, demo):
    def hooks(config):
        return [
            (
                w.name,
                w.kind,
                _without_default(w.url),
                sorted(w.visible_to),
                [(a.name, a.type) for a in w.arguments],
                [(i.name, i.type) for i in w.inline_return_type],
            )
            for w in config.webhooks
        ]

    def triggers(config):
        return sorted(
            (t.id, t.cron, t.on_table, t.on_event, _without_default(t.url))
            for t in config.scheduled_triggers
        )

    # The demo's URLs as written: their variables, each with the default the demo gives it.
    assert hooks(demo.written) == hooks(imported)
    assert triggers(demo.written) == triggers(imported)
