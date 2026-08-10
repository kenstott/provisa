# Copyright (c) 2026 Kenneth Stott
# Canary: 6d4e2a91-8f3c-4b17-9a5d-2e7c1f0b6a3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Regression: strict-mode group-by Cypher must not treat relationship fields as bare
node properties. Reproduces the user-reported error:

    line 1:284: Column 'a.user' cannot be resolved

for `ps__inquiriesGroupBy(by: [userId]) { nodes { id ... user { id name } pet { id name } } }`.
"""

from provisa.api.admin.dev_queries import _merge_nodes_cypher, _merge_nodes_cypher_denormalized


GROUP_BY_CYPHER = """MATCH (a:Inquiries)
RETURN a.userId AS groupKey, count(*) AS count"""

NODES_CYPHER = """MATCH (a:Inquiries)
OPTIONAL MATCH (a)-[:HAS_USER]->(u:Users)
OPTIONAL MATCH (a)-[:HAS_PET]->(p:Pets)
RETURN a.id AS id, a.inquiryType AS inquiryType, {id: u.id, name: u.name} AS user, {id: p.id, name: p.name} AS pet"""

# The shape semantic_sql_to_cypher actually emits: scalar columns come back as bare property
# accesses with no AS alias, and each relationship field is wrapped in its own collect().
REAL_NODES_CYPHER = """MATCH (a:Inquiries)
OPTIONAL MATCH (a:Inquiries)-[:SUBMITTED_BY]->(b:Users)
OPTIONAL MATCH (a:Inquiries)-[:HAS_PETS]->(c:Pets)
RETURN a.id, a.inquiryType, collect({id: b.id, name: b.name}) AS user, collect({id: c.id, name: c.name}) AS pet, a.userId"""


class TestMergeNodesCypher:
    def test_relationship_fields_become_nested_maps_not_bare_properties(self):
        merged = _merge_nodes_cypher(GROUP_BY_CYPHER, NODES_CYPHER)
        assert merged is not None
        assert "user: a.user" not in merged
        assert "pet: a.pet" not in merged
        assert "user: {id: u.id, name: u.name}" in merged
        assert "pet: {id: p.id, name: p.name}" in merged

    def test_scalar_fields_still_resolve_off_base_var(self):
        merged = _merge_nodes_cypher(GROUP_BY_CYPHER, NODES_CYPHER)
        assert merged is not None
        assert "id: a.id" in merged
        assert "inquiryType: a.inquiryType" in merged

    def test_optional_matches_spliced_after_match(self):
        merged = _merge_nodes_cypher(GROUP_BY_CYPHER, NODES_CYPHER)
        assert merged is not None
        lines = merged.splitlines()
        match_idx = next(i for i, ln in enumerate(lines) if ln.strip().startswith("MATCH"))
        assert lines[match_idx + 1].strip() == "OPTIONAL MATCH (a)-[:HAS_USER]->(u:Users)"
        assert lines[match_idx + 2].strip() == "OPTIONAL MATCH (a)-[:HAS_PET]->(p:Pets)"

    def test_no_nodes_return_line_yields_none(self):
        assert _merge_nodes_cypher(GROUP_BY_CYPHER, "MATCH (a:Inquiries)") is None

    def test_existing_optional_match_not_duplicated(self):
        group_by_with_opt = (
            "MATCH (a:Inquiries)\n"
            "OPTIONAL MATCH (a)-[:HAS_USER]->(u:Users)\n"
            "RETURN a.userId AS groupKey, count(*) AS count"
        )
        merged = _merge_nodes_cypher(group_by_with_opt, NODES_CYPHER)
        assert merged is not None
        assert merged.count("OPTIONAL MATCH (a)-[:HAS_USER]->(u:Users)") == 1
        assert "OPTIONAL MATCH (a)-[:HAS_PET]->(p:Pets)" in merged


class TestMergeRealCompilerOutput:
    """Regression for the second report: 'Function to_json not registered', from a merge that
    dropped every unaliased scalar and nested collect() inside collect()."""

    def test_unaliased_scalars_are_kept(self):
        merged = _merge_nodes_cypher(GROUP_BY_CYPHER, REAL_NODES_CYPHER)
        assert merged is not None
        assert "id: a.id" in merged
        assert "inquiryType: a.inquiryType" in merged
        assert "userId: a.userId" in merged

    def test_relationship_collect_is_unwrapped_no_nested_aggregate(self):
        merged = _merge_nodes_cypher(GROUP_BY_CYPHER, REAL_NODES_CYPHER)
        assert merged is not None
        assert "collect({id: b.id, name: b.name})" not in merged
        assert "user: {id: b.id, name: b.name}" in merged
        assert "pet: {id: c.id, name: c.name}" in merged
        assert merged.count("collect(") == 1

    def test_denormalized_variant_also_unwraps(self):
        merged = _merge_nodes_cypher_denormalized(GROUP_BY_CYPHER, REAL_NODES_CYPHER)
        assert merged is not None
        assert "user: {id: b.id, name: b.name}" in merged
        assert "id: a.id" in merged


class TestMergeNodesCypherDenormalized:
    def test_relationship_fields_become_nested_maps_in_collect(self):
        merged = _merge_nodes_cypher_denormalized(GROUP_BY_CYPHER, NODES_CYPHER)
        assert merged is not None
        assert "user: a.user" not in merged
        assert "pet: a.pet" not in merged
        assert "user: {id: u.id, name: u.name}" in merged
        assert "pet: {id: p.id, name: p.name}" in merged

    def test_final_return_projects_nested_map_by_alias(self):
        merged = _merge_nodes_cypher_denormalized(GROUP_BY_CYPHER, NODES_CYPHER)
        assert merged is not None
        assert "node.user AS user" in merged
        assert "node.pet AS pet" in merged
        assert "node.id AS id" in merged

    def test_optional_matches_spliced_before_with(self):
        merged = _merge_nodes_cypher_denormalized(GROUP_BY_CYPHER, NODES_CYPHER)
        assert merged is not None
        lines = merged.splitlines()
        with_idx = next(i for i, ln in enumerate(lines) if ln.strip().startswith("WITH"))
        assert "OPTIONAL MATCH (a)-[:HAS_USER]->(u:Users)" in lines[:with_idx]
        assert "OPTIONAL MATCH (a)-[:HAS_PET]->(p:Pets)" in lines[:with_idx]
