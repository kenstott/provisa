# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-392: the schema-wide graph-count sweep must exclude PARAMETERIZED nodes (native-filter
columns) — they are functions with no snapshot, so ``MATCH (n:Label) RETURN count(n)`` cannot be
satisfied and (because the counter re-raises) would zero the whole node/rel panel."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.rest.cypher_router import _countable_labels


def _node(label, domain_id="d", nf=None):
    return SimpleNamespace(label=label, domain_id=domain_id, native_filter_columns=nf or {})


def _rel(source_label, target_label, rel_type):
    return SimpleNamespace(source_label=source_label, target_label=target_label, rel_type=rel_type)


def _label_map(nodes, rels):
    return SimpleNamespace(
        nodes={nm.label: nm for nm in nodes},
        relationships={i: r for i, r in enumerate(rels)},
    )


def test_excludes_parameterized_nodes_and_their_relationships():
    lm = _label_map(
        nodes=[
            _node("Breeds"),
            _node("Breed", nf={"_nf_name": "text"}),  # parameterized → excluded
            _node("Employees"),
        ],
        rels=[
            _rel("Employees", "Breeds", "WORKS_WITH"),  # both countable → kept
            _rel("Breed", "Employees", "BREED_OF"),  # touches parameterized → excluded
        ],
    )
    node_labels, rel_types = _countable_labels(lm, set())
    assert node_labels == ["Breeds", "Employees"]
    assert rel_types == ["WORKS_WITH"]


def test_domain_filter_applies_alongside_parameterized_exclusion():
    lm = _label_map(
        nodes=[
            _node("A", domain_id="keep"),
            _node("B", domain_id="drop"),
            _node("P", domain_id="keep", nf={"_nf_k": "text"}),  # parameterized → excluded
        ],
        rels=[],
    )
    node_labels, rel_types = _countable_labels(lm, {"keep"})
    assert node_labels == ["A"]
    assert rel_types == []
