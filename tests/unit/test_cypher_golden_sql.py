# Copyright (c) 2026 Kenneth Stott
# Canary: f2685139-b100-4edd-9599-6d517beefa70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every Cypher query the repository has compiles to the SQL it compiled to before the graph
compiler was split (REQ-2135).

tests/fixtures/cypher_golden holds each translation captured from 3c3aff532 (scripts/cypher_golden.py):
the unit tests' Cypher, the demo tours', the perf bench's and the UI e2e specs'. Each is parsed
here by the Cypher front end and translated by the back end with the label map and parameters it
was given then, and must produce byte-identical SQL, the same parameter order and the same
graph-variable kinds -- or be refused the same way."""

# Requirements: REQ-2135
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "scripts"))

import cypher_golden  # noqa: E402

_ENTRIES = [
    json.loads(line)
    for name in ("entries.jsonl", "approved.jsonl")
    for line in (cypher_golden.GOLDEN / name).read_text().splitlines()
    if line.strip()
]


def _produced(entry: dict) -> dict:
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.translator import cypher_to_sql

    label_map = cypher_golden.load_blob(entry["label_map"])
    params = cypher_golden.load_blob(entry["params"])
    try:
        ast = parse_cypher(entry["cypher"])
        if entry["call"] is not None:
            ast = ast.call_subqueries[entry["call"]].body
        return cypher_golden.rendered(cypher_to_sql(ast, label_map, params))
    except Exception as exc:
        return cypher_golden.refused(exc)


def test_the_corpus_is_every_kind_of_cypher_the_repository_has():
    origins = {e["origin"].split(":", 1)[0].split("#", 1)[0] for e in _ENTRIES}
    assert {"test", "tour", "bench", "e2e"} <= origins
    assert len(_ENTRIES) >= 700


@pytest.mark.parametrize(
    "entry", _ENTRIES, ids=[f"{i}:{e['origin'][:80]}" for i, e in enumerate(_ENTRIES)]
)
def test_the_same_sql_as_before_the_split(entry):
    assert _produced(entry) == entry["expected"]


#: The model each non-test corpus was compiled against.
_MODELS = {
    "bench:perf": "demo/named/perf/config.yaml",
    "e2e": "config/provisa.yaml",
}


# REQ-2163: the label map's SQL-only tables (sql_only_nodes, sql_only_tables) and the relationships
# touching them (sql_only_relationships, sql_only_aliases) came after it too.
_ADDED_AFTER_THE_SPLIT = frozenset(
    {
        "property_types",
        "key_columns",
        "sql_only_nodes",
        "sql_only_tables",
        "sql_only_relationships",
        "sql_only_aliases",
        "attribute_types",  # REQ-2176: a junction attribute's registered type
        "registered",  # REQ-1586: whether a registered relationship row declares the edge
        "directed",  # REQ-2176: a stored edge (only the session's) has one direction
        "identity_column",  # REQ-2176: a stored edge (only the session's) is its own row
        "property_zones",  # REQ-2176: a session date time's zone (only the session's)
        "alias",  # REQ-1385: a table's alias, its model name for SPARQL's IRIs
        "property_aliases",  # REQ-1385: each aliased column's alias, its model name
    }
) | frozenset(
    # Dropped after the split: labels are case-sensitive (openCypher), so no case-folding index of
    # nodes, domains or tables is built.
    {"_nodes_ci", "_domains_ci", "_nodes_by_table_ci"}
)


def _structure(value):
    """A label map as plain data, compared field by field: it defines no equality of its own."""
    import dataclasses

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # A field added after the split carries what the split never built (REQ-778: each
        # property's registered type; REQ-2136: a compound key's columns); the comparison is of
        # what the split moved.
        return (
            type(value).__name__,
            {
                f.name: _structure(getattr(value, f.name))
                for f in dataclasses.fields(value)
                if f.name not in _ADDED_AFTER_THE_SPLIT
            },
        )
    if isinstance(value, dict):
        return {k: _structure(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_structure(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(map(repr, value))
    if hasattr(value, "__dict__") and type(value).__module__.startswith("provisa"):
        return (
            type(value).__name__,
            {k: _structure(v) for k, v in vars(value).items() if k not in _ADDED_AFTER_THE_SPLIT},
        )
    return value


_MODEL_MAPS = {}
for _entry in _ENTRIES:
    _origin = _entry["origin"].split("#", 1)[0]
    if not _origin.startswith(("test:", "approved:")):
        _MODEL_MAPS.setdefault(_origin, _entry["label_map"])


# REQ-1586 (amended 2026-11-18): a junction table that registered relationships of its own join
# (Join through keeps its two hops beside the joined relationship) keeps its node and every edge
# into and out of it, where the split's label map dropped it. These are the junction labels each
# model keeps; the comparison leaves them, and the edges that touch them, out.
_KEPT_JUNCTION_LABELS = {
    "tour:sonnets": frozenset({"S__Echo"}),
    "tour:worldcup": frozenset({"W__CountryAlias"}),
}


def _without_labels(value, labels):
    """A _structure with the given node labels, and every edge that touches one, removed."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if isinstance(k, str) and "::" in k:
                ends = k.split("::", 1)[1].split("→")
                if any(e in labels for e in ends):
                    continue
            if k in labels:
                continue
            kept = _without_labels(v, labels)
            if kept == [] and v != []:
                continue
            out[k] = kept
        return out
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[1], dict):
        return (value[0], _without_labels(value[1], labels))
    if isinstance(value, list):
        kept = []
        for v in value:
            if isinstance(v, str) and v in labels:
                continue
            if (
                isinstance(v, tuple)
                and len(v) == 2
                and isinstance(v[1], dict)
                and (v[1].get("source_label") in labels or v[1].get("target_label") in labels)
            ):
                continue
            kept.append(_without_labels(v, labels))
        return kept
    return value


# REQ-194 (dcb6300ba8, 2026-11-21): a relationship field's head noun that neither the supplement,
# the dictionary nor lemminflect knows stays as written, where the split's rule appended "s". These
# are the relationship field names each model renamed so; the comparison expects the new name.
_RENAMED_RELATIONSHIP_FIELDS = {
    "bench:perf": {"placeds": "placed"},
}


def _with_field_names(value, renames):
    """A _structure with each relationship field name in ``renames`` replaced by its new name."""
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[1], dict):
        fields = {k: _with_field_names(v, renames) for k, v in value[1].items()}
        if value[0] == "RelationshipMapping" and fields.get("field_name") in renames:
            fields["field_name"] = renames[fields["field_name"]]
        return (value[0], fields)
    if isinstance(value, dict):
        return {k: _with_field_names(v, renames) for k, v in value.items()}
    if isinstance(value, list):
        return [_with_field_names(v, renames) for v in value]
    return value


@pytest.mark.parametrize("origin", sorted(_MODEL_MAPS))
def test_each_models_label_map_is_built_as_before_the_split(origin):
    config = _MODELS.get(origin) or f"demo/named/{origin.split(':', 1)[1]}/config.yaml"
    built = _structure(cypher_golden.model_label_map(_REPO / config))
    built = _without_labels(built, _KEPT_JUNCTION_LABELS.get(origin, frozenset()))
    expected = _structure(cypher_golden.load_blob(_MODEL_MAPS[origin]))
    expected = _with_field_names(expected, _RENAMED_RELATIONSHIP_FIELDS.get(origin, {}))
    assert built == expected
