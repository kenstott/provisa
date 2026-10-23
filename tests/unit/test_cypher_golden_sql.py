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


def _structure(value):
    """A label map as plain data, compared field by field: it defines no equality of its own."""
    import dataclasses

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return (
            type(value).__name__,
            {f.name: _structure(getattr(value, f.name)) for f in dataclasses.fields(value)},
        )
    if isinstance(value, dict):
        return {k: _structure(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_structure(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(map(repr, value))
    if hasattr(value, "__dict__") and type(value).__module__.startswith("provisa"):
        return (type(value).__name__, _structure(vars(value)))
    return value


_MODEL_MAPS = {}
for _entry in _ENTRIES:
    _origin = _entry["origin"].split("#", 1)[0]
    if not _origin.startswith("test:"):
        _MODEL_MAPS.setdefault(_origin, _entry["label_map"])


@pytest.mark.parametrize("origin", sorted(_MODEL_MAPS))
def test_each_models_label_map_is_built_as_before_the_split(origin):
    config = _MODELS.get(origin) or f"demo/named/{origin.split(':', 1)[1]}/config.yaml"
    built = cypher_golden.model_label_map(_REPO / config)
    assert _structure(built) == _structure(cypher_golden.load_blob(_MODEL_MAPS[origin]))
