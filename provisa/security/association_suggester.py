# Copyright (c) 2026 Kenneth Stott
# Canary: 8e0d4c72-1a95-4f63-b8e1-3d7a5c9f2061
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Protocol-specific mutation-to-table association suggesters (REQ-871).

At registration time each remote-schema mutation is matched against the known
tables to propose which table it most likely writes. The rules are protocol-aware
because the strongest alignment signal differs by protocol:

- GraphQL: walk the mutation's return type, unwrap NON_NULL/LIST, match leaf object
  types via ``type_to_table``; a single object outranks a list-of-table-type, and
  scalar/stats fields are ignored.
- OpenAPI: align via the URL path template's resource segment, the operationId stem,
  and tags; the response schema is a tiebreaker only, never the primary signal.
- gRPC: the response message's repeated-field type plus the method-name entity stem
  (the weakest of the three protocols).

All adapters share two fallbacks for operations whose response carries no queryable
type: operation-name affixes (create*/update*/delete*/upsert*) and the input-type
stem. Every function returns a ranked list of ``TableCandidate`` — these are HINTS
ONLY. Nothing here binds a mutation to a table; an admin confirms a suggestion, and a
confirmed association registers with an empty ``writable_by`` (default-deny, REQ-867).
False negatives are expected: an empty list means "no confident suggestion", not an
error, and the mutation remains registerable (global function or manual binding).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Relative signal strengths, strongest → weakest. A single, direct return-type match
# is the most trustworthy; shared name/affix fallbacks are the least.
_SCORE_GQL_OBJECT = 1.0  # return type is a single table object
_SCORE_GQL_LIST = 0.8  # return type is a list of the table's type (changed-records)
_SCORE_PATH = 0.9  # OpenAPI URL path resource segment
_SCORE_OPID_STEM = 0.7  # operationId noun stem
_SCORE_TAG = 0.6  # OpenAPI tag
_SCORE_GRPC_FIELD = 0.6  # gRPC repeated response-field type
_SCORE_GRPC_METHOD = 0.4  # gRPC method-name entity stem
_SCORE_FALLBACK = 0.3  # name-affix / input-type stem (all adapters)
_TIEBREAK = 0.05  # response-schema confirmation nudge (never a primary signal)

_AFFIXES = ("create", "update", "delete", "upsert", "insert", "remove", "add", "put", "patch")
# Common wrapper suffixes on input/request message type names, stripped to the entity stem.
_INPUT_SUFFIXES = ("input", "request", "payload", "args", "params", "dto", "message", "req")


@dataclass(frozen=True)
class TableCandidate:  # REQ-871
    """A ranked, non-binding suggestion that a mutation writes ``table``."""

    table: str
    score: float
    reason: str


_MIN_IES_STEM = 3  # keep at least one char before the "ies"→"y" rewrite
_MIN_PLURAL_STEM = 2  # keep at least one char before stripping a trailing "s"


def _normalize(word: str) -> str:
    """Lowercase, drop non-alphanumerics, and singularize for name comparison."""
    w = re.sub(r"[^a-z0-9]+", "", word.lower())
    if w.endswith("ies") and len(w) > _MIN_IES_STEM:
        return w[:-3] + "y"
    if w.endswith(("ses", "xes", "zes")):
        return w[:-2]
    if w.endswith("s") and not w.endswith("ss") and len(w) > _MIN_PLURAL_STEM:
        return w[:-1]
    return w


def _type_stem(type_name: str) -> str:
    """Normalized entity stem of an input/request type name (strips Input/Request/... suffix)."""
    norm = _normalize(type_name)
    for suffix in _INPUT_SUFFIXES:
        if norm.endswith(suffix) and len(norm) > len(suffix):
            return _normalize(norm[: -len(suffix)])
    return norm


def _strip_affix(op_name: str) -> str:
    """Return the entity stem of an operation name minus a leading/trailing CRUD affix."""
    # camelCase / snake_case → tokens
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", op_name)
    tokens = [t for t in re.split(r"[_\s]+", spaced) if t]
    lowered = [t.lower() for t in tokens]
    if lowered and lowered[0] in _AFFIXES:
        tokens = tokens[1:]
    elif lowered and lowered[-1] in _AFFIXES:
        tokens = tokens[:-1]
    return "".join(tokens)


def _table_index(table_names: list[str]) -> dict[str, str]:
    """Map normalized table name → original name (first wins on collision)."""
    index: dict[str, str] = {}
    for name in table_names:
        key = _normalize(name)
        index.setdefault(key, name)
    return index


def _rank(raw: list[TableCandidate]) -> list[TableCandidate]:
    """Deduplicate by table (keep the strongest signal, merge reasons), sort strongest first."""
    best: dict[str, TableCandidate] = {}
    for cand in raw:
        cur = best.get(cand.table)
        if cur is None or cand.score > cur.score:
            best[cand.table] = cand
        elif cand.reason not in cur.reason:
            best[cand.table] = TableCandidate(
                cand.table, cur.score + _TIEBREAK, f"{cur.reason}; {cand.reason}"
            )
    return sorted(best.values(), key=lambda c: (-c.score, c.table))


def suggest_graphql(
    *,
    return_leaf_types: list[str],
    list_valued_types: set[str],
    type_to_table: dict[str, str],
    op_name: str = "",
    input_type_stem: str = "",
    table_names: list[str] | None = None,
) -> list[TableCandidate]:
    """Rank tables for a GraphQL mutation (REQ-871).

    ``return_leaf_types`` are the object type names reached by unwrapping the return
    type's NON_NULL/LIST wrappers (scalar/stats fields already excluded by the caller);
    ``list_valued_types`` marks which of those were LIST-valued (a changed-records
    collection, ranked below a single object). ``type_to_table`` maps a GraphQL type to
    a table name. Falls back to the operation-name affix + input-type stem.
    """
    raw: list[TableCandidate] = []
    for gql_type in return_leaf_types:
        table = type_to_table.get(gql_type)
        if not table:
            continue
        if gql_type in list_valued_types:
            raw.append(TableCandidate(table, _SCORE_GQL_LIST, f"return list of type {gql_type}"))
        else:
            raw.append(TableCandidate(table, _SCORE_GQL_OBJECT, f"return type {gql_type}"))
    raw.extend(_fallback_candidates(op_name, input_type_stem, table_names or []))
    return _rank(raw)


def suggest_openapi(
    *,
    path: str,
    operation_id: str = "",
    tags: list[str] | None = None,
    response_leaf_types: list[str] | None = None,
    input_type_stem: str = "",
    table_names: list[str],
) -> list[TableCandidate]:
    """Rank tables for an OpenAPI operation (REQ-871).

    Primary signals: the last non-parameter segment of the URL path template, the
    operationId noun stem, and tags. The response schema's leaf types are a tiebreaker
    only. Falls back to the operationId affix + input-type stem.
    """
    index = _table_index(table_names)
    raw: list[TableCandidate] = []

    segments = [s for s in path.split("/") if s and not s.startswith("{")]
    if segments:
        table = index.get(_normalize(segments[-1]))
        if table:
            raw.append(TableCandidate(table, _SCORE_PATH, f"path resource /{segments[-1]}"))

    if operation_id:
        table = index.get(_normalize(_strip_affix(operation_id)))
        if table:
            raw.append(TableCandidate(table, _SCORE_OPID_STEM, f"operationId stem {operation_id}"))

    for tag in tags or []:
        table = index.get(_normalize(tag))
        if table:
            raw.append(TableCandidate(table, _SCORE_TAG, f"tag {tag}"))

    for leaf in response_leaf_types or []:
        table = index.get(_normalize(leaf))
        if table:
            raw.append(TableCandidate(table, _TIEBREAK, f"response type {leaf}"))

    # operationId stem is already a primary signal above; fallback adds only the input-type stem.
    raw.extend(_fallback_candidates("", input_type_stem, table_names))
    return _rank(raw)


def suggest_grpc(
    *,
    response_repeated_types: list[str],
    method_name: str,
    input_type_stem: str = "",
    table_names: list[str],
) -> list[TableCandidate]:
    """Rank tables for a gRPC method (REQ-871).

    Signals (weakest of the three protocols): the response message's repeated-field
    type, then the method-name entity stem. Falls back to the method-name affix +
    input-type stem.
    """
    index = _table_index(table_names)
    raw: list[TableCandidate] = []

    for field_type in response_repeated_types:
        table = index.get(_normalize(field_type))
        if table:
            raw.append(
                TableCandidate(table, _SCORE_GRPC_FIELD, f"repeated response field {field_type}")
            )

    stem_table = index.get(_normalize(_strip_affix(method_name)))
    if stem_table:
        raw.append(TableCandidate(stem_table, _SCORE_GRPC_METHOD, f"method stem {method_name}"))

    # method-name stem is already a primary signal above; fallback adds only the input-type stem.
    raw.extend(_fallback_candidates("", input_type_stem, table_names))
    return _rank(raw)


def _fallback_candidates(
    op_name: str, input_type_stem: str, table_names: list[str]
) -> list[TableCandidate]:
    """Shared low-confidence fallbacks: operation-name affix stem and input-type stem."""
    index = _table_index(table_names)
    out: list[TableCandidate] = []
    if op_name:
        table = index.get(_normalize(_strip_affix(op_name)))
        if table:
            out.append(TableCandidate(table, _SCORE_FALLBACK, f"name affix stem of {op_name}"))
    if input_type_stem:
        table = index.get(_type_stem(input_type_stem))
        if table:
            out.append(TableCandidate(table, _SCORE_FALLBACK, f"input-type stem {input_type_stem}"))
    return out
