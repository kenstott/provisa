# Copyright (c) 2026 Kenneth Stott
# Canary: e6b1702e-7879-4727-ac41-ec12b02a5845
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How an OpenAPI operation's names and JSON Schema types become Provisa names and column types.

An OpenAPI source registers nothing on its own: its GET operations are tables on offer and its
other operations commands on offer, each registered one at a time (REQ-316, REQ-317)."""

from __future__ import annotations
import logging
import re
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    pass

# Requirements: REQ-314, REQ-316, REQ-317, REQ-319, REQ-320, REQ-321

log = logging.getLogger(__name__)

_VERB_PREFIXES = (
    "get",
    "list",
    "fetch",
    "search",
    "find",
    "query",
    "create",
    "post",
    "add",
    "insert",
    "update",
    "put",
    "patch",
    "edit",
    "delete",
    "remove",
    "destroy",
)


def _singularize(word: str) -> str:
    """Best-effort English singularization for the noun segment of an alias."""
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith("ses") or word.endswith("xes") or word.endswith("zes"):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 2:
        return word[:-1]
    return word


def _operation_id_to_alias(op_id: str) -> str:
    """Convert camelCase/PascalCase/snake_case operationId to a snake_case alias.

    Format: {noun_singular}_{modifiers} (e.g. findPetsByStatus → pet_by_status).
    """
    # camelCase / PascalCase → snake_case (intermediate normalization for verb-stripping parse)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", op_id)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()
    # strip leading verb segment
    for verb in _VERB_PREFIXES:
        if s.startswith(verb + "_"):
            s = s[len(verb) + 1 :]
            break
        if s == verb:
            return s
    # singularize the first (noun) segment
    parts = s.split("_", 1)
    parts[0] = _singularize(parts[0])
    return "_".join(parts) or op_id.lower()


_OPENAPI_TYPE_MAP = {
    "string": "string",
    "integer": "integer",
    "number": "number",
    "boolean": "boolean",
    "array": "jsonb",
    "object": "jsonb",
}


def _openapi_to_provisa_type(t: str | None) -> str:
    return _OPENAPI_TYPE_MAP.get(t or "string", "string")


def _schema_to_columns(schema: dict | None) -> list[dict]:
    """Extract column list from a JSON Schema object or array-of-objects schema."""
    if not schema:
        return []
    # Unwrap array wrapper
    if schema.get("type") == "array" and "items" in schema:
        schema = schema["items"]
    assert schema is not None
    props = schema.get("properties", {})
    if not props and isinstance(schema.get("additionalProperties"), dict):
        # Map-shaped response (e.g. {"available": 3, "sold": 12}) has no fixed property
        # names — api_source.flattener.flatten_response flattens it to {"status": k, "count": v} rows,
        # so the registered columns must match.
        value_type = schema["additionalProperties"].get("type")
        return [
            {"name": "status", "type": "string"},
            {"name": "count", "type": _openapi_to_provisa_type(value_type)},
        ]
    cols = []
    for name, prop in props.items():
        col: dict = {"name": name, "type": _openapi_to_provisa_type(prop.get("type"))}
        desc = prop.get("description") or prop.get("title")
        if desc:
            col["description"] = desc
        if prop.get("type") == "object" and prop.get("properties"):
            sub_fields = []
            for sub_name, sub_prop in prop["properties"].items():
                sf: dict = {
                    "name": sub_name,
                    "type": _openapi_to_provisa_type(sub_prop.get("type")),
                }
                sub_desc = sub_prop.get("description") or sub_prop.get("title")
                if sub_desc:
                    sf["description"] = sub_desc
                sub_fields.append(sf)
            col["object_fields"] = sub_fields
        cols.append(col)
    return cols
