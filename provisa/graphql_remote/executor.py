# Copyright (c) 2026 Kenneth Stott
# Canary: f912261d-dbda-4025-83cd-f63d504ab859
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute queries against a remote GraphQL endpoint (REQ-309)."""

from __future__ import annotations
import json
import re
from collections.abc import Iterator
from typing import Any

import httpx
from provisa.graphql_remote.introspect import _build_headers

# Requirements: REQ-307, REQ-309, REQ-310, REQ-313


def _safe_json(resp: httpx.Response) -> dict:
    """Parse JSON response, fixing lone \\u escapes that aren't valid JSON."""
    try:
        return resp.json()
    except json.JSONDecodeError:
        # Some servers emit backslash-u not followed by 4 hex digits (e.g. Windows paths).
        fixed = re.sub(r"(?<!\\)\\u(?![0-9a-fA-F]{4})", r"\\\\u", resp.text)
        return json.loads(fixed)


_OBJECT_FIELD_RE = re.compile(r"Field '([^']+)' of type '[^']+' must have a selection of subfields")

# Matches a column selection that aliases a single-subfield object projection down to a scalar,
# e.g. ``employee_id: employee { id }`` — the alias is the sql column name, the braces hold exactly
# one subfield. Rows come back keyed by the alias but holding a nested {subfield: value} dict; this
# flattens it to the scalar the column is declared as, per the alias's own selection contract.
_SCALAR_PROJECTION_RE = re.compile(r"^(\w+):\s*\w+\s*\{\s*(\w+)\s*\}$")


def _object_fields_from_errors(errors: list) -> set[str]:
    """Extract field names that require subfield selection from GQL error list."""
    found: set[str] = set()
    for err in errors:
        m = _OBJECT_FIELD_RE.search(str(err.get("message", "")))
        if m:
            found.add(m.group(1))
    return found


def _flatten_scalar_projections(rows: list[dict], columns: list[str]) -> None:
    """Unwrap ``alias: field { subfield }`` projections in place from {subfield: v} to v."""
    projections = [m.groups() for c in columns if (m := _SCALAR_PROJECTION_RE.match(c.strip()))]
    if not projections:
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        for alias, subfield in projections:
            v = row.get(alias)
            if isinstance(v, dict):
                row[alias] = v.get(subfield)


async def execute_remote(  # REQ-309, REQ-307, REQ-310, REQ-313
    url: str,
    auth: dict | None,
    field_name: str,
    columns: list[str],
    variables: dict | None = None,
    required_args: list[dict] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    pagination: dict | None = None,
) -> list[dict]:
    """Build a minimal GraphQL query and forward to the remote endpoint.

    Returns list of row dicts from data.<field_name>.
    Raises httpx.HTTPError on network failure.
    Raises ValueError if the response contains errors.

    If the server rejects OBJECT-type fields (no subselection), retries once
    with those fields removed so scalar columns are still returned.

    When pagination is provided and the remote supports limit/offset args,
    passes them as literal arg values to cap rows at the remote rather than
    fetching all and truncating locally.
    """
    selected_cols = list(columns)
    headers = {"Content-Type": "application/json", **_build_headers(auth)}

    pagination_arg_strs: list[str] = []
    if pagination and limit is not None:
        limit_arg = pagination.get("limit_arg")
        if limit_arg:
            pagination_arg_strs.append(f"{limit_arg}: {limit}")
        offset_arg = pagination.get("offset_arg")
        if offset is not None and offset_arg:
            pagination_arg_strs.append(f"{offset_arg}: {offset}")

    data: dict = {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        for attempt in range(2):
            col_selection = "\n".join(selected_cols) if selected_cols else "__typename"
            if variables and required_args:
                var_decls = ", ".join(
                    f"${a['name']}: {a['gql_type']}"
                    for a in required_args
                    if a["name"] in variables
                )
                required_arg_strs = [
                    f"{a['name']}: ${a['name']}" for a in required_args if a["name"] in variables
                ]
                all_arg_strs = required_arg_strs + pagination_arg_strs
                arg_pass = ", ".join(all_arg_strs)
                query = f"query({var_decls}) {{ {field_name}({arg_pass}) {{ {col_selection} }} }}"
            elif pagination_arg_strs:
                arg_pass = ", ".join(pagination_arg_strs)
                query = f"query {{ {field_name}({arg_pass}) {{ {col_selection} }} }}"
            else:
                query = f"query {{ {field_name} {{ {col_selection} }} }}"
            payload: dict = {"query": query}
            if variables:
                payload["variables"] = variables
            resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = _safe_json(resp)
            if "errors" in data:
                object_fields = _object_fields_from_errors(data["errors"])
                if object_fields and attempt == 0:
                    selected_cols = [c for c in selected_cols if c.split()[0] not in object_fields]
                    continue
                raise ValueError(f"Remote GraphQL errors: {data['errors']}")
            break

    rows = data.get("data", {}).get(field_name, [])
    rows = rows if isinstance(rows, list) else [rows]
    _flatten_scalar_projections(rows, selected_cols)
    return rows


def iter_remote_rows_spooled(  # REQ-1915
    url: str, auth: dict | None, field_name: str, columns: list[str], spooled: Any
) -> Iterator[dict]:
    """The whole-collection read of :func:`execute_remote` (no variables, no paging), with the
    endpoint's answer written to a spool file as it arrives and parsed from the file: first its
    ``errors`` (same one retry without the fields that need a subselection), then the rows of
    ``data.<field_name>`` one at a time.

    The document is parsed as the endpoint sent it: the repair :func:`_safe_json` makes to an
    invalid ``\\u`` escape needs the whole text in memory and is not applied, so such an answer
    fails the read with the parser's error."""
    from provisa.federation.replica_spool import json_items, json_starts

    selected = list(columns)
    headers = {"Content-Type": "application/json", **_build_headers(auth)}
    with httpx.Client(timeout=30.0) as client:
        for attempt in range(2):
            selection = "\n".join(selected) if selected else "__typename"
            payload = {"query": f"query {{ {field_name} {{ {selection} }} }}"}
            with spooled(
                lambda: client.stream("POST", url, json=payload, headers=headers)  # noqa: B023
            ) as body:
                errors = list(json_items(body, "errors.item"))
                if errors:
                    object_fields = _object_fields_from_errors(errors)
                    if object_fields and attempt == 0:
                        selected = [c for c in selected if c.split()[0] not in object_fields]
                        continue
                    raise ValueError(f"Remote GraphQL errors: {errors}")
                at = f"data.{field_name}"
                # A list field is read an element at a time; a single object is one row.
                rows = (
                    json_items(body, f"{at}.item")
                    if json_starts(body, at) == "start_array"
                    else json_items(body, at)
                )
                for row in rows:
                    if row is None:
                        continue
                    _flatten_scalar_projections([row], selected)
                    yield row
            return
