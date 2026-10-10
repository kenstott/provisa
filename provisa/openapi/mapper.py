# Copyright (c) 2026 Kenneth Stott
# Canary: d928bc46-d67e-4795-ad85-16d617d3a39d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map an OpenAPI 3.x or Swagger 2.0 spec to query and mutation descriptors.

The spec is read through ``jsonschema_path``, which follows its references; nothing here resolves
one. What is here is the mapping: which operation is a table or a command, and its columns."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from jsonschema_path import SchemaPath
from referencing.exceptions import Unresolvable

from provisa.core.paging import PaginationConfig, PaginationType

# Requirements: REQ-314, REQ-316, REQ-317, REQ-408


@dataclass
class OpenAPIQuery:  # REQ-316
    operation_id: str
    path: str
    method: str = "GET"
    summary: str | None = None
    path_params: list[dict] = field(default_factory=list)  # [{name, type}]
    query_params: list[dict] = field(default_factory=list)  # [{name, type}]
    response_schema: dict | None = None  # JSON Schema of 200 response (item schema if is_list)
    is_list: bool = False  # True when the rows are an array: the response, or its rows_field
    # REQ-316: the property of the response the rows sit under (a page wrapper's ``values``);
    # None when the response is the rows. ``response_schema`` is the schema of one row there.
    rows_field: str | None = None
    # REQ-318: the paging the operation's parameters and responses suggest, offered to the steward
    # who registers the table (accepted or edited there); None when nothing suggests one.
    pagination: PaginationConfig | None = None
    # The address the operation is called at where it declares one of its own (``servers`` on
    # the operation or its path); None for the source's.
    server: str | None = None


@dataclass
class OpenAPIMutation:  # REQ-317
    operation_id: str
    path: str
    method: str
    summary: str | None = None
    input_schema: dict | None = None  # JSON Schema of requestBody
    # Its body is sent form-encoded (``application/x-www-form-urlencoded``), the only encoding the
    # operation declares; else as JSON.
    form: bool = False
    # Its body is sent as ``multipart/form-data``, the only encoding the operation declares: a
    # file upload. ``files`` names the properties that are files (``format: binary``), each given
    # as a bytea in its canonical text form.
    multipart: bool = False
    files: frozenset[str] = frozenset()
    # The address the operation is called at where it declares one of its own (``servers`` on
    # the operation or its path); None for the source's.
    server: str | None = None
    response_schema: dict | None = None
    # REQ-1924: a GET whose response declares no row schema. It changes nothing in the remote
    # system; it is a command because what it answers is not rows a table could hold.
    reads: bool = False
    # Its answer is a file (a media type that is neither text nor JSON): one binary value.
    binary: bool = False


# What an undeclared section of a spec reads as.
_NOTHING = SchemaPath.from_dict({})


def _at(node: SchemaPath, *keys: str) -> SchemaPath | None:
    """The node under ``keys``, or None where the spec does not declare it."""
    for key in keys:
        if key not in node:
            return None
        node = node / key
    return node


_SCALAR_TYPES = {"string", "number", "boolean", "integer"}

# How far a schema's properties are read: a table's columns, and the fields of an object column.
_PROPERTY_DEPTH = 2


def _schema(node: SchemaPath, depth: int = _PROPERTY_DEPTH) -> dict:
    """A schema as a table reads it: the members of an ``allOf`` as one set of properties, and
    each property likewise down to ``depth``. A schema that is one of several kinds of object
    (``anyOf``/``oneOf``, every member an object with properties) and declares no properties of
    its own reads as the properties of all of them (:func:`_one_of_objects`). References are
    followed by the spec reader."""
    schema = dict(node.read_value())
    properties: dict = {}
    for member in _at(node, "allOf") or ():
        merged = _schema(member, depth)
        properties.update(merged.pop("properties", {}))
        schema = {**merged, **schema}
    for name, prop in (_at(node, "properties") or _NOTHING).str_items():
        properties[name] = _schema(prop, depth - 1) if depth > 1 else dict(prop.read_value())
    if not properties:
        properties = _one_of_objects(node, depth)
    schema.pop("allOf", None)
    if properties:
        schema["properties"] = properties
    return schema


def _row_schema(node: SchemaPath) -> tuple[dict, bool]:
    """(the schema of one row, whether the declared schema is an array of them)."""
    is_list = node.read_value().get("type") == "array" and "items" in node
    return _schema(node / "items" if is_list else node), is_list


def _success_response(operation: SchemaPath) -> SchemaPath | None:
    responses = _at(operation, "responses")
    if responses is None:
        return None
    # A status range is written "2XX" by the OpenAPI specification and "2xx" by some authors.
    declared = {str(code).lower(): code for code in responses.keys()}
    return next(
        (responses / declared[code] for code in ("200", "2xx", "default") if code in declared),
        None,
    )


def _media(name: str) -> str:
    """A media type without its parameters (``application/json;charset=UTF-8`` is JSON)."""
    return name.split(";", 1)[0].strip().lower()


def _is_json(media: str) -> bool:
    return media == "application/json" or media.endswith("+json")


def _is_text(media: str) -> bool:
    """A media type whose answer is text, or is not said (``*/*``): JSON, XML, ``text/*``."""
    return (
        _is_json(media)
        or media.startswith("text/")
        or media == "application/xml"
        or media.endswith("+xml")
        or "*" in media
    )


def _json_schema(body: SchemaPath) -> SchemaPath | None:
    """The JSON schema a response or a request body declares: under its JSON media type
    (OpenAPI 3.x), or on the response itself (Swagger 2.0)."""
    content = _at(body, "content")
    if content is None:
        return _at(body, "schema")
    return next(
        (
            schema
            for name in content.str_keys()
            if _is_json(_media(name))
            for schema in [_at(content, name, "schema")]
            if schema is not None
        ),
        None,
    )


def _one_of_objects(node: SchemaPath, depth: int) -> dict:
    """The properties of a schema that is one of several kinds of object: every property any kind
    declares, since a row is one kind and holds that kind's. A property the kinds type
    differently is left untyped, which a column reads as text. Empty unless every member is an
    object with properties: a value that may also be a scalar (an id or the object it names) is
    not a row."""
    for keyword in ("anyOf", "oneOf"):
        kinds = [_schema(member, depth) for member in _at(node, keyword) or ()]
        if not kinds or not all(kind.get("properties") for kind in kinds):
            continue
        merged: dict = {}
        for kind in kinds:
            for name, prop in kind["properties"].items():
                if name in merged and merged[name].get("type") != prop.get("type"):
                    merged[name] = {k: v for k, v in merged[name].items() if k != "type"}
                merged.setdefault(name, prop)
        return merged
    return {}


def _answers_binary(root: SchemaPath, operation: SchemaPath) -> bool:
    """Whether the operation answers with a file: every media type it declares for its answer is
    one that is neither text nor JSON. The media types are the keys of the success response's
    ``content`` (OpenAPI 3.x), or the operation's ``produces``, else the spec's (Swagger 2.0)."""
    response = _success_response(operation)
    content = None if response is None else _at(response, "content")
    if content is not None:
        media = list(content.str_keys())
    else:
        produces = _at(operation, "produces")
        if produces is None:
            produces = _at(root, "produces")
        media = [] if produces is None else list(produces.read_value())
    return bool(media) and not any(_is_text(_media(m)) for m in media)


def _property(node: SchemaPath, name: str) -> SchemaPath | None:
    """The property ``name`` of an object schema, declared on it or on a member of its ``allOf``."""
    found = _at(node, "properties", name)
    if found is not None:
        return found
    return next(
        (p for member in _at(node, "allOf") or () for p in [_property(member, name)] if p), None
    )


# A response that carries one of these is a thing in its own right, not a page of things.
_IDENTITY = frozenset({"id", "key", "name"})


def _wrapped_rows(schema: SchemaPath) -> str | None:
    """The property a page wrapper holds its rows under (REQ-316): the response is an object
    with exactly one property that is an array of objects, and no identity of its own. Offered
    to the steward as where the table's rows are; nothing is read from it until it is accepted."""
    properties = _schema(schema, 1).get("properties") or {}
    if _IDENTITY & set(properties):
        return None
    lists = [
        name
        for name, prop in properties.items()
        if prop.get("type") == "array" and _holds_objects(_property(schema, name))
    ]
    return lists[0] if len(lists) == 1 else None


def _holds_objects(array: SchemaPath | None) -> bool:
    items = None if array is None else _at(array, "items")
    if items is None:
        return False
    item = _schema(items, 1)
    return bool(item.get("properties"))


_PROPOSED = object()  # the row location the spec suggests, where the caller names none


class NoRowsField(LookupError):
    """A row location (``rows_field``) that names no property of the operation's response."""

    def __init__(self, rows_field: str) -> None:
        self.rows_field = rows_field
        super().__init__(f"the response has no property {rows_field!r} to read rows from")


def _extract_response_schema(
    operation: SchemaPath, rows_field: object = _PROPOSED
) -> tuple[dict | None, bool, str | None]:
    """(row schema, is_list, rows_field) of the success response. Only that response is read:
    one declared beside it (``default``) is the error the remote answers with. The rows are at
    ``rows_field`` -- a property of the response, None for the response itself -- which is the
    one the spec suggests (:func:`_wrapped_rows`) unless the caller names it."""
    response = _success_response(operation)
    schema = None if response is None else _json_schema(response)
    if schema is None:
        return None, False, None
    is_array = schema.read_value().get("type") == "array"
    if rows_field is _PROPOSED:
        field = None if is_array else _wrapped_rows(schema)
    else:
        field = None if rows_field is None else str(rows_field)
    if field is None:
        return (*_row_schema(schema), None)
    rows = None if is_array else _property(schema, field)
    if rows is None:
        raise NoRowsField(field)
    return (*_row_schema(rows), field)


_FORM = "application/x-www-form-urlencoded"


_MULTIPART = "multipart/form-data"

#: How a request body is sent.
JSON_BODY, FORM_BODY, MULTIPART_BODY = "json", "form", "multipart"


def _extract_request(operation: SchemaPath) -> tuple[dict | None, str]:
    """(schema, encoding) of the request body: ``requestBody`` (OpenAPI 3.x) or the body
    parameter (Swagger 2.0). JSON where the operation declares it; else form-encoded, else
    multipart, whichever it declares."""
    body = _at(operation, "requestBody")
    schema = None if body is None else _json_schema(body)
    if schema is not None:
        return _row_schema(schema)[0], JSON_BODY
    content = (None if body is None else _at(body, "content")) or _NOTHING
    for media, encoding in ((_FORM, FORM_BODY), (_MULTIPART, MULTIPART_BODY)):
        for name in content.str_keys():
            declared = _at(content, name, "schema")
            if _media(name) == media and declared is not None:
                return _row_schema(declared)[0], encoding
    for param in _at(operation, "parameters") or ():
        if param.read_value().get("in") == "body" and "schema" in param:
            return _row_schema(param / "schema")[0], JSON_BODY
    return None, JSON_BODY


def _own_server(path_item: SchemaPath, operation: SchemaPath) -> str | None:
    """The address an operation declares for itself: its own ``servers``, else its path's. None
    where it declares neither and is called at the source's."""
    for node in (operation, path_item):
        servers = node.read_value().get("servers")
        if servers:
            return servers[0]["url"]
    return None


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _operation_id(operation: dict, method: str, path: str) -> str:
    if "operationId" in operation:
        return operation["operationId"]
    return _slugify(f"{method}_{path}")


def _parameters(path_item: SchemaPath, operation: SchemaPath | None) -> list[dict]:
    """The operation's parameters: the path's, overridden by the operation's own (by name+in)."""
    merged = {
        (p["name"], p["in"]): p
        for node in (path_item, operation)
        if node is not None
        for p in (param.read_value() for param in _at(node, "parameters") or ())
    }
    return list(merged.values())


def _extract_params(params: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split parameters into path_params and query_params."""
    path_params: list[dict] = []
    query_params: list[dict] = []
    for p in params:
        location = p.get("in", "")
        schema = p.get("schema", {})
        param_type = schema.get("type") if isinstance(schema, dict) else p.get("type", "string")
        # OpenAPI: a parameter is required when it says so; a path parameter always is, and
        # the specification's own default for the others is false.
        entry = {
            "name": p.get("name", ""),
            "type": param_type or "string",
            "required": location == "path" or p.get("required") is True,
        }
        if location == "path":
            path_params.append(entry)
        elif location == "query":
            query_params.append(entry)
    return path_params, query_params


def operation_parameters(spec: dict, path: str, method: str = "get") -> list[dict]:
    """The declared parameters of ``method`` at ``path``; none where the spec has no such path."""
    path_item = _at(SchemaPath.from_dict(spec), "paths", path)
    if path_item is None:
        return []
    return _parameters(path_item, _at(path_item, method))


# Query parameter names that say how an operation pages (REQ-318). First match wins.
_PAGE_NAMES = ("page", "page_number", "pageNumber", "page_no")
_OFFSET_NAMES = ("offset", "skip", "start", "startAt")
# A parameter that carries the last row's id, and the row property it carries.
_AFTER_NAMES = ("starting_after",)
_ROW_ID = "id"
_SIZE_NAMES = (
    "limit",
    "per_page",
    "perPage",
    "page_size",
    "pageSize",
    "pagelen",
    "maxResults",
    "size",
    "top",
    "count",
)


def _declares_link_header(operation: SchemaPath) -> bool:
    """Whether the operation's success response declares a ``Link`` header (RFC 8288 paging)."""
    response = _success_response(operation)
    headers = None if response is None else _at(response, "headers")
    return headers is not None and any(name.lower() == "link" for name in headers.str_keys())


def _answered_cursor(operation: SchemaPath, names: list[str]) -> str | None:
    """The query parameter the answer carries the next value of: ``x`` where the success
    response declares a ``next_x`` property."""
    response = _success_response(operation)
    answer = None if response is None else _json_schema(response)
    if answer is None:
        return None
    return next((n for n in names if _property(answer, f"next_{n}") is not None), None)


def propose_paging(
    operation: SchemaPath,
    query_params: list[dict],
    is_list: bool,
    rows_field: str | None = None,
    row_properties: frozenset[str] = frozenset(),
) -> PaginationConfig | None:
    """The paging a GET operation suggests, from what it declares: where its rows are
    (``rows_field``, a page wrapper's property), and how it pages -- a parameter that starts a
    page after the last row's id (the rows, ``row_properties``, having one), a page-number
    parameter, an offset with a size parameter, or a ``Link`` response header. Only a list is
    paged. A cursor the answer carries is proposed where the spec names it: a parameter ``x``
    beside a ``next_x`` property of the answer, with the size parameter where it declares one."""
    if not is_list:
        return None
    declared: dict = {} if rows_field is None else {"rows_field": rows_field}
    names = [p["name"] for p in query_params]
    size = next((n for n in _SIZE_NAMES if n in names), None)
    page = next((n for n in _PAGE_NAMES if n in names), None)
    offset = next((n for n in _OFFSET_NAMES if n in names), None)
    after = next((n for n in _AFTER_NAMES if n in names), None)
    if after is not None and size is not None and _ROW_ID in row_properties:
        declared |= {
            "type": PaginationType.last_row,
            "cursor_param": after,
            "cursor_field": _ROW_ID,
            "page_size_param": size,
        }
    elif (cursor := _answered_cursor(operation, names)) is not None:
        declared |= {
            "type": PaginationType.cursor,
            "cursor_param": cursor,
            "cursor_field": f"next_{cursor}",
        }
        if size is not None:
            declared["page_size_param"] = size
    elif page is not None:
        declared |= {"type": PaginationType.page_number, "page_param": page}
        if size is not None:
            declared["page_size_param"] = size
    elif offset is not None and size is not None:
        declared |= {
            "type": PaginationType.offset,
            "page_param": offset,
            "page_size_param": size,
        }
    elif _declares_link_header(operation):
        declared["type"] = PaginationType.link_header
    return PaginationConfig.model_validate(declared) if declared else None


def parse_spec(
    spec: dict,
    operation_overrides: dict[str, str] | None = None,
    rows_fields: dict[str, str | None] | None = None,
) -> tuple[list[OpenAPIQuery], list[OpenAPIMutation]]:  # REQ-314, REQ-316, REQ-317, REQ-408
    """Parse an OpenAPI 3.x or Swagger 2.0 spec into queries and mutations.

    operation_overrides: {operationId: "query" | "mutation"} — takes priority over x-provisa-kind.
    rows_fields: {operationId: where a registered table's rows are} — the property its paging
    names, None for the response itself; an operation not in it takes what the spec suggests.
    """
    try:
        return _map_operations(
            SchemaPath.from_dict(spec), operation_overrides or {}, rows_fields or {}
        )
    except Unresolvable as exc:
        # The reader's own message carries the whole spec; name the reference only.
        raise ValueError(f"unresolvable $ref: {exc.ref}") from None


def _map_operations(
    root: SchemaPath, overrides: dict[str, str], rows_fields: dict[str, str | None]
) -> tuple[list[OpenAPIQuery], list[OpenAPIMutation]]:
    queries: list[OpenAPIQuery] = []
    mutations: list[OpenAPIMutation] = []

    for path, path_item in (_at(root, "paths") or _NOTHING).str_items():
        if not isinstance(path_item.read_value(), dict):
            continue

        for method in ("get", "post", "put", "patch", "delete"):
            operation = _at(path_item, method)
            if operation is None:
                continue
            raw = operation.read_value()
            binary = _answers_binary(root, operation)
            path_params, query_params = _extract_params(_parameters(path_item, operation))
            op_id = _operation_id(raw, method, path)
            summary = raw.get("summary") or raw.get("description")
            response_schema, is_list, rows_field = _extract_response_schema(
                operation, rows_fields[op_id] if op_id in rows_fields else _PROPOSED
            )

            # Payload override > x-provisa-kind > a GET that declares the rows it answers with
            explicit_kind = (
                overrides.get(op_id, "").lower() or (raw.get("x-provisa-kind") or "").lower()
            )
            response_is_scalar = (
                isinstance(response_schema, dict) and response_schema.get("type") in _SCALAR_TYPES
            )
            reads = method == "get" and explicit_kind != "mutation"
            is_query = not (response_is_scalar or binary) and (
                explicit_kind == "query" or (reads and response_schema is not None)
            )

            if is_query:
                queries.append(
                    OpenAPIQuery(
                        operation_id=op_id,
                        path=path,
                        method=method.upper(),
                        summary=summary,
                        path_params=path_params,
                        query_params=query_params,
                        response_schema=response_schema,
                        is_list=is_list,
                        rows_field=rows_field,
                        server=_own_server(path_item, operation),
                        pagination=propose_paging(
                            operation,
                            query_params,
                            is_list,
                            rows_field,
                            frozenset((response_schema or {}).get("properties") or ()),
                        ),
                    )
                )
            else:
                request_schema, encoding = _extract_request(operation)
                body_properties = (request_schema or {}).get("properties") or {}
                mutations.append(
                    OpenAPIMutation(
                        operation_id=op_id,
                        path=path,
                        method=method.upper(),
                        summary=summary,
                        input_schema=request_schema,
                        form=encoding == FORM_BODY,
                        multipart=encoding == MULTIPART_BODY,
                        files=frozenset(
                            name
                            for name, prop in body_properties.items()
                            if encoding == MULTIPART_BODY and prop.get("format") == "binary"
                        ),
                        server=_own_server(path_item, operation),
                        response_schema=response_schema,
                        reads=reads,
                        binary=binary,
                    )
                )

    return queries, mutations


def classify_operation(
    method: str, _path: str, operation: dict
) -> str:  # REQ-408  # pyright: ignore[reportUnusedParameter]
    """Return 'query' or 'mutation' for an OpenAPI operation."""
    explicit_kind = (operation.get("x-provisa-kind") or "").lower()
    if explicit_kind in ("query", "mutation"):
        return explicit_kind
    return "query" if method.lower() == "get" else "mutation"
