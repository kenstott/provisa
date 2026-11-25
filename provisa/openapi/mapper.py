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

import logging
import re
from dataclasses import dataclass, field

from jsonschema_path import SchemaPath
from referencing.exceptions import Unresolvable

from provisa.core.paging import PaginationConfig, PaginationType

# Requirements: REQ-314, REQ-316, REQ-317, REQ-408, REQ-2265, REQ-1874

log = logging.getLogger(__name__)


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
    # REQ-1924: a GET whose response declares no row schema, offered as a table whose columns
    # the steward declares when registering it (``response_schema`` is None); it is offered as a
    # command that reads too.
    columns_declared: bool = False
    # REQ-1986: the type of one row as the spec names it -- the component its row schema is a
    # reference to (``Pet`` of ``#/components/schemas/Pet``); None where the spec writes the
    # schema in place, which names no type. Two operations return the same type when this is
    # the same name.
    returns_type: str | None = None


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
    with no identity of its own and exactly one property that is an array of objects -- or,
    where it has several (a page's rows beside its errors and warnings, REQ-2265), exactly one
    of them that the response requires. Offered to the steward as where the table's rows are;
    nothing is read from it until it is accepted."""
    read = _schema(schema, 1)
    properties = read.get("properties") or {}
    if _IDENTITY & set(properties):
        return None
    lists = [
        name
        for name, prop in properties.items()
        if prop.get("type") == "array" and _holds_objects(_property(schema, name))
    ]
    if len(lists) > 1:
        lists = [name for name in lists if name in (read.get("required") or ())]
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


def _reference_name(node: SchemaPath) -> str | None:
    """The component ``node`` is written as a reference to, by its name; None when the spec
    writes the schema in place. The spec reader follows a reference wherever it reads one, so
    the reference is read from what holds the node."""
    with node.parent.open() as held:
        written = held.get(node.parts[-1]) if isinstance(held, dict) else None
    reference = written.get("$ref") if isinstance(written, dict) else None
    return reference.rsplit("/", 1)[-1] if isinstance(reference, str) else None


def _row_type(operation: SchemaPath, rows_field: str | None) -> str | None:  # REQ-1986
    """The named type of one row of the success response, its rows at ``rows_field`` as
    :func:`_extract_response_schema` settled it: read from the spec's own references, never
    from what the schema holds."""
    response = _success_response(operation)
    schema = None if response is None else _json_schema(response)
    if schema is None:
        return None
    rows = schema if rows_field is None else _property(schema, rows_field)
    if rows is None:
        return None
    if rows.read_value().get("type") == "array" and "items" in rows:
        rows = rows / "items"
    return _reference_name(rows)


_FORM = "application/x-www-form-urlencoded"


_MULTIPART = "multipart/form-data"

#: How a request body is sent.
JSON_BODY, FORM_BODY, MULTIPART_BODY = "json", "form", "multipart"
#: The request schema of a JSON body declared without one: any JSON object.
UNTYPED_BODY: dict = {"type": "object"}


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
    if any(_is_json(_media(name)) for name in content.str_keys()):
        # A JSON body the operation declares without its schema (as ServiceNow publishes every
        # API's, REQ-1954): it is still taken, as one untyped JSON object sent as given.
        return UNTYPED_BODY, JSON_BODY
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


# Parameters that page backwards: never the cursor a read follows to the end.
_BACKWARD_NAMES = frozenset({"before", "ending_before", "prev", "previous"})


# How deep the answer is searched for the next value of a cursor: HubSpot's
# ``paging.next.after`` is three properties down (REQ-1874).
_CURSOR_DEPTH = 4


def _words(name: str) -> tuple[str, ...]:
    """A name's words, lower case, whether it is written snake_case or camelCase."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return tuple(w for w in spaced.lower().split("_") if w)


def _cursor_words(param: str) -> tuple[str, ...]:
    """The words of what a cursor parameter carries: its own, less a leading ``start``, which
    says where a page starts rather than what (Notion's ``start_cursor`` carries a cursor)."""
    words = _words(param)
    return words[1:] if len(words) > 1 and words[0] == "start" else words


def _is_value(prop: dict) -> bool:
    """Whether a property holds a value a parameter can carry, not an object or a list."""
    kinds = prop.get("type")
    kinds = set(kinds) if isinstance(kinds, list) else {kinds}
    return not (kinds & {"object", "array"} or "properties" in prop)


def _named_next(
    properties: dict, names: list[str], skip: str | None, path: tuple[str, ...] = ()
) -> list[tuple[str, str]]:
    """Every (parameter, dotted path) where the answer names the next value of a parameter
    ``x``: a value property ``next_x`` or ``nextX`` at any depth (``pagination.next_page_token``,
    ``pagination.nextCursor``), or ``x`` itself inside an object named ``next``
    (``paging.next.after``). ``skip`` is the rows, which are not searched."""
    found: list[tuple[str, str]] = []
    for name, prop in properties.items():
        if name == skip or not isinstance(prop, dict):
            continue
        here = (*path, name)
        if _is_value(prop):
            words = _words(name)
            in_next = bool(path) and _words(path[-1]) == ("next",)
            for param in names:
                base = _cursor_words(param)
                if words == ("next", *base) or (in_next and words == base):
                    found.append((param, ".".join(here)))
        elif "properties" in prop:
            found += _named_next(prop["properties"], names, None, here)
    return found


def _answered_cursors(
    operation: SchemaPath, query_params: list[dict], rows_field: str | None
) -> list[tuple[str, str]]:
    """Each (query parameter the answer carries the next value of, where in the answer it is):
    ``x`` where the success response names its next value (:func:`_named_next`), at any depth
    and in snake_case or camelCase, the dotted path naming where (REQ-1874); else (REQ-2265) a
    text parameter ``x`` the answer holds as text, beside its rows (``scrollId``) or in an
    object property of its own (``paging.after``). A parameter that pages backwards, sizes or
    numbers a page, or starts it at an offset is not one of the latter. More than one where the
    answer names the next value in more than one place; none where it holds no cursor."""
    response = _success_response(operation)
    answer = None if response is None else _json_schema(response)
    if answer is None:
        return []
    names = [p["name"] for p in query_params]
    named = _named_next(_schema(answer, _CURSOR_DEPTH).get("properties") or {}, names, rows_field)
    if named:
        return named
    excluded = _BACKWARD_NAMES.union(_SIZE_NAMES, _PAGE_NAMES, _OFFSET_NAMES)
    tokens = [
        p["name"] for p in query_params if p["type"] == "string" and p["name"] not in excluded
    ]
    properties = _schema(answer, 2).get("properties") or {}
    for name in tokens:
        if properties.get(name, {}).get("type") == "string":
            return [(name, name)]
    for name in tokens:
        for holder, prop in properties.items():
            if holder == rows_field or prop.get("type") not in ("object", None):
                continue
            if (prop.get("properties") or {}).get(name, {}).get("type") == "string":
                return [(name, f"{holder}.{name}")]
    return []


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
    beside a ``next_x`` property of the answer, or one the answer holds itself or in an object
    property (:func:`_answered_cursors`), with the size parameter where it declares one. Where
    the answer names the next page's cursor in more than one place, no paging is suggested and
    the places are logged: which one the remote fills is not in the spec (REQ-1874)."""
    if not is_list:
        return None
    declared: dict = {} if rows_field is None else {"rows_field": rows_field}
    cursors = _answered_cursors(operation, query_params, rows_field)
    if len(cursors) > 1:
        log.warning(
            "%s %s: no paging suggested: the answer names the next page's cursor in %d places "
            "(%s), and the spec does not say which one the remote fills",
            str(operation.parts[-1]).upper(),
            operation.parts[-2],
            len(cursors),
            ", ".join(f"{param} <- {path}" for param, path in cursors),
        )
        return PaginationConfig.model_validate(declared) if declared else None
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
    elif cursors:
        ((cursor_param, cursor_field),) = cursors
        declared |= {
            "type": PaginationType.cursor,
            "cursor_param": cursor_param,
            "cursor_field": cursor_field,
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
            # REQ-1874, REQ-1924: rows whose schema has no properties (``{type: object}``, an
            # empty ``allOf``, a reference to a propertyless component) are no more typed than
            # rows with no schema at all.
            rows_typed = bool((response_schema or {}).get("properties"))
            reads = method == "get" and explicit_kind != "mutation"
            is_query = not (response_is_scalar or binary) and (
                explicit_kind == "query" or (reads and rows_typed)
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
                        returns_type=_row_type(operation, rows_field),
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
                if reads and not (rows_typed or response_is_scalar or binary):
                    # REQ-1924: nothing says what its rows hold, so the steward declares its
                    # columns when registering it as a table.
                    queries.append(
                        OpenAPIQuery(
                            operation_id=op_id,
                            path=path,
                            method=method.upper(),
                            summary=summary,
                            path_params=path_params,
                            query_params=query_params,
                            response_schema=None,
                            is_list=True,
                            server=_own_server(path_item, operation),
                            columns_declared=True,
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
