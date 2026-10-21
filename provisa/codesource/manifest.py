# Copyright (c) 2026 Kenneth Stott
# Canary: c3d79418-e2eb-4760-8dd3-6cdcab732867
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a code source's bundle offers, from what its declarations say (REQ-1984, REQ-1993).

Every language's reader writes the same document (docs/arch/code-sources.md, section 2); Node's
(provisa/node/reader.mjs) writes every export of the bundle's entry module with its
JSDoc tags and its signature's types as trees. This module applies REQ-1984's rules to that:

- an export tagged ``@table`` is a table, ``@command`` a command, ``@stream`` a live table,
  ``@columns`` a batch column transform (REQ-2131); untagged exports are ignored;
- a tag that is neither one of those nor a standard JSDoc tag is a problem named by the export
  and the tag;
- a ``@table`` returns a collection (an array or an iterable, synchronous or asynchronous) whose
  elements are its rows; one that returns a single object is a problem by name;
- a ``@stream`` is an async generator (its declaration returns ``AsyncGenerator<T>`` or
  ``AsyncIterable<T>``), with the same parameter and column rules as ``@table``;
- a ``@columns`` function takes a batch: each parameter is one input column, a list of its values
  in row order, and it returns a list holding one result per row, each an object whose fields are
  the output columns (REQ-2131); a parameter that is not a list, or a result that is not a list of
  objects, is a problem by name;
- a non-optional parameter is required by the source; an optional one is not;
- the columns are the row type's fields, or -- when the row type is loose (``any``, ``unknown``
  or a ``Record``) -- found by a preview;
- every type a tagged function refers to is mapped (string -> text, number -> double, bigint ->
  bigint, boolean -> boolean, Date -> timestamp, ``T | null`` / ``T | undefined`` / an optional
  field -> nullable ``T``, arrays -> lists, interfaces and object types -> named object types, a
  union of string literals -> an enum, ``any`` / ``unknown`` / ``Record`` -> json), and one that
  cannot be mapped is a problem named by where it is used.

Every problem is collected, and the bundle is refused with all of them at once.
"""

# Requirements: REQ-1984, REQ-1993, REQ-2131
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.codesource.bundle import BundleRefused

TABLE, COMMAND, STREAM, COLUMNS = "table", "command", "stream", "columns"
PROVISA_TAGS = frozenset({TABLE, COMMAND, STREAM, COLUMNS})

#: The standard JSDoc and TSDoc tags an export may carry beside Provisa's. Any other is unknown.
STANDARD_TAGS = frozenset(
    {
        "param", "arg", "argument", "returns", "return", "example", "deprecated", "see",
        "since", "throws", "exception", "remarks", "privateRemarks", "link", "linkcode",
        "linkplain", "typeParam", "template", "type", "typedef", "property", "prop", "author",
        "internal", "beta", "alpha", "experimental", "public", "private", "protected", "readonly",
        "default", "defaultValue", "override", "virtual", "sealed", "packageDocumentation",
        "inheritDoc", "label", "summary", "description", "desc", "async", "function", "func",
        "method", "file", "fileoverview", "module", "version", "license", "copyright", "ignore",
        "todo", "callback", "constant", "const", "abstract", "access", "category",
    }
)  # fmt: skip


@dataclass(frozen=True)
class Rules:
    """What a language's declarations may say beside Provisa's tags."""

    #: The language's own documentation tags, which an export may carry and Provisa ignores.
    #: Any other tag is unknown, and a problem.
    standard_tags: frozenset[str]
    #: Whether a ``@stream`` may be a plain (synchronous) generator as well as an async one.
    sync_streams: bool


#: Node (REQ-1984): JSDoc and TSDoc tags; a stream is an async generator.
NODE_RULES = Rules(standard_tags=STANDARD_TAGS, sync_streams=False)
#: Python (REQ-1993): a docstring has no standard tag beginning with ``@``, so every one but
#: Provisa's is unknown; a stream is a generator or an async generator.
PYTHON_RULES = Rules(standard_tags=frozenset(), sync_streams=True)


# -- shapes -----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Scalar:
    #: The IR type: text, double, bigint, numeric, boolean, timestamp, date or json.
    ir: str


@dataclass(frozen=True)
class ListOf:
    element: "Shape"


@dataclass(frozen=True)
class Named:
    """A named object type, in :attr:`Manifest.types`."""

    name: str


@dataclass(frozen=True)
class Anonymous:
    """An object type with no name of its own (``{ ok: boolean }``)."""

    fields: tuple["Field", ...]


@dataclass(frozen=True)
class EnumOf:
    #: The union's alias name, or None for an inline union.
    name: str | None
    values: tuple[str, ...]


Shape = Scalar | ListOf | Named | Anonymous | EnumOf

#: Loose: a value of any shape, served as json (and, as a row type, typed by a preview).
LOOSE = Scalar("json")


@dataclass(frozen=True)
class Field:
    name: str
    shape: Shape
    nullable: bool
    #: The field's own documentation (a Pydantic Field's description, a property's doc comment):
    #: its column's description. Empty when the code gives none.
    description: str = ""


@dataclass(frozen=True)
class ObjectType:
    name: str
    fields: tuple[Field, ...]


@dataclass(frozen=True)
class Parameter:
    name: str
    shape: Shape
    nullable: bool
    #: A non-optional parameter is required by the source (REQ-1986's native_filter_required).
    required: bool


@dataclass(frozen=True)
class Export:
    name: str
    kind: str
    description: str
    parameters: tuple[Parameter, ...]
    #: A table's or stream's row type; a command's result (None: it returns nothing); a
    #: ``@columns`` function's result row, whose fields are its output columns.
    returns: Shape | None
    #: Whether the columns are found by a preview: the row type is loose.
    preview: bool = False
    #: A command's result: rows (a collection) or one value.
    many: bool = False


@dataclass(frozen=True)
class Manifest:
    exports: tuple[Export, ...]
    types: dict[str, ObjectType]

    def export(self, name: str) -> Export:
        return next(e for e in self.exports if e.name == name)


# -- kept with a built source (REQ-2098) -----------------------------------------------------------
#
# A source in built mode runs on a remote host and is not read on the server (its code may need
# what the server does not have), so the manifest it was read with in dev mode is kept with it.

_SHAPES = {"scalar": Scalar, "list": ListOf, "named": Named, "anonymous": Anonymous, "enum": EnumOf}


def _shape_json(shape: Shape | None) -> Any:
    if shape is None:
        return None
    if isinstance(shape, Scalar):
        return {"kind": "scalar", "ir": shape.ir}
    if isinstance(shape, ListOf):
        return {"kind": "list", "element": _shape_json(shape.element)}
    if isinstance(shape, Named):
        return {"kind": "named", "name": shape.name}
    if isinstance(shape, Anonymous):
        return {"kind": "anonymous", "fields": [_field_json(f) for f in shape.fields]}
    return {"kind": "enum", "name": shape.name, "values": list(shape.values)}


def _field_json(field: Field) -> dict:
    return {
        "name": field.name,
        "shape": _shape_json(field.shape),
        "nullable": field.nullable,
        "description": field.description,
    }


def _shape(raw: Any) -> Shape | None:
    if raw is None:
        return None
    kind = raw["kind"]
    if kind == "scalar":
        return Scalar(raw["ir"])
    if kind == "list":
        element = _shape(raw["element"])
        assert element is not None
        return ListOf(element)
    if kind == "named":
        return Named(raw["name"])
    if kind == "anonymous":
        return Anonymous(tuple(_field(f) for f in raw["fields"]))
    if kind == "enum":
        return EnumOf(raw["name"], tuple(raw["values"]))
    raise ValueError(f"a kept manifest holds an unknown shape {kind!r}")


def _field(raw: dict) -> Field:
    shape = _shape(raw["shape"])
    assert shape is not None
    return Field(raw["name"], shape, raw["nullable"], raw["description"])


def manifest_json(manifest: Manifest) -> dict:
    """``manifest`` as JSON, to keep with a built source."""
    return {
        "exports": [
            {
                "name": e.name,
                "kind": e.kind,
                "description": e.description,
                "parameters": [
                    {
                        "name": p.name,
                        "shape": _shape_json(p.shape),
                        "nullable": p.nullable,
                        "required": p.required,
                    }
                    for p in e.parameters
                ],
                "returns": _shape_json(e.returns),
                "preview": e.preview,
                "many": e.many,
            }
            for e in manifest.exports
        ],
        "types": {
            name: {"name": t.name, "fields": [_field_json(f) for f in t.fields]}
            for name, t in manifest.types.items()
        },
    }


def manifest_from_json(raw: dict) -> Manifest:
    """A manifest kept with a built source."""
    exports = []
    for e in raw["exports"]:
        parameters = []
        for p in e["parameters"]:
            shape = _shape(p["shape"])
            assert shape is not None
            parameters.append(Parameter(p["name"], shape, p["nullable"], p["required"]))
        exports.append(
            Export(
                e["name"],
                e["kind"],
                e["description"],
                tuple(parameters),
                _shape(e["returns"]),
                e["preview"],
                e["many"],
            )
        )
    types = {
        name: ObjectType(t["name"], tuple(_field(f) for f in t["fields"]))
        for name, t in raw["types"].items()
    }
    return Manifest(tuple(exports), types)


class ManifestRefused(BundleRefused):
    """A bundle whose declarations break a rule: every problem, each by name."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__(
            "manifest_refused",
            "the bundle's declarations cannot be used: " + "; ".join(problems),
            problems="; ".join(problems),
        )
        self.problems = problems


# -- mapping ----------------------------------------------------------------------------------------


class _Mapper:
    def __init__(self, raw_types: dict[str, Any]) -> None:
        self.raw_types = raw_types
        self.types: dict[str, ObjectType] = {}
        self.problems: list[str] = []
        self._mapping: set[str] = set()

    def fail(self, where: str, reason: str) -> Shape:
        self.problems.append(f"{where}: {reason}")
        return LOOSE

    def shape(self, node: dict, where: str) -> tuple[Shape, bool]:
        """``node``'s shape and whether it is nullable."""
        kind = node["kind"]
        if kind == "union":
            present = [m for m in node["members"] if m["kind"] not in ("undefined", "null")]
            nullable = len(present) != len(node["members"])
            kinds = {m["kind"] for m in present}
            if kinds == {"boolean"}:
                return Scalar("boolean"), nullable
            if (
                present
                and kinds == {"literal"}
                and all(isinstance(m["value"], str) for m in present)
            ):
                return EnumOf(node.get("name"), tuple(m["value"] for m in present)), nullable
            if len(present) == 1:
                inner, inner_nullable = self.shape(present[0], where)
                return inner, nullable or inner_nullable
            return self.fail(
                where, f"a union of {', '.join(sorted(kinds))} cannot be mapped"
            ), nullable
        return self._single(node, where), False

    def _single(self, node: dict, where: str) -> Shape:
        kind = node["kind"]
        simple = {
            "string": "text",
            "bigint": "bigint",
            "boolean": "boolean",
            "date": "timestamp",
            "day": "date",  # a calendar date with no time (Python's date)
            "decimal": "numeric",  # Python's Decimal (decision P5)
        }
        if kind in simple:
            return Scalar(simple[kind])
        if kind == "number":
            # A Python int is an integer; a JavaScript number, and a Python float, a double.
            return Scalar("bigint" if node.get("integer") else "double")
        if kind in ("any", "unknown", "record"):
            return LOOSE
        if kind == "literal":
            if isinstance(node["value"], str):
                return EnumOf(None, (node["value"],))
            return Scalar("double")
        if kind == "array":
            element, _ = self.shape(node["element"], f"{where}[]")
            return ListOf(element)
        if kind == "ref":
            self._named(node["name"])
            return Named(node["name"])
        if kind == "object":
            return Anonymous(self._fields(node["fields"], where))
        if kind in ("promise", "iterable"):
            return self.fail(where, "a Promise or an iterable is a function's result only")
        text = node.get("text", kind)
        return self.fail(where, f"the type {text} cannot be mapped")

    def _fields(self, fields: list[dict], owner: str) -> tuple[Field, ...]:
        out = []
        for f in fields:
            shape, nullable = self.shape(f["type"], f"{owner}.{f['name']}")
            out.append(Field(f["name"], shape, nullable or f["optional"], f["description"]))
        return tuple(out)

    def _named(self, name: str) -> None:
        if name in self.types or name in self._mapping:
            return
        self._mapping.add(name)
        raw = self.raw_types[name]
        self.types[name] = ObjectType(name, self._fields(raw["fields"], name))
        self._mapping.discard(name)


def _unwrap_promise(node: dict) -> dict:
    return node["value"] if node["kind"] == "promise" else node


def _rows(mapper: _Mapper, export: str, element: dict) -> tuple[Shape, bool]:
    """The row type of a table or stream, and whether its columns come from a preview."""
    if element["kind"] in ("any", "unknown", "record"):
        return LOOSE, True
    shape, _ = mapper.shape(element, f"{export} (row)")
    if not isinstance(shape, (Named, Anonymous)):
        mapper.fail(export, "its rows are not objects; a row is an object whose fields are columns")
    return shape, False


def _columns_export(
    mapper: _Mapper, name: str, entry: dict, parameters: list[Parameter], returns: dict
) -> Export | None:
    """A ``@columns`` function (REQ-2131): each parameter one input column, a list of the batch's
    values, and a list of result rows, one per input row, whose fields are the output columns."""
    ok = True
    for p in parameters:
        if not isinstance(p.shape, ListOf):
            mapper.fail(name, f"@columns parameter {p.name} is one input column: a list of values")
            ok = False
    if not parameters:
        mapper.fail(name, "@columns takes at least one input column")
        ok = False
    result = _unwrap_promise(returns)
    if result["kind"] != "array":
        mapper.fail(name, "@columns returns a list holding one result per input row")
        return None
    element = result["element"]
    if element["kind"] in ("any", "unknown", "record"):
        mapper.fail(name, "@columns declares its output columns: its result is a typed object")
        return None
    row, _ = mapper.shape(element, f"{name} (result)")
    if not isinstance(row, (Named, Anonymous)):
        mapper.fail(name, "@columns returns objects whose fields are its output columns")
        return None
    if not ok:
        return None
    return Export(name, COLUMNS, entry["description"], tuple(parameters), row, many=True)


def read_manifest(raw: dict, rules: Rules = NODE_RULES) -> Manifest:
    """The bundle's tables, commands and streams, from the reader's output, under its language's
    ``rules``; refused with every problem when any rule is broken."""
    mapper = _Mapper(raw.get("types", {}))
    problems = mapper.problems
    for diagnostic in raw.get("diagnostics", []):
        problems.append(f"the declarations do not type-check: {diagnostic}")
    exports: list[Export] = []
    for entry in raw["exports"]:
        name, tags = entry["name"], entry["tags"]
        for tag in tags:
            if tag not in PROVISA_TAGS and tag not in rules.standard_tags:
                problems.append(f"{name}: unknown tag @{tag}")
        kinds = sorted(PROVISA_TAGS.intersection(tags))
        if not kinds:
            continue  # untagged: ignored
        if len(kinds) > 1:
            problems.append(f"{name}: tagged {', '.join('@' + k for k in kinds)}; an export is one")
            continue
        kind = kinds[0]
        signatures = entry["signatures"]
        if len(signatures) != 1:
            problems.append(
                f"{name}: @{kind} needs one function signature, it has {len(signatures)}"
            )
            continue
        (signature,) = signatures
        parameters = []
        for p in signature["parameters"]:
            if p["rest"]:
                problems.append(f"{name}: the rest parameter {p['name']} cannot be a parameter")
                continue
            shape, nullable = mapper.shape(p["type"], f"{name}({p['name']})")
            parameters.append(Parameter(p["name"], shape, nullable, required=not p["optional"]))
        returns = signature["returns"]
        if kind == TABLE:
            result = _unwrap_promise(returns)
            if result["kind"] == "array":
                element = result["element"]
            elif result["kind"] == "iterable":
                element = result["element"]
            else:
                problems.append(f"{name}: @table returns a single value, not a collection of rows")
                continue
            row, preview = _rows(mapper, name, element)
            exports.append(
                Export(name, kind, entry["description"], tuple(parameters), row, preview)
            )
        elif kind == COLUMNS:
            column_transform = _columns_export(mapper, name, entry, parameters, returns)
            if column_transform is not None:
                exports.append(column_transform)
        elif kind == STREAM:
            if not (returns["kind"] == "iterable" and (returns["async"] or rules.sync_streams)):
                problems.append(
                    f"{name}: @stream is a generator or an async generator"
                    if rules.sync_streams
                    else f"{name}: @stream is an async generator (returns AsyncGenerator<T>)"
                )
                continue
            row, preview = _rows(mapper, name, returns["element"])
            exports.append(
                Export(name, kind, entry["description"], tuple(parameters), row, preview)
            )
        else:
            result = _unwrap_promise(returns)
            if result["kind"] == "undefined":
                shape, many = None, False
            elif result["kind"] in ("array", "iterable"):
                shape, _ = mapper.shape(result["element"], f"{name} (result)")
                many = True
            else:
                shape, _ = mapper.shape(result, f"{name} (result)")
                many = False
            exports.append(
                Export(name, kind, entry["description"], tuple(parameters), shape, many=many)
            )
    if problems:
        raise ManifestRefused(problems)
    return Manifest(tuple(exports), mapper.types)
