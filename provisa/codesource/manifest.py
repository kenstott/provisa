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

- an export tagged ``@table`` is a table, ``@command`` a command, ``@stream`` a live table;
  untagged exports are ignored;
- a tag that is neither one of those nor a standard JSDoc tag is a problem named by the export
  and the tag;
- a ``@table`` returns a collection (an array or an iterable, synchronous or asynchronous) whose
  elements are its rows; one that returns a single object is a problem by name;
- a ``@stream`` is an async generator (its declaration returns ``AsyncGenerator<T>`` or
  ``AsyncIterable<T>``), with the same parameter and column rules as ``@table``;
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

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.pushdown import Pushdown, parse, type_problem

TABLE, COMMAND, STREAM = "table", "command", "stream"
PROVISA_TAGS = frozenset({TABLE, COMMAND, STREAM})

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
    #: REQ-2100: the predicate on a return column this parameter is given, if any.
    pushes: Pushdown | None = None


@dataclass(frozen=True)
class Export:
    name: str
    kind: str
    description: str
    parameters: tuple[Parameter, ...]
    #: A table's or stream's row type; a command's result (None: it returns nothing).
    returns: Shape | None
    #: Whether the columns are found by a preview: the row type is loose.
    preview: bool = False
    #: A command's result: rows (a collection) or one value.
    many: bool = False
    #: REQ-2100: a table that takes a ``loader`` parameter, called with a join's keys in batches.
    loader: bool = False
    #: REQ-2100: the largest batch its ``loader`` takes (``loader N``); None, every key in one.
    loader_batch: int | None = None


#: REQ-2100: the parameter a table's function takes a join's keys in, as a hash of sparse keys.
LOADER = "loader"
#: A batch-size declaration: ``loader N``, in a tag line, an inline comment or ``Annotated``.
_LOADER_SIZE = re.compile(r"^loader\s+(\S+)$")


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
    return {"name": field.name, "shape": _shape_json(field.shape), "nullable": field.nullable}


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
    return Field(raw["name"], shape, raw["nullable"])


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
                        "pushes": None
                        if p.pushes is None
                        else {"column": p.pushes.column, "op": p.pushes.op},
                    }
                    for p in e.parameters
                ],
                "returns": _shape_json(e.returns),
                "preview": e.preview,
                "many": e.many,
                "loader": e.loader,
                "loader_batch": e.loader_batch,
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
            pushes = p["pushes"] and Pushdown(p["pushes"]["column"], p["pushes"]["op"])
            parameters.append(
                Parameter(p["name"], shape, p["nullable"], p["required"], pushes or None)
            )
        exports.append(
            Export(
                e["name"],
                e["kind"],
                e["description"],
                tuple(parameters),
                _shape(e["returns"]),
                e["preview"],
                e["many"],
                e["loader"],
                e["loader_batch"],
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
            out.append(Field(f["name"], shape, nullable or f["optional"]))
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


def ir_of(shape: Shape) -> str:
    """The IR type of a value of ``shape``: a scalar's own, an enum's text, a list's ``list``, and
    an object's json."""
    if isinstance(shape, Scalar):
        return shape.ir
    if isinstance(shape, EnumOf):
        return "text"
    if isinstance(shape, ListOf):
        return "list"
    return "json"


def _declared_pushdown(entry: dict, signature: dict, rules: Rules) -> dict[str, list[str]]:
    """Each parameter's explicit pushdown declarations (REQ-2100), by parameter: the tag lines
    naming it, then its inline comment or ``Annotated`` strings, as the reader wrote them."""
    declared: dict[str, list[str]] = {
        p["name"]: [d for d in p.get("pushdown", []) if not _LOADER_SIZE.match(d)]
        for p in signature["parameters"]
    }
    for tag, parameter in _pushdown_tags(entry, rules):
        declared.setdefault(parameter, []).insert(0, tag)
    return declared


_IDENTIFIER = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")


def _loader_sizes(entry: dict, signature: dict) -> list[str]:
    """REQ-2100: the batch sizes the export declares, as written: ``@loader N`` tag lines, and
    ``loader N`` on its parameters (an inline comment or an ``Annotated`` string)."""
    sizes = [t["text"] for t in entry.get("tag_arguments", []) if t["tag"] == LOADER]
    for p in signature["parameters"]:
        for declared in p.get("pushdown", []):
            match = _LOADER_SIZE.match(declared)
            if match:
                sizes.append(match.group(1))
    return sizes


def _loader(
    mapper: _Mapper, name: str, entry: dict, signature: dict, parameters: list[Parameter]
) -> tuple[bool, int | None]:
    """REQ-2100: whether the table takes a ``loader`` and the largest batch it declares; a size it
    cannot take is a problem by name."""
    has_loader = any(p.name == LOADER for p in parameters)
    sizes = _loader_sizes(entry, signature)
    if not sizes:
        return has_loader, None
    if not has_loader:
        mapper.problems.append(
            f"{name}: loader {sizes[0]} declares a batch size, and {name} has no loader parameter"
        )
        return False, None
    if len(sizes) > 1:
        mapper.problems.append(f"{name}: two batch sizes declared, {sizes[0]} and {sizes[1]}")
        return True, None
    if not sizes[0].isdigit() or int(sizes[0]) < 1:
        mapper.problems.append(
            f"{name}: loader {sizes[0]}: a batch size is a positive whole number"
        )
        return True, None
    return True, int(sizes[0])


def _pushdown_tags(entry: dict, rules: Rules) -> list[tuple[str, str]]:
    """REQ-2100: the export's pushdown tag lines, ``(tag, parameter)``: a tag that is neither
    Provisa's nor one of the language's own, whose argument is one identifier."""
    return [
        (t["tag"], t["text"])
        for t in entry.get("tag_arguments", [])
        if t["tag"] not in PROVISA_TAGS
        and t["tag"] not in rules.standard_tags
        and _IDENTIFIER.match(t["text"])
    ]


def _resolve_pushdown(
    mapper: _Mapper,
    name: str,
    parameters: list[Parameter],
    declared: dict[str, list[str]],
    row: Shape,
    preview: bool,
) -> list[Parameter]:
    """``parameters`` with the predicate each is given (REQ-2100), every problem collected."""
    problems = mapper.problems
    known = {p.name for p in parameters}
    for parameter in sorted(p for p, d in declared.items() if d and p not in known):
        problems.append(
            f"{name}: @{declared[parameter][0]} names {parameter}, which is not a parameter"
        )
    explicit = {p: d for p, d in declared.items() if d and p in known}
    if preview:
        for parameter in explicit:
            problems.append(
                f"{name}({parameter}): its rows' columns are found by a preview, so no predicate "
                "can be pushed to it"
            )
        return parameters
    fields = (
        mapper.types[row.name].fields
        if isinstance(row, Named) and row.name in mapper.types
        else row.fields
        if isinstance(row, Anonymous)
        else ()
    )
    columns = {f.name: f for f in fields}
    names = frozenset(columns)
    out: list[Parameter] = []
    taken: dict[Pushdown, str] = {}
    for p in parameters:
        spellings = explicit.get(p.name, [])
        if len(spellings) > 1:
            problems.append(f"{name}({p.name}): declared to take two predicates, {spellings}")
            out.append(p)
            continue
        if spellings:
            pushes = parse(spellings[0], names)
            if pushes is None:
                problems.append(
                    f"{name}({p.name}): {spellings[0]} names no return column of {name}"
                )
                out.append(p)
                continue
        else:
            pushes = parse(p.name, names)
            if pushes is None:
                out.append(p)
                continue
        column = columns[pushes.column]
        element = ir_of(p.shape.element) if isinstance(p.shape, ListOf) else None
        problem = type_problem(pushes.op, ir_of(column.shape), ir_of(p.shape), element)
        if problem:
            # Only a declaration is refused: a name that merely looks like one is a parameter.
            if spellings:
                problems.append(f"{name}({p.name}): {problem}")
            out.append(p)
            continue
        if pushes in taken:
            problems.append(
                f"{name}: {pushes.op} on {pushes.column} is pushed to both {taken[pushes]} and "
                f"{p.name}"
            )
            out.append(p)
            continue
        taken[pushes] = p.name
        out.append(Parameter(p.name, p.shape, p.nullable, p.required, pushes))
    return out


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
        pushdown_tags = {tag for tag, _ in _pushdown_tags(entry, rules)} | {LOADER}
        for tag in tags:
            if tag not in PROVISA_TAGS and tag not in rules.standard_tags | pushdown_tags:
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
            loader, batch = _loader(mapper, name, entry, signature, parameters)
            keys = [p for p in parameters if p.name != LOADER]
            keys = _resolve_pushdown(
                mapper, name, keys, _declared_pushdown(entry, signature, rules), row, preview
            )
            # The loader keeps its place among the parameters (the function's own order).
            resolved: dict[str, Parameter] = {k.name: k for k in keys}
            ordered: list[Parameter] = [
                resolved[p.name] if p.name in resolved else p for p in parameters
            ]
            exports.append(
                Export(
                    name,
                    kind,
                    entry["description"],
                    tuple(ordered),
                    row,
                    preview,
                    loader=loader,
                    loader_batch=batch,
                )
            )
        elif kind == STREAM:
            if not (returns["kind"] == "iterable" and (returns["async"] or rules.sync_streams)):
                problems.append(
                    f"{name}: @stream is a generator or an async generator"
                    if rules.sync_streams
                    else f"{name}: @stream is an async generator (returns AsyncGenerator<T>)"
                )
                continue
            row, preview = _rows(mapper, name, returns["element"])
            parameters = _resolve_pushdown(
                mapper, name, parameters, _declared_pushdown(entry, signature, rules), row, preview
            )
            exports.append(
                Export(name, kind, entry["description"], tuple(parameters), row, preview)
            )
        else:
            for parameter, spellings in _declared_pushdown(entry, signature, rules).items():
                if spellings:
                    problems.append(
                        f"{name}({parameter}): @{spellings[0]} pushes a predicate, and a "
                        "@command has no columns to filter"
                    )
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
