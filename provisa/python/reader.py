# Copyright (c) 2026 Kenneth Stott
# Canary: 02640725-37e6-4068-9151-7430a5bf5676
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Python source type's reader (REQ-1993): what a module's functions say about themselves.

    <python> reader.py <code directory> <entry module>

Run under the source's own Python, inside the worker's sandbox: a type hint can name a type
from another module or a dependency, so the module is imported to read them, and importing code
is running it. It writes one JSON document, the same every language's reader writes
(docs/arch/code-sources.md, section 2): each public function of the code's own modules, with the
tags of its docstring (a line that is exactly ``@name``), the docstring's other text, and its
parameters' and result's types as trees. Nothing is decided here; which functions are tables,
commands or streams, and what each type maps to, is Provisa's (provisa/codesource/manifest.py).

REQ-2267: the module's one function tagged ``@discover`` is called here, once, and each table,
query and command it generates (provisa_services.Discovered) is written as an entry of its own, as
a hand-written ``@table``, ``@query`` or ``@command`` function's would be, carrying the name of the
handler function that serves it.

Standard library only: the code's environment is the code's, and nothing of Provisa's is on it.
"""

# Requirements: REQ-1993, REQ-2267
from __future__ import annotations

import collections.abc as cabc
import dataclasses
import datetime
import decimal
import enum
import importlib
import importlib.util
import inspect
import json
import os
import re
import sys
import types
import typing

#: REQ-2225: a connector's script, read as the module :data:`_SCRIPT_MODULE`, when this is set.
SCRIPT_FILE_ENV = "PROVISA_SCRIPT_FILE"
_SCRIPT_MODULE = "connector_script"

_TAG = re.compile(r"^@([A-Za-z_][A-Za-z0-9_-]*)$")
#: A tag line with an argument, ``@name argument`` (REQ-2100: ``@loader N``).
_TAG_ARGUMENT = re.compile(r"^@([A-Za-z_][A-Za-z0-9_-]*)\s+(\S+)$")
_NONE = type(None)


def _pydantic_fields(cls: type) -> dict | None:  # name -> pydantic FieldInfo
    """A Pydantic (v2) model's fields, by attribute name; None for any other class. Read from
    the class, so pydantic itself is never imported here: it is the code's, when it has it."""
    fields = getattr(cls, "__pydantic_fields__", None)
    return fields if isinstance(fields, dict) else None


class _Reader:
    def __init__(self) -> None:
        self.types: dict[str, dict] = {}
        self._reading: set[str] = set()

    # -- types ------------------------------------------------------------------------------------

    def tree(self, hint: typing.Any) -> dict:  # a class, a special form or a generic alias
        """``hint`` as a type tree."""
        if hint is inspect.Parameter.empty:
            return {"kind": "unsupported", "text": "no type hint"}
        origin, args = typing.get_origin(hint), typing.get_args(hint)
        if origin is typing.Annotated:
            return self.tree(args[0])
        if hint is _NONE or hint is None:
            return {"kind": "null"}
        if hint is typing.Any:
            return {"kind": "any"}
        if hint is object:
            return {"kind": "unknown"}
        if hint is str:
            return {"kind": "string"}
        if hint is bool:
            return {"kind": "boolean"}
        if hint is int:
            return {"kind": "number", "integer": True}
        if hint is float:
            return {"kind": "number"}
        if hint is decimal.Decimal:
            return {"kind": "decimal"}
        if hint is datetime.datetime:
            return {"kind": "date"}
        if hint is datetime.date:
            return {"kind": "day"}
        if origin in (typing.Union, types.UnionType):
            return {"kind": "union", "members": [self.tree(a) for a in args]}
        if origin is typing.Literal:
            members = [{"kind": "literal", "value": a} for a in args]
            return members[0] if len(members) == 1 else {"kind": "union", "members": members}
        if isinstance(hint, type) and issubclass(hint, enum.Enum):
            values = [m.value for m in hint]
            return {
                "kind": "union",
                "name": hint.__name__,
                "members": [{"kind": "literal", "value": v} for v in values],
            }
        if hint in (dict, cabc.Mapping) or origin in (dict, cabc.Mapping, cabc.MutableMapping):
            return {"kind": "record"}
        if origin in (list, cabc.Sequence, cabc.Collection, set, frozenset, cabc.Set):
            return {"kind": "array", "element": self.tree(args[0] if args else typing.Any)}
        if origin is tuple and len(args) == 2 and args[1] is Ellipsis:
            return {"kind": "array", "element": self.tree(args[0])}
        if hint in (list, set, frozenset, tuple):
            return {"kind": "array", "element": {"kind": "any"}}
        for origins, is_async, generator in (
            ((cabc.Iterable, cabc.Iterator), False, False),
            ((cabc.Generator,), False, True),
            ((cabc.AsyncIterable, cabc.AsyncIterator), True, False),
            ((cabc.AsyncGenerator,), True, True),
        ):
            if origin in origins:
                return {
                    "kind": "iterable",
                    "async": is_async,
                    "generator": generator,
                    "element": self.tree(args[0] if args else typing.Any),
                }
        if getattr(origin, "__module__", "") == "provisa_services" and (
            origin.__name__ == "TableRef"
        ):
            # REQ-2226: a parameter taking a governed table, its row type the columns it declares.
            return {"kind": "table_ref", "row": self.tree(args[0])}
        if isinstance(hint, type) and hint.__module__ == "provisa_services":
            # REQ-2100: Provisa's own types (the ReadContext) are named, never read: theirs are
            # no columns, and their names are no name of the code's.
            return {"kind": "ref", "name": hint.__name__}
        if isinstance(hint, type) and self._is_record_type(hint):
            self._read_type(hint)
            return {"kind": "ref", "name": hint.__name__}
        return {"kind": "unsupported", "text": getattr(hint, "__name__", repr(hint))}

    @staticmethod
    def _is_record_type(kind: type) -> bool:
        """A dataclass, a TypedDict, a NamedTuple, a Pydantic model, or a class with annotated
        fields."""
        if dataclasses.is_dataclass(kind) or typing.is_typeddict(kind):
            return True
        if issubclass(kind, tuple) and hasattr(kind, "_fields"):
            return True
        return bool(getattr(kind, "__annotations__", None)) and kind.__module__ != "builtins"

    def _read_type(self, cls: type) -> None:
        name = cls.__name__
        if name in self.types or name in self._reading:
            return
        self._reading.add(name)
        hints = typing.get_type_hints(cls, include_extras=True)
        model_fields = _pydantic_fields(cls)
        optional_keys = getattr(cls, "__optional_keys__", frozenset())
        fields = []
        for field, hint in hints.items():
            if typing.get_origin(hint) is typing.ClassVar:
                continue
            description = ""
            if model_fields is not None:
                if field not in model_fields:
                    continue  # a private attribute: not one of the model's fields
                info = model_fields[field]
                # A model's row is its dump by alias (host.py), so a column is named as it is sent.
                field = info.serialization_alias or info.alias or field
                description = info.description or ""
            optional = field in optional_keys
            origin = typing.get_origin(hint)
            if origin in (typing.NotRequired, typing.Required):
                optional = origin is typing.NotRequired
                hint = typing.get_args(hint)[0]
            fields.append(
                {
                    "name": field,
                    "optional": optional,
                    # REQ-2100: a field with a default (an input left out takes it).
                    "default": _has_default(cls, field, model_fields),
                    "type": self.tree(hint),
                    "description": description,
                }
            )
        self.types[name] = {"name": name, "fields": fields}
        self._reading.discard(name)

    # -- functions --------------------------------------------------------------------------------

    def function(self, name: str, fn: typing.Callable) -> dict:
        doc = inspect.getdoc(fn) or ""
        tags, text, tag_arguments = [], [], []
        for line in doc.splitlines():
            match = _TAG.match(line.strip())
            argued = _TAG_ARGUMENT.match(line.strip())
            if match:
                tags.append(match.group(1))
            elif argued:
                tag_arguments.append({"tag": argued.group(1), "text": argued.group(2)})
            else:
                text.append(line)
        hints = typing.get_type_hints(fn, include_extras=True)
        parameters = []
        for p in inspect.signature(fn).parameters.values():
            rest = p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
            hint = hints.get(p.name, inspect.Parameter.empty)
            parameters.append(
                {
                    "name": p.name,
                    "optional": p.default is not p.empty,
                    "rest": rest,
                    "type": self.tree(hint),
                }
            )
        returns = self.tree(hints.get("return", inspect.Parameter.empty))
        if inspect.iscoroutinefunction(fn):
            returns = {"kind": "promise", "value": returns}
        return {
            "name": name,
            "tags": tags,
            "tag_arguments": tag_arguments,
            "description": "\n".join(text).strip(),
            "signatures": [{"parameters": parameters, "returns": returns}],
        }


def _has_default(cls: type, field: str, model_fields: dict | None) -> bool:
    """Whether ``cls`` gives its field ``field`` a default: a dataclass's, a NamedTuple's, a
    Pydantic model's (one not required), or a class attribute."""
    if model_fields is not None:
        info = next((i for n, i in model_fields.items() if n == field or i.alias == field), None)
        return info is not None and not info.is_required()
    if dataclasses.is_dataclass(cls):
        found = {f.name: f for f in dataclasses.fields(cls)}.get(field)
        return found is not None and (
            found.default is not dataclasses.MISSING
            or found.default_factory is not dataclasses.MISSING
        )
    defaults = getattr(cls, "_field_defaults", None)
    if isinstance(defaults, dict):
        return field in defaults
    return field in vars(cls)


def functions(module: types.ModuleType) -> list[tuple[str, typing.Callable]]:
    """The module's public functions that are the code's own (defined in the entry module or a
    module under it, not imported from elsewhere): what ``__all__`` names, else every public
    name, in the order the module defines them."""
    package = module.__name__.split(".")[0]
    names = getattr(module, "__all__", None) or [n for n in vars(module) if not n.startswith("_")]
    out = []
    for name in names:
        fn = getattr(module, name, None)
        if inspect.isfunction(fn) and (fn.__module__ or "").split(".")[0] == package:
            out.append((name, fn))
    return out


def _load_entry(entry: str) -> types.ModuleType:
    """The module to read: the connector's script (REQ-2225) when one is named, else ``entry``."""
    script = os.environ.get(SCRIPT_FILE_ENV)
    if not script:
        return importlib.import_module(entry)
    spec = importlib.util.spec_from_file_location(_SCRIPT_MODULE, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[_SCRIPT_MODULE] = module
    spec.loader.exec_module(module)
    return module


#: REQ-2267: the tag of the function whose result is the code's generated items.
_DISCOVER = "discover"
_CONTEXT_PARAMETER = {
    "name": "context",
    "optional": False,
    "rest": False,
    "type": {"kind": "ref", "name": "ReadContext"},
}


class _Discovery:
    """REQ-2267: what one ``@discover`` call generated, as the reader's entries."""

    def __init__(self, reader: _Reader, module: types.ModuleType, function: str) -> None:
        self.reader = reader
        self.module = module
        self.function = function
        self.problems: list[str] = []

    def _handler(self, kind: str, fn: typing.Any) -> str | None:
        """The name of the module's function ``fn`` that serves every generated item of ``kind``."""
        field = f"{kind}_handler"
        if fn is None:
            self.problems.append(f"{self.function}: it generates {kind}s and names no {field}")
            return None
        name = getattr(fn, "__name__", "")
        if not inspect.isfunction(fn) or getattr(self.module, name, None) is not fn:
            self.problems.append(
                f"{self.function}: its {field} is no function of the code's own module"
            )
            return None
        return name

    def _fields(self, item: str, kind: type | None, what: str) -> list[dict] | None:
        """The fields of ``kind``, a class whose fields are the item's inputs or parameters."""
        if kind is None:
            return []
        tree = self.reader.tree(kind)
        if tree["kind"] != "ref" or tree["name"] not in self.reader.types:
            self.problems.append(f"{item}: its {what} type is a class whose fields are its {what}")
            return None
        return self.reader.types[tree["name"]]["fields"]

    def _parameters(self, item: str, kind: type | None) -> list[dict] | None:
        fields = self._fields(item, kind, "parameters")
        if fields is None:
            return None
        return [
            {
                "name": f["name"],
                "optional": f["optional"] or f["default"],
                "rest": False,
                "type": f["type"],
            }
            for f in fields
        ]

    @staticmethod
    def _entry(name: str, tags: list[str], arguments: list[dict], description: str) -> dict:
        return {"name": name, "tags": tags, "tag_arguments": arguments, "description": description}

    def entries(self, discovered: typing.Any, hand_written: list[dict]) -> list[dict]:
        """The generated items of ``discovered`` (a provisa_services.Discovered), each an entry;
        a query's or command's ``table`` is one of them or one of ``hand_written``."""
        if type(discovered).__name__ != "Discovered":
            self.problems.append(
                f"{self.function}: @discover returns a provisa_services.Discovered"
            )
            return []
        rows: dict[str, dict] = {}
        for entry in hand_written:
            if "table" in entry["tags"] and len(entry["signatures"]) == 1:
                returns = entry["signatures"][0]["returns"]
                returns = returns["value"] if returns["kind"] == "promise" else returns
                if returns["kind"] in ("array", "iterable"):
                    rows[entry["name"]] = returns["element"]
        out: list[dict] = []
        tables = list(discovered.tables)
        if tables:
            handler = self._handler("table", discovered.table_handler)
            for table in tables:
                rows[table.name] = self.reader.tree(table.row)
                inputs = self._fields(table.name, table.inputs, "inputs")
                if handler is None or inputs is None:
                    continue
                entry = self._entry(table.name, ["table"], [], table.description)
                if table.loader_batch is not None:
                    entry["tag_arguments"].append(
                        {"tag": "loader", "text": str(table.loader_batch)}
                    )
                elif table.loader:
                    entry["tags"].append("loader")
                inputs_type = {"kind": "object", "fields": inputs}
                entry["signatures"] = [
                    {
                        "parameters": [
                            {
                                "name": "inputs",
                                "optional": False,
                                "rest": False,
                                "type": inputs_type,
                            },
                            _CONTEXT_PARAMETER,
                        ],
                        "returns": {"kind": "array", "element": rows[table.name]},
                    }
                ]
                entry["handler"] = handler
                out.append(entry)
        out.extend(self._commands("query", discovered.queries, discovered.query_handler, rows))
        out.extend(self._commands("command", discovered.commands, discovered.command_handler, rows))
        return out

    def _commands(
        self, kind: str, items: typing.Sequence, fn: typing.Any, rows: dict[str, dict]
    ) -> list[dict]:
        items = list(items)
        if not items:
            return []
        handler = self._handler(kind, fn)
        out = []
        for item in items:
            parameters = self._parameters(item.name, item.parameters)
            table = item.table
            if table is not None and table not in rows:
                self.problems.append(f"{item.name}: its table {table} is no table of the code's")
                continue
            if handler is None or parameters is None:
                continue
            if table is not None:
                returns = {"kind": "array", "element": rows[table]}
            elif getattr(item, "returns", None) is not None:
                returns = self.reader.tree(item.returns)
            else:
                returns = {"kind": "undefined"}
            entry = self._entry(item.name, [kind], [], item.description)
            entry["signatures"] = [{"parameters": parameters, "returns": returns}]
            entry["handler"] = handler
            out.append(entry)
        return out


def _discover(reader: _Reader, module: types.ModuleType, exports: list[dict]) -> tuple[list, list]:
    """REQ-2267: the module's ``@discover`` function called, and the entries of what it generated,
    with any problem by name. Two such functions are called neither: the manifest refuses them."""
    tagged = [e["name"] for e in exports if _DISCOVER in e["tags"]]
    if len(tagged) != 1:
        return [], []
    (function,) = tagged
    discovery = _Discovery(reader, module, function)
    try:
        discovered = getattr(module, function)()
    except Exception as exc:  # noqa: BLE001 -- the code's own failure, reported by name
        return [], [f"{function}: @discover failed: {type(exc).__name__}: {exc}"]
    try:
        entries = discovery.entries(discovered, exports)
    except Exception as exc:  # noqa: BLE001 -- a generated type that cannot be read, by name
        return [], [f"{function}: what it generated cannot be read: {type(exc).__name__}: {exc}"]
    return entries, discovery.problems


def read(entry: str) -> dict:
    reader = _Reader()
    try:
        module = _load_entry(entry)
    except BaseException as exc:  # noqa: BLE001 -- any failure to import is the code's, reported
        return {
            "exports": [],
            "types": {},
            "diagnostics": [f"importing {entry} failed: {type(exc).__name__}: {exc}"],
        }
    exports, diagnostics = [], []
    for name, fn in functions(module):
        try:
            exports.append(reader.function(name, fn))
        except Exception as exc:  # noqa: BLE001 -- a hint that cannot be resolved, reported by name
            diagnostics.append(
                f"{name}: its type hints cannot be read: {type(exc).__name__}: {exc}"
            )
    generated, problems = _discover(reader, module, exports)
    exports.extend(generated)
    diagnostics.extend(problems)
    return {"exports": exports, "types": reader.types, "diagnostics": diagnostics}


def main(argv: list[str]) -> int:
    code, entry = argv
    sys.path.insert(0, code)
    # REQ-2106: Provisa's services module, always present where the code runs, so code that
    # imports it is read as it runs (services/ beside this reader, readable in its sandbox).
    sys.path.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), "services"))
    # What the code prints while it is imported goes to stderr; the document is stdout's alone.
    out = os.dup(1)
    os.dup2(2, 1)
    document = read(entry)
    with os.fdopen(out, "w") as stdout:
        json.dump(document, stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
