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

Standard library only: the code's environment is the code's, and nothing of Provisa's is on it.
"""

# Requirements: REQ-1993
from __future__ import annotations

import collections.abc as cabc
import dataclasses
import datetime
import decimal
import enum
import importlib
import inspect
import json
import os
import re
import sys
import types
import typing

_TAG = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)$")
#: REQ-2100: a pushdown tag line, ``@{op}_{column} parameter``.
_PUSHDOWN_TAG = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)\s+(\S+)$")
_NONE = type(None)


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
        if isinstance(hint, type) and self._is_record_type(hint):
            self._read_type(hint)
            return {"kind": "ref", "name": hint.__name__}
        return {"kind": "unsupported", "text": getattr(hint, "__name__", repr(hint))}

    @staticmethod
    def _is_record_type(kind: type) -> bool:
        """A dataclass, a TypedDict, a NamedTuple, or a class with annotated fields."""
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
        optional_keys = getattr(cls, "__optional_keys__", frozenset())
        fields = []
        for field, hint in hints.items():
            if typing.get_origin(hint) is typing.ClassVar:
                continue
            optional = field in optional_keys
            origin = typing.get_origin(hint)
            if origin in (typing.NotRequired, typing.Required):
                optional = origin is typing.NotRequired
                hint = typing.get_args(hint)[0]
            fields.append({"name": field, "optional": optional, "type": self.tree(hint)})
        self.types[name] = {"name": name, "fields": fields}
        self._reading.discard(name)

    # -- functions --------------------------------------------------------------------------------

    def function(self, name: str, fn: typing.Callable) -> dict:
        doc = inspect.getdoc(fn) or ""
        tags, text, tag_arguments = [], [], []
        for line in doc.splitlines():
            match = _TAG.match(line.strip())
            pushdown = _PUSHDOWN_TAG.match(line.strip())
            if match:
                tags.append(match.group(1))
            elif pushdown:
                tag_arguments.append({"tag": pushdown.group(1), "text": pushdown.group(2)})
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
                    "pushdown": _annotated_strings(hint),
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


def _annotated_strings(hint: typing.Any) -> list[str]:
    """REQ-2100: the strings a ``typing.Annotated`` hint carries, each a pushdown declaration."""
    if typing.get_origin(hint) is not typing.Annotated:
        return []
    return [m for m in typing.get_args(hint)[1:] if isinstance(m, str)]


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


def read(entry: str) -> dict:
    reader = _Reader()
    try:
        module = importlib.import_module(entry)
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
