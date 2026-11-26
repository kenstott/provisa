# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c2a8e-5b1d-4e7a-9c62-8d4f1b7e2a95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A JSON column's template fake (REQ-2279), as the operator writes it::

    {"email": email(), "phone": phone_number(), "age": uniform(min=18, max=90)::integer,
     "address": {"city": city(), "postcode": postcode()}, "tags": [word(), word()],
     "kind": "contact"}

The declaration is the object itself: each field is a fake as a column declares one, a nested
object, a fixed-length array of fields, or a JSON literal (a double-quoted string, a number,
true, false or null) for a field that does not vary. A fake may name the type its value is
written as (``::integer``); without one it is the type the fake makes. The checks each field's
fake passes against that type are :func:`provisa.fakes.checks.check_template`; the read that
builds the object, the same text on every engine, is :mod:`provisa.fakes.read_sql`.
"""

# Requirements: REQ-2279

from __future__ import annotations

import datetime as _dt
import decimal
import json
import re
from collections.abc import Iterator

from provisa.fakes.kinds import (
    Bool,
    Categories,
    FakeKind,
    FakeRefused,
    Hash,
    LogNormal,
    Method,
    Normal,
    Percentiles,
    Poisson,
    Template,
    TemplateArray,
    TemplateField,
    TemplateLiteral,
    TemplateNode,
    TemplateObject,
    Triangular,
    Uniform,
    kind_name,
    parse,
)

#: The types a field's value is written as, by the family it holds
#: (provisa.fakes.checks.family): text and dates as JSON strings, numbers and booleans as JSON
#: numbers and booleans.
FIELD_TYPES = {
    "text": "varchar",
    "integer": "bigint",
    "numeric": "double",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TYPE = re.compile(r"[A-Za-z_][A-Za-z0-9_ ]*(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?")
_NUMBER = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][-+]?\d+)?")


class _Reader:
    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def space(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def peek(self) -> str:
        self.space()
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def take(self, ch: str) -> None:
        if self.peek() != ch:
            found = self.text[self.pos : self.pos + 12] or "the end"
            raise FakeRefused(f"expected {ch!r} at {found!r}")
        self.pos += 1

    def string(self) -> str:
        """A JSON string at the reader."""
        self.space()
        try:
            value, end = json.JSONDecoder().raw_decode(self.text, self.pos)
        except json.JSONDecodeError as exc:
            raise FakeRefused(f"cannot read the string at {self.text[self.pos :][:12]!r}") from exc
        if not isinstance(value, str):
            raise FakeRefused(f"expected a double-quoted name at {self.text[self.pos :][:12]!r}")
        self.pos = end
        return value

    def call(self) -> str:
        """The text of one fake call, ``name(...)``, to its matching parenthesis."""
        self.space()
        start = self.pos
        m = _IDENT.match(self.text, self.pos)
        if m is None:
            raise FakeRefused(f"cannot read {self.text[start:][:12]!r}")
        self.pos = m.end()
        self.space()
        if self.peek() != "(":
            raise FakeRefused(
                f"{m.group(0)!r} is not a field: write a fake as {m.group(0)}(), or a literal "
                f"in double quotes"
            )
        depth, quote = 0, ""
        while self.pos < len(self.text):
            ch = self.text[self.pos]
            self.pos += 1
            if quote:
                if ch == quote:
                    quote = ""
            elif ch in "'\"":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return self.text[start : self.pos]
        raise FakeRefused(f"{self.text[start:][:24]!r} has no closing parenthesis")

    def cast(self) -> str | None:
        """A field's ``::type``, or None."""
        self.space()
        if not self.text.startswith("::", self.pos):
            return None
        self.pos += 2
        self.space()
        m = _TYPE.match(self.text, self.pos)
        if m is None:
            raise FakeRefused(f"expected a type after '::' at {self.text[self.pos :][:12]!r}")
        self.pos = m.end()
        return " ".join(m.group(0).split())


def parse_template(declaration: str) -> Template:
    """The template a declaration writes, refusing by name what cannot describe an object."""
    r = _Reader(declaration)
    try:
        root = _object(r, "")
        if r.peek():
            raise FakeRefused("has text after its closing brace")
    except FakeRefused as exc:
        raise FakeRefused(f"the JSON template: {exc}") from exc
    return Template(root)


def _path(prefix: str, step: str) -> str:
    return f"{prefix}.{step}" if prefix else step


def _object(r: _Reader, prefix: str) -> TemplateObject:
    r.take("{")
    fields: list[tuple[str, TemplateNode]] = []
    if r.peek() == "}":
        r.take("}")
        return TemplateObject(())
    while True:
        key = r.string()
        if any(k == key for k, _ in fields):
            raise FakeRefused(f"names the field {_path(prefix, key)!r} twice")
        r.take(":")
        fields.append((key, _node(r, _path(prefix, key))))
        if r.peek() == ",":
            r.take(",")
            continue
        r.take("}")
        return TemplateObject(tuple(fields))


def _array(r: _Reader, prefix: str) -> TemplateArray:
    r.take("[")
    items: list[TemplateNode] = []
    if r.peek() == "]":
        r.take("]")
        return TemplateArray(())
    while True:
        items.append(_node(r, f"{prefix}[{len(items)}]"))
        if r.peek() == ",":
            r.take(",")
            continue
        r.take("]")
        return TemplateArray(tuple(items))


def _node(r: _Reader, path: str) -> TemplateNode:
    ch = r.peek()
    if ch == "{":
        return _object(r, path)
    if ch == "[":
        return _array(r, path)
    if ch == '"':
        return TemplateLiteral(r.string())
    m = _NUMBER.match(r.text, r.pos)
    if m is not None:
        r.pos = m.end()
        text = m.group(0)
        return TemplateLiteral(json.loads(text))
    for word, value in (("true", True), ("false", False), ("null", None)):
        if r.text.startswith(word, r.pos) and not _IDENT.match(r.text, r.pos + len(word)):
            r.pos += len(word)
            return TemplateLiteral(value)
    call = r.call()
    data_type = r.cast()
    try:
        kind = parse(call)
        return TemplateField(kind, data_type if data_type is not None else made_type(kind))
    except FakeRefused as exc:
        raise FakeRefused(f"field {path!r}: {exc}") from exc


def made_type(kind: FakeKind) -> str:
    """The type a field's fake makes, when the field names none: what a method's value is, a
    distribution's number (a timestamp over dates and times), poisson's integer, bool's boolean,
    the text of a category or a hash. A kind no field may declare is refused by
    provisa.fakes.checks.check_template, named there."""
    if isinstance(kind, Method):
        from provisa.fakes.methods import column_value, sample_of

        sample = column_value(kind.name, sample_of(kind))
        if isinstance(sample, bool):
            return FIELD_TYPES["boolean"]
        if isinstance(sample, int):
            return FIELD_TYPES["integer"]
        if isinstance(sample, (float, decimal.Decimal)):
            return FIELD_TYPES["numeric"]
        if isinstance(sample, _dt.datetime):
            return FIELD_TYPES["timestamp"]
        if isinstance(sample, _dt.date):
            return FIELD_TYPES["date"]
        return FIELD_TYPES["text"]
    if isinstance(kind, (Uniform, Normal, LogNormal, Triangular, Percentiles)):
        return FIELD_TYPES["timestamp" if kind.temporal else "numeric"]
    if isinstance(kind, Poisson):
        return FIELD_TYPES["integer"]
    if isinstance(kind, Bool):
        return FIELD_TYPES["boolean"]
    if isinstance(kind, (Categories, Hash)):
        return FIELD_TYPES["text"]
    # Checked by name in check_template: the field's type does not matter for a refused kind.
    return kind_name(kind)


def fields(node: TemplateNode, prefix: str = "") -> Iterator[tuple[str, TemplateField]]:
    """Every field of a template computed by a fake, with its path (``address.city``,
    ``tags[0]``), in the order declared."""
    if isinstance(node, TemplateField):
        yield prefix, node
    elif isinstance(node, TemplateObject):
        for key, child in node.fields:
            yield from fields(child, _path(prefix, key))
    elif isinstance(node, TemplateArray):
        for i, child in enumerate(node.items):
            yield from fields(child, f"{prefix}[{i}]")


def is_json(data_type: str) -> bool:
    """Whether a column of ``data_type`` holds JSON: what a template fake fakes."""
    return data_type.lower().split("(")[0].strip() in ("json", "jsonb")


__all__ = ["FIELD_TYPES", "fields", "is_json", "made_type", "parse_template"]
