# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c6b8e-5a2d-4c1e-9b7a-2d8e4f6a1c90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: the shape of a DDL source's text -- what it can create in an org's schema, without
what cannot change that: SQL comments and the layout of SQL outside its quoted text, and a Python
module's comments, docstrings and formatting. :func:`provisa.core.schema_org.ddl_fingerprint`
hashes this, so a demo template is kept across a cloud patch that changed none of it."""

from __future__ import annotations

import ast

_QUOTES = ("'", '"')


def _dollar_tag(text: str, i: int) -> str | None:
    """The PostgreSQL dollar-quote tag (``$$`` or ``$name$``) opening at ``text[i]``, else None."""
    end = text.find("$", i + 1)
    if end < 0:
        return None
    tag = text[i : end + 1]
    inner = tag[1:-1]
    if inner and not (inner[0].isalpha() or inner[0] == "_"):
        return None
    if not all(c.isalnum() or c == "_" for c in inner):
        return None
    return tag


def _quoted_end(text: str, i: int) -> int:
    """The index past the quoted text (``'...'`` or ``"..."``, a doubled quote inside) opening at
    ``text[i]``; the end of ``text`` when it is not closed there."""
    quote = text[i]
    j = i + 1
    while j < len(text):
        if text[j] == quote:
            if j + 1 < len(text) and text[j + 1] == quote:
                j += 2
                continue
            return j + 1
        j += 1
    return len(text)


def sql_shape(text: str) -> str:
    """``text`` with its SQL comments (``--`` to the line's end, ``/* */``) dropped and each run of
    whitespace outside quoted text made one space: what PostgreSQL reads of it. Quoted strings,
    quoted identifiers and dollar-quoted bodies are kept as written."""
    out: list[str] = []
    space = False
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c in _QUOTES:
            end = _quoted_end(text, i)
            piece = text[i:end]
        elif c == "$" and (tag := _dollar_tag(text, i)) is not None:
            close = text.find(tag, i + len(tag))
            end = n if close < 0 else close + len(tag)
            piece = text[i:end]
        elif text.startswith("--", i):
            newline = text.find("\n", i)
            i = n if newline < 0 else newline
            space = True
            continue
        elif text.startswith("/*", i):
            close = text.find("*/", i + 2)
            i = n if close < 0 else close + 2
            space = True
            continue
        elif c.isspace():
            space = True
            i += 1
            continue
        else:
            end = i + 1
            piece = c
        if space and out:
            out.append(" ")
        space = False
        out.append(piece)
        i = end
    return "".join(out)


def _without_docstrings(tree: ast.AST) -> ast.AST:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
    return tree


def python_shape(source: bytes) -> str:
    """A Python module's code: its syntax tree without comments, docstrings or layout, each string
    in it read as SQL (:func:`sql_shape`), since the module's DDL is written in its strings."""
    tree = _without_docstrings(ast.parse(source))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            node.value = sql_shape(node.value)
    return ast.dump(tree)


def source_shape(path: str, source: bytes) -> bytes:
    """The shape of DDL source ``path`` (a ``.sql`` script or a ``.py`` module)."""
    if path.endswith(".sql"):
        return sql_shape(source.decode()).encode()
    if path.endswith(".py"):
        return python_shape(source).encode()
    raise ValueError(f"a DDL source is a .sql script or a .py module: {path}")
