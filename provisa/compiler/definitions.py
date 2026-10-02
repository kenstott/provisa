# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5e9a41-6b27-4f83-a1d9-3e7b2f8c6d10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Nothing is defined through a query protocol.

A table or a view is a model object: it is created in the model (admin pages, admin API or
config), or created in the data source and admitted into the model. A statement that creates,
alters or drops a relation — ``CREATE TABLE``, ``CREATE TABLE … AS SELECT``, ``CREATE VIEW``,
``ALTER``, ``DROP``, an index, a sequence, a schema — is refused wherever a client can send a
statement, with one message.

The refusal is decided in ONE place: the semantic layer of the one pipeline, where a statement
has been parsed and before it is governed or routed (``provisa.pgwire._pipeline``). Every surface
that lowers to SQL passes there, so pgwire, SQL over HTTP, Flight, MCP and whatever Cypher
lowers to are covered by the same check. A surface whose protocol has a definition of its own
that never becomes a SQL statement (a Cypher ``CREATE INDEX``, an Airport ``create_table``
action) raises the same :class:`DefinitionNotAvailable` where it recognises it.

Data writes are not definitions: ``INSERT`` / ``UPDATE`` / ``DELETE`` / ``MERGE`` parse as their
own statement kinds and are not touched here; they are admitted by
``provisa.compiler.write_admission``. ``TRUNCATE`` is refused here with them: it empties a table
whatever the role may see and cannot carry a row filter, so it is not a data write — ``DELETE``
is."""

from __future__ import annotations

import re
from typing import Any

import sqlglot.expressions as exp

_HOW = (
    "is not available here: create it in the model (admin pages, admin API or config), or "
    "create it in the data source and admit it into the model."
)


_USE_DELETE = "is not available here: use DELETE, which is governed."


class NotAvailableHere(ValueError):
    """A statement a surface does not carry at all — refused whatever the role, before anything
    is governed or sent to a source. pgwire answers it 0A000 (feature_not_supported)."""


class DefinitionNotAvailable(NotAvailableHere):
    """A statement that defines, alters or drops a relation, sent through a query protocol —
    or a TRUNCATE, which empties one outside every rule a data write is admitted by."""

    def __init__(self, kind: str) -> None:
        self.kind = kind
        super().__init__(f"{kind} {_USE_DELETE if kind == 'TRUNCATE' else _HOW}")


def definition_kind(tree: Any) -> str | None:
    """The statement kind of ``tree`` when it defines, alters or drops something — ``CREATE
    TABLE``, ``DROP VIEW``, ``ALTER TABLE`` … — else None."""
    if isinstance(tree, exp.TruncateTable):
        # Not a data write: it removes every row whatever the role may see, and cannot carry a
        # row filter. Refused on every surface; DELETE is the governed way to remove rows.
        return "TRUNCATE"
    if isinstance(tree, exp.Create):
        verb = "CREATE"
    elif isinstance(tree, exp.Drop):
        verb = "DROP"
    elif isinstance(tree, exp.Alter):
        verb = "ALTER"
    elif isinstance(tree, exp.Command) and str(tree.this).upper() in ("CREATE", "ALTER", "DROP"):
        # A definition the parser has no node for (``ALTER SEQUENCE q RESTART``) is kept as a
        # raw command: its verb and the word after it still say what it is.
        words = str(tree.expression or "").split()
        return f"{str(tree.this).upper()} {words[0].upper()}" if words else str(tree.this).upper()
    else:
        return None
    return f"{verb} {str(tree.args['kind']).upper()}"


def refuse_definition(tree: Any) -> None:
    """Raise :class:`DefinitionNotAvailable` when ``tree`` is a definition statement."""
    kind = definition_kind(tree)
    if kind is not None:
        raise DefinitionNotAvailable(kind)


# A statement's opening words, for a surface that must tell what language a text is in before it
# can parse it (Flight tickets, Cypher): CREATE / DROP / ALTER followed by a WORD is a definition
# in SQL and in Cypher alike. A Cypher data write is told apart by what follows the verb — CREATE
# is followed by a pattern, ``(``, never a word — and DROP / ALTER write no rows in either.
_LEADING_COMMENTS_RE = re.compile(r"^(?:\s+|--[^\n]*\n?|//[^\n]*\n?|/\*.*?\*/)+", re.DOTALL)
_OPENS_AS_DEFINITION_RE = re.compile(
    r"(?P<verb>CREATE|DROP|ALTER|TRUNCATE)\s+"
    r"(?:(?:OR|REPLACE|TEMP|TEMPORARY|MATERIALIZED|RECURSIVE|GLOBAL|LOCAL|UNLOGGED|UNIQUE|RANGE|"
    r"TEXT|POINT|LOOKUP|FULLTEXT|VECTOR|BTREE|COMPOSITE)\s+)*"
    # ... but not a word that is being assigned to: Cypher's ``CREATE p = (a)-[:R]->(b)`` names a
    # path variable and writes rows.
    r"(?P<object>[A-Za-z_]+)\b(?!\s*=)",
    re.IGNORECASE,
)


def refuse_definition_text(text: str) -> None:
    """Raise :class:`DefinitionNotAvailable` when ``text`` opens as a definition statement.

    For the places a statement is recognised BEFORE it is lowered to SQL or parsed as SQL — the
    Cypher front end, whose index and constraint statements lower to nothing, and Flight's
    language detection. Everything else is refused from its parsed tree (:func:`refuse_definition`)."""
    m = _OPENS_AS_DEFINITION_RE.match(_LEADING_COMMENTS_RE.sub("", text, count=1))
    if m:
        verb = m.group("verb").upper()
        raise DefinitionNotAvailable(
            verb if verb == "TRUNCATE" else f"{verb} {m.group('object').upper()}"
        )


def definition_refusal(exc: BaseException | None) -> NotAvailableHere | None:
    """The refusal ``exc`` is, or was raised from — a surface that wraps pipeline errors in its
    own exception type still answers this one in its own error shape."""
    seen = 0
    while exc is not None and seen < 16:
        if isinstance(exc, NotAvailableHere):
            return exc
        exc = exc.__cause__ or exc.__context__
        seen += 1
    return None
