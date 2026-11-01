# Copyright (c) 2026 Kenneth Stott
# Canary: a16f39d4-7e02-48c5-9b3d-20c7f5e8d1a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's default text search parser and its english and simple configurations (REQ-2171), as
wparser_def.c reads a document and to_tsany.c builds a value of it.

wparser_def.c is a state machine over two hundred states. :func:`tokens` reads the shapes below,
each as the recording of postgres:17.11 tokenizes it, and answers :class:`Unread` for a text that
holds anything else: a dot, an at sign, a slash, an ampersand, a hyphen or an angle bracket outside
them, where the machine's reading turns on more than these shapes say."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from provisa.core import pg_tsearch
from provisa.core.pg_error import PostgresError
from provisa.core.pg_tsearch import Node
from provisa.core.pg_tsearch_english import lexize as english

ASCIIWORD, WORD, NUMWORD, EMAIL, URL, HOST, SFLOAT = 1, 2, 3, 4, 5, 6, 7
HWORD_ASCIIPART, BLANK, TAG, PROTOCOL = 11, 12, 13, 14
ASCIIHWORD, URL_PATH, FILE, FLOAT, INT, UINT = 16, 18, 19, 20, 21, 22
TYPES = {
    ASCIIWORD: ("asciiword", "Word, all ASCII"),
    WORD: ("word", "Word, all letters"),
    NUMWORD: ("numword", "Word, letters and digits"),
    EMAIL: ("email", "Email address"),
    URL: ("url", "URL"),
    HOST: ("host", "Host"),
    SFLOAT: ("sfloat", "Scientific notation"),
    HWORD_ASCIIPART: ("hword_asciipart", "Hyphenated word part, all ASCII"),
    BLANK: ("blank", "Space symbols"),
    TAG: ("tag", "XML tag"),
    PROTOCOL: ("protocol", "Protocol head"),
    ASCIIHWORD: ("asciihword", "Hyphenated word, all ASCII"),
    URL_PATH: ("url_path", "URL path"),
    FILE: ("file", "File or path name"),
    FLOAT: ("float", "Decimal notation"),
    INT: ("int", "Signed integer"),
    UINT: ("uint", "Unsigned integer"),
}
_NOT_INDEXED = frozenset({BLANK, TAG, PROTOCOL})
_STEMMED = frozenset({ASCIIWORD, WORD, ASCIIHWORD, HWORD_ASCIIPART})
_DOMAIN = r"[A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z][A-Za-z0-9]*)*\.[A-Za-z]{2,}"
_BOUNDARY = r"(?![\w@./<&-])"
_SHAPES: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"</?[A-Za-z][A-Za-z0-9]*>"), TAG),
    (re.compile(rf"[A-Za-z][A-Za-z0-9]*@{_DOMAIN}{_BOUNDARY}"), EMAIL),
    (re.compile(rf"https?://{_DOMAIN}(?:/[A-Za-z0-9._-]+)+/?{_BOUNDARY}"), URL),
    (re.compile(rf"(?:/[A-Za-z0-9_-]+)+{_BOUNDARY}"), FILE),
    (re.compile(rf"{_DOMAIN}{_BOUNDARY}"), HOST),
    (re.compile(rf"[A-Za-z]+(?:-[A-Za-z]+)+{_BOUNDARY}"), ASCIIHWORD),
    (re.compile(rf"[0-9]+[eE][0-9]+{_BOUNDARY}"), SFLOAT),
    (re.compile(rf"-?[0-9]+\.[0-9]+{_BOUNDARY}"), FLOAT),
    (re.compile(rf"-[0-9]+{_BOUNDARY}"), INT),
    (re.compile(rf"[0-9]+{_BOUNDARY}"), UINT),
    (re.compile(rf"[A-Za-z]+[0-9][A-Za-z0-9]*{_BOUNDARY}"), NUMWORD),
    (re.compile(rf"[A-Za-z]+{_BOUNDARY}"), ASCIIWORD),
    (re.compile(rf"[^\W\d_]+{_BOUNDARY}"), WORD),
)
#: What is between tokens, and can open none.
_BLANK = re.compile(r"[\s,;:!?'\"()\[\]{}=+*~^%$#|`]+")


class Unread(ValueError):
    """A text with a shape :func:`tokens` does not say the machine's reading of."""


@dataclass(frozen=True)
class Token:
    kind: int
    text: str


def _unpacked(kind: int, text: str) -> list[Token]:
    """What the parser adds after a compound token."""
    if kind == ASCIIHWORD:
        parts: list[Token] = [Token(ASCIIHWORD, text)]
        for at, part in enumerate(text.split("-")):
            if at:
                parts.append(Token(BLANK, "-"))
            parts.append(Token(HWORD_ASCIIPART, part))
        return parts
    if kind == URL:
        head, rest = text.split("://", 1)
        host, path = rest.split("/", 1)
        return [
            Token(PROTOCOL, f"{head}://"),
            Token(URL, rest),
            Token(HOST, host),
            Token(URL_PATH, f"/{path}"),
        ]
    return [Token(kind, text)]


def tokens(text: str) -> list[Token]:
    """``text`` as ts_parse answers it."""
    found: list[Token] = []
    at = 0
    while at < len(text):
        for shape, kind in _SHAPES:
            match = shape.match(text, at)
            if match is not None:
                found.extend(_unpacked(kind, match.group()))
                at = match.end()
                break
        else:
            blank = _BLANK.match(text, at)
            if blank is None:
                raise Unread(text)
            found.append(Token(BLANK, blank.group()))
            at = blank.end()
    return found


def lexemes(config: str, token: Token) -> list[str] | None:
    """The token's lexemes: None where the configuration reads it not at all."""
    if token.kind in _NOT_INDEXED:
        return None
    if config == "english" and token.kind in _STEMMED:
        return english(token.text)
    return [token.text.lower()]


def positioned(config: str, text: str) -> list[tuple[int, str]]:
    """Each lexeme of ``text`` with its position: a stop word has one and no lexeme."""
    held: list[tuple[int, str]] = []
    position = 0
    for token in tokens(text):
        found = lexemes(config, token)
        if found is None:
            continue
        position += 1
        held.extend((position, lexeme) for lexeme in found)
    return held


def to_tsvector(config: str, text: str, start: int = 0) -> dict[str, list[tuple[int, int]]]:
    entries: dict[str, list[tuple[int, int]]] = {}
    for position, lexeme in positioned(config, text):
        entries.setdefault(lexeme, []).append((position + start, 0))
    return entries


def _chain(held: list[tuple[int, str]], op: str) -> Node | None:
    """The lexemes joined by ``op``; a phrase holds the distance between each two."""
    query: Node | None = None
    last = 0
    for position, lexeme in held:
        word = Node(lexeme=lexeme)
        query = word if query is None else Node(op, (query, word), position - last)
        last = position
    return query


def plainto_tsquery(config: str, text: str) -> Node | None:
    return _chain(positioned(config, text), "&")


def phraseto_tsquery(config: str, text: str) -> Node | None:
    return _chain(positioned(config, text), "<->")


def to_tsquery(config: str, text: str) -> Node | None:
    """``text`` as a query, each operand read through ``config``, stop words left out."""
    return _morphed(config, pg_tsearch.tsquery_in(text))


def _morphed(config: str, node: Node | None) -> Node | None:
    if node is None:
        return None
    if node.op == "!":
        inner = _morphed(config, node.children[0])
        return None if inner is None else Node("!", (inner,))
    if node.op:
        left, right = (_morphed(config, c) for c in node.children)
        return (
            pg_tsearch.joined(node.op, left, right, node.distance)
            if left is not None or right is not None
            else None
        )
    word = phraseto_tsquery(config, node.lexeme)
    if word is None or word.op or not (node.weight or node.prefix):
        return word
    return Node(lexeme=word.lexeme, weight=node.weight, prefix=node.prefix)


_WEB = re.compile(r'"([^"]*)"?|(-)|(\S+)')


def websearch_to_tsquery(config: str, text: str) -> Node | None:
    """websearch's terms: a quoted phrase, ``or``, ``-`` for NOT, anything else a word."""
    query: Node | None = None
    pending_or = False
    negate = False
    for quoted, minus, word in _WEB.findall(text):
        if minus:
            negate = True
            continue
        if word.lower() == "or" and query is not None:
            pending_or = True
            continue
        term = plainto_tsquery(config, word) if word else phraseto_tsquery(config, quoted)
        if negate and term is not None:
            term = Node("!", (term,))
        negate = False
        if term is None:
            continue
        query = pg_tsearch.joined("|" if pending_or else "&", query, term)
        pending_or = False
    return query


def lexize(dictionary: str, text: str) -> list[str]:
    """ts_lexize."""
    if dictionary == "simple":
        return [text.lower()]
    return english(text)


def dictionary_of(config: str, token: Token) -> str:
    return "english_stem" if config == "english" and token.kind in _STEMMED else "simple"


def json_filter(text: str) -> frozenset[str]:
    """json_to_tsvector's filter: a word, or an array of them."""
    value = json.loads(text)
    if isinstance(value, dict):
        raise PostgresError("22023", "wrong flag type, only arrays and scalars are allowed")
    names = value if isinstance(value, list) else [value]
    for name in names:
        if not isinstance(name, str):
            raise PostgresError("22023", "flag array element is not a string")
        if name not in ("all", "key", "string", "numeric", "boolean"):
            raise PostgresError("22023", f'wrong flag in flag array: "{name}"')
    if "all" in names:
        return frozenset({"key", "string", "numeric", "boolean"})
    return frozenset(names)


def _leaves(value: object, wanted: frozenset[str], binary: bool) -> list[str]:
    """The texts of a document, in the order iterate_json_values gives them."""
    if isinstance(value, dict):
        keys = sorted(value, key=lambda k: (len(k.encode()), k.encode())) if binary else list(value)
        found: list[str] = []
        for key in keys:
            if "key" in wanted:
                found.append(key)
            found.extend(_leaves(value[key], wanted, binary))
        return found
    if isinstance(value, list):
        return [text for item in value for text in _leaves(item, wanted, binary)]
    if isinstance(value, str):
        return [value] if "string" in wanted else []
    if isinstance(value, bool):
        return [str(value).lower()] if "boolean" in wanted else []
    return [str(value)] if value is not None and "numeric" in wanted else []


def json_to_tsvector(
    config: str, document: str, binary: bool, wanted: frozenset[str]
) -> dict[str, list[tuple[int, int]]]:
    """Each text's lexemes, a text's start one past the last position of the one before it."""
    parsed = json.loads(document, parse_float=Decimal, parse_int=Decimal)
    entries: dict[str, list[tuple[int, int]]] = {}
    start = 0
    for text in _leaves(parsed, wanted, binary):
        start += 1 if entries else 0
        indexed = 0
        for token in tokens(text):
            found = lexemes(config, token)
            if found is None:
                continue
            indexed += 1
            for lexeme in found:
                entries.setdefault(lexeme, []).append((start + indexed, 0))
        start += indexed
    return entries


#: Words of a document a headline shows all of, where its options say no fewer.
_MIN_WORDS = 15
_OPTION = re.compile(r"\s*(\w+)\s*=\s*([^,]*?)\s*(?:,|$)")
_COMPOUND = frozenset({ASCIIHWORD, URL})


def headline(config: str, document: str, query: Node | None, options: str = "") -> str | None:
    """prsd_headline of a document short enough to be shown whole, each word of a satisfied query
    between its marks; None for anything else, which turns on the fragment search."""
    start, stop = "<b>", "</b>"
    for name, value in _OPTION.findall(options):
        key = name.lower()
        if key == "startsel":
            start = value
        elif key == "stopsel":
            stop = value
        elif key != "shortword":
            return None
    read = tokens(document)
    if sum(1 for t in read if t.kind not in _NOT_INDEXED | _COMPOUND) > _MIN_WORDS:
        return None
    if any(t.kind == TAG for t in read):
        return None
    entries = to_tsvector(config, document)
    try:
        satisfied = pg_tsearch.matches(entries, query)
    except ValueError:
        return None
    wanted = pg_tsearch.operands(query) if satisfied else []
    out: list[str] = []
    for token in read:
        if token.kind in _COMPOUND:
            continue
        found = lexemes(config, token) or []
        hit = any(
            lexeme == w.lexeme or (w.prefix and lexeme.startswith(w.lexeme))
            for lexeme in found
            for w in wanted
        )
        out.append(f"{start}{token.text}{stop}" if hit else token.text)
    return "".join(out)


def _with_headlines(value: object, mark: Callable[[str], str | None]) -> object:
    if isinstance(value, dict):
        return {k: _with_headlines(v, mark) for k, v in value.items()}
    if isinstance(value, list):
        return [_with_headlines(v, mark) for v in value]
    if isinstance(value, str):
        written = mark(value)
        if written is None:
            raise Unread(value)
        return written
    return value


def json_headline(document: str, mark: Callable[[str], str | None]) -> str:
    """A document with each string a headline, written as json_out writes a transformed one."""
    parsed = json.loads(document, parse_float=Decimal, parse_int=Decimal)
    return _json_text(_with_headlines(parsed, mark))


def _json_text(value: object) -> str:
    if isinstance(value, dict):
        members = (f"{json.dumps(k, ensure_ascii=False)}:{_json_text(v)}" for k, v in value.items())
        return "{" + ",".join(members) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_json_text(v) for v in value) + "]"
    if isinstance(value, Decimal):
        return str(value)
    return json.dumps(value, ensure_ascii=False)
