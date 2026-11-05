"""SPARQL's hash and URI-encoding functions (REQ-2135), each written once in the semantic SQL as a
call of its own -- ``provisa_digest(x, 'sha256')``, ``provisa_uri_encode(x)`` -- and lowered by
``transpile`` to the target dialect's own spelling: a hash as lowercase hex of the text's UTF-8
bytes, a URI encoding as RFC 3986 percent-encoding (its unreserved characters kept).

``LOWERINGS`` holds, for each dialect, the functions its SQL computes; the route's capability
list reads the same table (dialect_capabilities), so a source that cannot compute one is not
asked to, and an engine that cannot is refused by name (``NotComputed``)."""

# Requirements: REQ-2135

from __future__ import annotations

from collections.abc import Callable

import sqlglot.expressions as exp

DIGEST = "provisa_digest"
URI_ENCODE = "provisa_uri_encode"

MD5 = "MD5"
SHA1 = "SHA1"
SHA256 = "SHA256"
SHA384 = "SHA384"
SHA512 = "SHA512"
ENCODE_FOR_URI = "ENCODE_FOR_URI"


def _call(name: str, *args: exp.Expr) -> exp.Expr:
    return exp.Anonymous(this=name, expressions=list(args))


def _postgres_hex(name: str) -> Callable[[exp.Expr], exp.Expr]:
    # PostgreSQL's sha2 functions take bytes and give bytes: the text's UTF-8, its hex.
    return lambda x: _call(
        "encode", _call(name, _call("convert_to", x, exp.Literal.string("UTF8"))), exp.Literal.string("hex")
    )


def _trino_hex(name: str) -> Callable[[exp.Expr], exp.Expr]:
    # Trino's hash functions take varbinary and give varbinary; to_hex is uppercase.
    return lambda x: _call("lower", _call("to_hex", _call(name, _call("to_utf8", x))))


def _trino_uri(x: exp.Expr) -> exp.Expr:
    # Trino's url_encode is a form's: a space as +, ~ escaped, * kept. RFC 3986's: %20, ~, %2A.
    out = _call("url_encode", x)
    for found, wanted in (("+", "%20"), ("%7E", "~"), ("*", "%2A")):
        out = _call("replace", out, exp.Literal.string(found), exp.Literal.string(wanted))
    return out


LOWERINGS: dict[str, dict[str, Callable[[exp.Expr], exp.Expr]]] = {
    "postgres": {
        MD5: lambda x: _call("md5", x),
        SHA256: _postgres_hex("sha256"),
        SHA384: _postgres_hex("sha384"),
        SHA512: _postgres_hex("sha512"),
    },
    "duckdb": {
        MD5: lambda x: _call("md5", x),
        SHA1: lambda x: _call("sha1", x),
        SHA256: lambda x: _call("sha256", x),
        ENCODE_FOR_URI: lambda x: _call("url_encode", x),
    },
    "trino": {
        MD5: _trino_hex("md5"),
        SHA1: _trino_hex("sha1"),
        SHA256: _trino_hex("sha256"),
        SHA512: _trino_hex("sha512"),
        ENCODE_FOR_URI: _trino_uri,
    },
}


class NotComputed(ValueError):
    """A function the target dialect's SQL does not compute."""

    def __init__(self, function: str, dialect: str) -> None:
        self.function = function
        self.dialect = dialect
        super().__init__(f"{function} is not computed by {dialect}")


def mentioned(sql: str) -> bool:
    lowered = sql.lower()
    return DIGEST in lowered or URI_ENCODE in lowered


def _function_of(node: exp.Anonymous) -> str | None:
    """The function a call names: a digest by its algorithm, else ENCODE_FOR_URI."""
    name = node.name.lower()
    if name == URI_ENCODE:
        return ENCODE_FOR_URI
    if name == DIGEST:
        return node.expressions[1].name.upper()
    return None


def functions(tree: exp.Expr) -> list[str]:
    """The functions ``tree`` calls, in the order it calls them."""
    out = []
    for node in tree.find_all(exp.Anonymous):
        function = _function_of(node)
        if function is not None:
            out.append(function)
    return out


def lower(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree`` with each call written in ``dialect``'s SQL; ``NotComputed`` for one it does not
    compute."""
    computed = LOWERINGS.get(dialect, {})
    for node in list(tree.find_all(exp.Anonymous)):
        function = _function_of(node)
        if function is None:
            continue
        lowering = computed.get(function)
        if lowering is None:
            raise NotComputed(function, dialect)
        node.replace(lowering(node.expressions[0].copy()))
    return tree
