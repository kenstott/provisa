# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4a92-3b5d-4f08-9e26-d18a6f30c7b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's xml type on DuckDB (REQ-2167, REQ-2171): an xml value is held as its text, and
each function here is src/backend/utils/adt/xml.c's, over the libxml2 lxml carries. No document
can reach outside: a DTD is never loaded, the network is off, and an external entity resolves to
nothing, as xmlPgEntityLoader makes it for PostgreSQL."""

# Requirements: REQ-2167

from __future__ import annotations

import base64
import copy
import json
import math
import re
import uuid
from decimal import Decimal
from typing import Any

from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]  # lxml ships no stubs for its C module

from provisa.federation.duckdb_pg_scalars import _error

_DEFAULT_VERSION = "1.0"
_SPACE = " \t\r\n"
#: A name character that continues the five bytes of "<?xml" into another processing instruction.
_NAME_CHAR = re.compile(r"[\w:.\-·]")
_OK = 0
#: parse_xml_decl's failure, whichever of libxml2's codes it is.
_BAD_DECLARATION = 1


def _skip_space(text: str, position: int) -> int:
    while position < len(text) and text[position] in _SPACE:
        position += 1
    return position


def _quoted(text: str, position: int) -> tuple[str, int] | None:
    """The quoted value at ``position`` and where it ends; None where there is none."""
    quote = text[position : position + 1]
    if quote not in ("'", '"'):
        return None
    end = text.find(quote, position + 1)
    return None if end < 0 else (text[position + 1 : end], end + 1)


def _standalone(text: str, position: int) -> tuple[int, int] | None:
    for spelled, value in (("'yes'", 1), ('"yes"', 1), ("'no'", 0), ('"no"', 0)):
        if text.startswith(spelled, position):
            return value, position + len(spelled)
    return None


def _named_value(text: str, position: int, name: str) -> tuple[int, int] | None:
    """Where the value of ``name`` at ``position`` starts, past the ``=`` and space, and the
    position after its name; None where the name is not followed by ``=``."""
    after = _skip_space(text, position + len(name))
    if text[after : after + 1] != "=":
        return None
    return _skip_space(text, after + 1), after


def parse_declaration(  # noqa: PLR0911, PLR0912 -- parse_xml_decl, one return per failure
    text: str,
) -> tuple[int, int, str | None, int]:
    """parse_xml_decl: (error code, the declaration's length, its version, its standalone: 1
    yes, 0 no, -1 not there)."""
    if not text.startswith("<?xml") or _NAME_CHAR.match(text[5:6] or " "):
        return _OK, 0, None, -1
    position = 5
    if text[position : position + 1] not in tuple(_SPACE):
        return _BAD_DECLARATION, 0, None, -1
    position = _skip_space(text, position)
    if not text.startswith("version", position):
        return _BAD_DECLARATION, 0, None, -1
    named = _named_value(text, position, "version")
    value = None if named is None else _quoted(text, named[0])
    if value is None:
        return _BAD_DECLARATION, 0, None, -1
    version, position = value

    saved = position
    position = _skip_space(text, position)
    if text.startswith("encoding", position):
        if text[saved : saved + 1] not in tuple(_SPACE):
            return _BAD_DECLARATION, 0, version, -1
        named = _named_value(text, position, "encoding")
        value = None if named is None else _quoted(text, named[0])
        if value is None:
            return _BAD_DECLARATION, 0, version, -1
        position = value[1]
    else:
        position = saved

    standalone = -1
    saved = position
    position = _skip_space(text, position)
    if text.startswith("standalone", position):
        if text[saved : saved + 1] not in tuple(_SPACE):
            return _BAD_DECLARATION, 0, version, -1
        named = _named_value(text, position, "standalone")
        said = None if named is None else _standalone(text, named[0])
        if said is None:
            return _BAD_DECLARATION, 0, version, -1
        standalone, position = said
    else:
        position = saved

    position = _skip_space(text, position)
    if not text.startswith("?>", position):
        return _BAD_DECLARATION, 0, version, standalone
    position += 2
    if not text[:position].isascii():
        return _BAD_DECLARATION, 0, version, standalone
    return _OK, position, version, standalone


def _print_declaration(version: str | None, standalone: int) -> str:
    """print_xml_decl: nothing unless a version other than 1.0 or a standalone says something."""
    if (version is not None and version != _DEFAULT_VERSION) or standalone != -1:
        said = {1: ' standalone="yes"', 0: ' standalone="no"'}.get(standalone, "")
        return f'<?xml version="{version or _DEFAULT_VERSION}"{said}?>'
    return ""


def xml_out(value: str) -> str:
    """xml_out: the value with its declaration written as print_xml_decl writes it, which is
    nothing for most; a newline after a declaration that goes goes with it."""
    code, length, version, standalone = parse_declaration(value)
    if code != _OK:
        return value
    declaration = _print_declaration(version, standalone)
    rest = value[length:]
    if not declaration and rest.startswith("\n"):
        rest = rest[1:]
    return declaration + rest


def xml_send(value: str) -> bytes:
    return xml_out(value).encode()


# --- reading a document ---------------------------------------------------------------------------


class _NoExternal(etree.Resolver):
    """xmlPgEntityLoader: whatever a document points outside itself at is empty. An empty string,
    not ``resolve_empty``, which lets libxml2 go on to read the file; and a resolver, not
    ``resolve_entities="internal"``, which leaves a parameter entity read."""

    def resolve(self, system_url: Any, public_id: Any, context: Any) -> Any:
        return self.resolve_string("", context)


def _parser(*, blanks: bool = True) -> Any:
    parser = etree.XMLParser(
        recover=True,  # the error log decides, as PostgreSQL's strictness levels do
        resolve_entities=True,
        no_network=True,
        load_dtd=False,
        dtd_validation=False,
        huge_tree=False,
        strip_cdata=False,
        remove_blank_text=not blanks,
        remove_comments=False,
        remove_pis=False,
        encoding="utf-8",
    )
    parser.resolvers.add(_NoExternal())
    return parser


def _failed(parser: Any, *, namespaces: bool) -> bool:
    """Whether a parse failed. PostgreSQL's PG_XML_STRICTNESS_WELLFORMED lets a namespace error
    by; PG_XML_STRICTNESS_ALL (``namespaces``) does not."""
    return any(
        entry.level >= etree.ErrorLevels.ERROR
        and (namespaces or entry.domain != etree.ErrorDomains.NAMESPACE)
        for entry in parser.error_log
    )


def _read_document(text: str, *, blanks: bool = True, namespaces: bool = False) -> Any | None:
    """The document ``text`` is, or None where it is not one."""
    parser = _parser(blanks=blanks)
    data = text.encode()
    try:
        root = etree.fromstring(data, parser) if data.strip() else None
    except etree.XMLSyntaxError:
        return None
    if root is None or _failed(parser, namespaces=namespaces):
        return None
    return root.getroottree()


def _read_content(text: str, *, blanks: bool = True) -> Any | None:
    """xmlParseBalancedChunkMemory: the element holding the nodes of ``text``, or None. The
    holder's name is drawn afresh, so no content can close it."""
    holder = f"h{uuid.uuid4().hex}"
    parser = _parser(blanks=blanks)
    try:
        root = etree.fromstring(f"<{holder}>{text}</{holder}>".encode(), parser)
    except etree.XMLSyntaxError:
        return None
    if root is None or root.tag != holder or _failed(parser, namespaces=False):
        return None
    return root


def _doctype_in_content(text: str) -> bool:
    """xml_doctype_in_content: whether only space, comments and processing instructions come
    before a <!DOCTYPE: such a value is parsed as a document."""
    position = 0
    while True:
        position = _skip_space(text, position)
        if text[position : position + 1] != "<":
            return False
        position += 1
        if text[position : position + 1] == "!":
            position += 1
            if text.startswith("DOCTYPE", position):
                return True
            if not text.startswith("--", position):
                return False
            end = text.find("--", position + 2)
            if end < 0 or text[end + 2 : end + 3] != ">":
                return False
            position = end + 3
            continue
        if text[position : position + 1] != "?":
            return False
        end = text.find("?>", position)
        if end < 0:
            return False
        position = end + 2


def _content_failure(value: str) -> str | None:
    """Why ``value`` is not CONTENT, as xml_parse says it; None where it is."""
    code, length, _, _ = parse_declaration(value)
    if code != _OK:
        return "invalid XML content: invalid XML declaration"
    rest = value[length:]
    if _doctype_in_content(rest):
        return None if _read_document(value) is not None else "invalid XML content"
    if rest and _read_content(rest) is None:
        return "invalid XML content"
    return None


def xml_in(value: str) -> str:
    """xml_in, and xmlparse(content ...): the value, as it was written, if it is XML content."""
    failure = _content_failure(value)
    if failure is not None:
        raise _error("2200N", failure)
    return value


def _array_cells(value: str) -> list[str | None]:
    """The elements of a one-dimensional array's text, as array_in reads them."""
    body = value.strip()
    if not (body.startswith("{") and body.endswith("}")):
        raise _error("22P02", f'malformed array literal: "{value}"')
    cells: list[str | None] = []
    position = 1
    end = len(body) - 1
    while position < end:
        if body[position] == '"':
            position += 1
            cell: list[str] = []
            while position < end and body[position] != '"':
                position += body[position] == "\\"
                cell.append(body[position])
                position += 1
            cells.append("".join(cell))
            position += 2  # the quote and the comma
        else:
            stop = body.find(",", position)
            stop = end if stop < 0 else stop
            written = body[position:stop].strip()
            cells.append(None if written.upper() == "NULL" else written)
            position = stop + 1
    return cells


def xml_array_in(value: str) -> list[str | None]:
    """xml[]'s input: each element read as xml's input reads it."""
    return [None if cell is None else xml_in(cell) for cell in _array_cells(value)]


def xmlparse_document(value: str) -> str:
    if _read_document(value) is None:
        raise _error("2200M", "invalid XML document")
    return value


def is_well_formed(value: str, document: bool) -> bool:
    if document:
        return _read_document(value) is not None
    return _content_failure(value) is None


def is_document(value: str) -> bool:
    return _read_document(value) is not None


# --- the constructors -----------------------------------------------------------------------------


def _escape(text: str) -> str:
    """escape_xml."""
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\r", "&#x0d;")
    )


def _ascii_reference(found: re.Match[str]) -> str:
    return f"&#x{ord(found[0]):X};"


def _attribute(text: str) -> str:
    """An attribute's value as libxml2's writer writes it with no document's encoding at hand: a
    character outside ASCII is a reference."""
    escaped = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("\n", "&#10;")
        .replace("\t", "&#9;")
        .replace("\r", "&#13;")
    )
    return re.sub(r"[^\x00-\x7f]", _ascii_reference, escaped)


def xmlcomment(value: str) -> str:
    if "--" in value or value.endswith("-"):
        raise _error("2200S", "invalid XML comment")
    return f"<!--{value}-->"


def xmltext(value: str) -> str:
    """xmlEncodeSpecialChars."""
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("\r", "&#13;")
    )


def xmlconcat(values: list[str | None] | None) -> str | None:
    """xmlconcat: the values without their declarations, under one where they all agree."""
    present = [v for v in values or () if v is not None]
    if not present:
        return None
    standalone = 1
    version: str | None = None
    no_version = False
    bodies: list[str] = []
    for value in present:
        _, length, own_version, own_standalone = parse_declaration(value)
        if own_standalone == 0 and standalone == 1:
            standalone = 0
        if own_standalone < 0:
            standalone = -1
        if own_version is None:
            no_version = True
        elif version is None:
            version = own_version
        elif own_version != version:
            no_version = True
        bodies.append(value[length:])
    declaration = ""
    if not no_version or standalone >= 0:
        declaration = _print_declaration(None if no_version else version, standalone)
    return declaration + "".join(bodies)


def xmlconcat2(a: str | None, b: str | None) -> str | None:
    if a is None or b is None:
        return b if a is None else a
    return xmlconcat([a, b])


def xmlagg(values: list[str | None] | None) -> str | None:
    """xmlagg: xmlconcat2 over the values, in turn; a group of no rows has no list."""
    total: str | None = None
    for value in values or ():
        total = xmlconcat2(total, value)
    return total


def xmlelement(
    name: str, names: list[str], values: list[str | None], contents: list[str | None]
) -> str:
    attributes = "".join(f' {n}="{_attribute(v)}"' for n, v in zip(names, values) if v is not None)
    held = [c for c in contents if c is not None]
    if not held:
        return f"<{name}{attributes}/>"
    return f"<{name}{attributes}>{''.join(held)}</{name}>"


def xmlforest(names: list[str], values: list[str | None]) -> str | None:
    held = [f"<{n}>{v}</{n}>" for n, v in zip(names, values) if v is not None]
    return "".join(held) if held else None


def xmlpi(target: str, value: str | None, has_value: bool) -> str | None:
    if target.lower() == "xml":
        raise _error("42601", "invalid XML processing instruction")
    if has_value and value is None:
        return None
    if value is None:
        return f"<?{target}?>"
    if "?>" in value:
        raise _error("2200T", "invalid XML processing instruction")
    return f"<?{target} {value.lstrip(' ')}?>"


#: xmlroot's STANDALONE: what the statement says; None is that it says nothing.
_STANDALONE_SAID = {"yes": 1, "no": 0, "no value": -1}


def xmlroot(value: str, version: str | None, standalone: str | None) -> str:
    _, length, _, own = parse_declaration(value)
    said = own if standalone is None else _STANDALONE_SAID[standalone]
    return _print_declaration(version, said) + value[length:]


def _pretty_node(node: Any) -> str:
    written = etree.tostring(node, pretty_print=True, with_tail=False, encoding="unicode")
    return written[:-1] if written.endswith("\n") else written


def _indented_content(root: Any) -> str:
    """Each node of the content in turn, a newline before each that is not text and has one
    before it (xmltotext_with_options)."""
    out: list[str] = []
    placed = False
    if root.text:
        out.append(_escape(root.text))
        placed = True
    for node in root:
        if placed:
            out.append("\n")
        out.append(_pretty_node(node))
        placed = True
        if node.tail:
            out.append(_escape(node.tail))
    return "".join(out)


def _indented_document(tree: Any, value: str, *, declared: bool) -> str:
    """xmlSaveDoc: its declaration, if the value had one, says the encoding it was saved in."""
    written = etree.tostring(tree, pretty_print=True, encoding="unicode")
    if not declared:
        return written
    _, _, version, standalone = parse_declaration(value)
    said = {1: ' standalone="yes"', 0: ' standalone="no"'}.get(standalone, "")
    return f'<?xml version="{version or _DEFAULT_VERSION}" encoding="UTF-8"{said}?>\n{written}'


def xmlserialize(value: str, document: bool, indent: bool) -> str:
    """xmltotext_with_options."""
    if not document and not indent:
        return value
    _, length, _, _ = parse_declaration(value)
    as_document = document or _doctype_in_content(value[length:])
    if as_document:
        tree = _read_document(value, blanks=not indent)
        if tree is None:
            raise _error("2200L", "not an XML document")
        if not indent:
            return value
        written = _indented_document(tree, value, declared=length > 0)
        return written.rstrip("\r\n") if document else written
    if not value[length:]:
        return ""
    root = _read_content(value[length:], blanks=False)
    if root is None:
        raise _error("2200L", "not an XML document")
    return _indented_content(root)


# --- a value as XML: map_sql_value_to_xml_value ---------------------------------------------------


def _xsd_time(text: str, *, zone: bool) -> str:
    """A timestamp's text as XSD writes it: a T between date and time, a zone with its minutes."""
    written = text.replace(" ", "T", 1)
    if zone and re.search(r"[+-]\d\d$", written):
        written += ":00"
    return written


def _infinite(text: str, kind: str) -> None:
    if text.lstrip("-").lower() in ("infinity", "inf"):
        raise _error("22008", f"{kind} out of range")


def _float_text(text: str, base: str) -> str:
    """A float's text as float4out or float8out write it, from DuckDB's."""
    if base == "DOUBLE":
        return _float_out(float(text))
    written = {"nan": "NaN", "inf": "Infinity", "-inf": "-Infinity"}.get(text.lower(), text)
    return written.removesuffix(".0")


def _blob_bytes(text: str) -> bytes:
    """A BLOB's bytes from DuckDB's text of it: \\xHH for all but printable ASCII."""
    return re.sub(rb"\\x([0-9A-Fa-f]{2})", lambda m: bytes.fromhex(m[1].decode()), text.encode())


def _json_element(value: Any) -> str:
    if value is True or value is False:
        return "true" if value else "false"
    return value if isinstance(value, str) else json.dumps(value)


def _array_value(listed: str, kind: str) -> str:
    """map_sql_value_to_xml_value of an array: an <element> for each value that is not NULL."""
    element = kind[:-2]
    values = json.loads(listed, parse_float=Decimal)
    return "".join(
        f"<element>{_element_text(v, element)}</element>" for v in values if v is not None
    )


def _element_text(value: Any, kind: str) -> str:
    if isinstance(value, list):
        return "".join(
            f"<element>{_element_text(v, kind)}</element>" for v in value if v is not None
        )
    return _value_text(_json_element(value), kind, True)


def _value_text(text: str, kind: str, escape: bool) -> str:  # noqa: PLR0911 -- one per kind
    base = kind.partition("(")[0]
    if base == "BOOLEAN":
        return text
    if base == "DATE":
        _infinite(text, "date")
        return text
    if base.startswith("TIMESTAMP"):
        _infinite(text, "timestamp")
        return _xsd_time(text, zone=base == "TIMESTAMP WITH TIME ZONE")
    if base == "BLOB":
        return base64.b64encode(_blob_bytes(text)).decode()
    if base in ("DOUBLE", "FLOAT"):
        return _float_text(text, base)
    return _escape(text) if escape else text


def xml_value(text: str | None, kind: str, listed: str | None, escape: bool) -> str | None:
    """map_sql_value_to_xml_value of a value DuckDB gives as its text, its type's name, and, for
    a list, its JSON. ``escape``: false for an attribute, whose writer escapes it."""
    if text is None:
        return None
    if listed is not None and kind.endswith("[]"):
        return _array_value(listed, kind)
    return _value_text(text, kind, escape)


# --- XPath ----------------------------------------------------------------------------------------


def _float_out(value: float) -> str:
    """float8out: the shortest digits that read back, in PostgreSQL's layout."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "0"
    sign, digits, exponent = Decimal(repr(value)).normalize().as_tuple()
    written = "".join(map(str, digits))
    position = len(written) + int(exponent)  # where the point is, from the first digit
    prefix = "-" if sign else ""
    if position - 1 < -4 or position - 1 >= 15:  # noqa: PLR2004 -- ryu's switch to an exponent
        mantissa = written[0] + ("." + written[1:] if len(written) > 1 else "")
        power = position - 1
        return f"{prefix}{mantissa}e{'+' if power >= 0 else '-'}{abs(power):02d}"
    if position <= 0:
        return f"{prefix}0.{'0' * -position}{written}"
    if position >= len(written):
        return f"{prefix}{written}{'0' * (position - len(written))}"
    return f"{prefix}{written[:position]}.{written[position:]}"


def _xpath_number(value: float) -> str:
    """xmlXPathCastNumberToString."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == int(value) and abs(value) < 1e15:  # noqa: PLR2004 -- its integer branch
        return str(int(value))
    return f"{value:.15g}"


_TAG = re.compile(r"<!--.*?-->|<\?.*?\?>|<!\[CDATA\[.*?\]\]>|<[^<>]*>", re.DOTALL)
_QUOTED = re.compile(r'"[^"]*"')


def _ascii_attributes(found: re.Match[str]) -> str:
    """A tag with each attribute's value as xmlNodeDump writes it with no document's encoding."""
    tag = found[0]
    if tag.startswith(("<!", "<?")):
        return tag
    return _QUOTED.sub(lambda q: re.sub(r"[^\x00-\x7f]", _ascii_reference, q[0]), tag)


def _dump(node: Any) -> str:
    """xmlNodeDump of a copy of ``node``, which carries the namespaces it uses and no others."""
    written = etree.tostring(copy.deepcopy(node), with_tail=False, encoding="unicode")
    return _TAG.sub(_ascii_attributes, written)


def _dump_tree(tree: Any) -> str:
    written = etree.tostring(tree, encoding="unicode")
    return _TAG.sub(_ascii_attributes, written) + "\n"


def _is_cdata(node: Any) -> bool:
    """Whether a text node is a CDATA section, which xmlNodeDump writes as one: lxml hands it
    over as text, so its parent's serialized form says."""
    if not (getattr(node, "is_text", False) or getattr(node, "is_tail", False)):
        return False
    parent = node.getparent()
    if parent is None:
        return False
    return f"<![CDATA[{node}]]>" in etree.tostring(parent, encoding="unicode")


def _node_text(node: Any) -> str:
    """xml_xmlnodetoxmltype."""
    if isinstance(node, tuple):  # a namespace node: its URI
        return _escape(node[1])
    if isinstance(node, str):  # an attribute or a text node
        return f"<![CDATA[{node}]]>" if _is_cdata(node) else _escape(str(node))
    return _dump(node)


def _namespace_pairs(spec: str | None) -> dict[str, str]:
    """xpath()'s text[] mapping, from its JSON: none, or two-column rows."""
    pairs = [] if spec is None else json.loads(spec)
    if isinstance(pairs, str):  # a text[] the statement gives as '{{a,b}}'
        pairs = [
            [cell.strip().strip('"') for cell in row.split(",")]
            for row in re.findall(r"\{([^{}]*)\}", pairs)
            if row
        ]
    if not pairs:
        return {}
    if not all(isinstance(row, list) and len(row) == 2 for row in pairs):  # noqa: PLR2004
        raise _error("22000", "invalid array for XML namespace mapping")
    mapping: dict[str, str] = {}
    for name, value in pairs:
        if name is None or value is None:
            raise _error("22004", "neither namespace name nor URI may be null")
        if not name:
            raise _error(
                "XX000", f'could not register XML namespace with name "" and URI "{value}"'
            )
        mapping[name] = value
    return mapping


def _xpath_document(value: str) -> Any:
    _, length, _, _ = parse_declaration(value)
    tree = _read_document(value[length:], namespaces=True)
    if tree is None:
        raise _error("2200M", "could not parse XML document")
    return tree


def _compiled(path: str, namespaces: dict[str, str], sqlstate: str = "XX000") -> Any:
    try:
        return etree.XPath(path, namespaces=namespaces or None)
    except etree.XPathSyntaxError as exc:
        raise _error(sqlstate, "invalid XPath expression") from exc


def _evaluated(find: Any, context: Any) -> Any:
    try:
        return find(context)
    except etree.XPathError as exc:
        raise _error("XX000", "could not create XPath object") from exc


def _with_document_node(path: str, namespaces: dict[str, str], tree: Any, found: list) -> list:
    """A node set with the document node back in it: lxml drops that node, so it is seen by the
    set's count."""
    if not path.lstrip().startswith("/"):
        return found
    try:
        counted = etree.XPath(f"count({path})", namespaces=namespaces or None)(tree)
    except etree.XPathError:
        return found
    if counted == len(found) + 1:
        return [tree, *found]
    return found


def _xpath_result(path: str, value: str, namespaces: str | None) -> tuple[Any, Any, dict]:
    mapping = _namespace_pairs(namespaces)
    if not path:
        raise _error("22000", "empty XPath expression")
    tree = _xpath_document(value)
    found = _evaluated(_compiled(path, mapping), tree)
    return found, tree, mapping


def xpath(path: str, value: str, namespaces: str | None) -> list[str]:
    found, tree, mapping = _xpath_result(path, value, namespaces)
    if isinstance(found, bool):
        return ["true" if found else "false"]
    if isinstance(found, float):
        return [_float_out(found)]
    if isinstance(found, str):
        return [_escape(str(found))]
    return [
        _dump_tree(node) if node is tree else _node_text(node)
        for node in _with_document_node(path, mapping, tree, found)
    ]


def xpath_exists(path: str, value: str, namespaces: str | None) -> bool:
    found, tree, mapping = _xpath_result(path, value, namespaces)
    if isinstance(found, list):
        return len(_with_document_node(path, mapping, tree, found)) > 0
    return True  # a boolean, a number or a string is one item, whatever it says


# --- XMLTABLE -------------------------------------------------------------------------------------


def _string_value(node: Any) -> str:
    """xmlXPathCastNodeSetToString of one node."""
    if isinstance(node, tuple):
        return node[1]
    if isinstance(node, str):
        return str(node)
    return etree.XPath("string()")(node)


def _column_value(found: Any, column: dict[str, Any]) -> str | None:
    """XmlTableGetValue: one column's text for one row, or None."""
    as_xml = column["xml"]
    if isinstance(found, bool):
        if column["numeric"]:
            return "1" if found else "0"
        return "true" if found else "false"
    if isinstance(found, float):
        return _xpath_number(found)
    if isinstance(found, str):
        return _escape(str(found)) if as_xml else str(found)
    if not found:
        return None
    if as_xml:
        return "".join(_node_text(node) for node in found)
    if len(found) > 1:
        raise _error("21000", "more than one value returned by column XPath expression")
    return _string_value(found[0])


def _value_at(row: Any, path: str, find: Any) -> Any:
    if isinstance(row, str):  # lxml keeps no context of a text or attribute node
        return row if path.strip() == "." else []
    return _evaluated(find, row)


def xmltable(  # noqa: PLR0912 -- XmlTableSetRowFilter to XmlTableGetValue, in their order
    value: str | None, path: str | None, namespaces: str, columns: str
) -> list[list[str | None]]:
    """The rows XMLTABLE reads, each column's text before its cast (or None)."""
    if path is None:
        raise _error("22004", "row filter expression must not be null")
    if not path:
        raise _error("22000", "row path filter must not be empty string")
    described = json.loads(columns)
    mapping = dict(json.loads(namespaces))
    for column in described:
        if column["path"] == "":
            raise _error("22000", "column path filter must not be empty string")
    if value is None:
        return []
    tree = _read_document(xml_out(value), namespaces=True)
    if tree is None:
        raise _error("2200M", "could not parse XML document")
    rows = _evaluated(_compiled(path, mapping, "42601"), tree)
    finders = [
        None if c["ordinality"] else _compiled(c["path"], mapping, "22000") for c in described
    ]
    if not isinstance(rows, list):
        return []
    out: list[list[str | None]] = []
    for number, row in enumerate(rows, 1):
        cells: list[str | None] = []
        for column, find in zip(described, finders):
            if find is None:
                cells.append(str(number))
            else:
                cells.append(_column_value(_value_at(row, column["path"], find), column))
        out.append(cells)
    return out


# --- the registrations ----------------------------------------------------------------------------

_V = "VARCHAR"

_UDFS: tuple[tuple[str, Any, list[str], str], ...] = (
    ("provisa_xml_in", xml_in, [_V], _V),
    ("provisa_xml_array_in", xml_array_in, [_V], "VARCHAR[]"),
    ("provisa_xmlparse_document", xmlparse_document, [_V], _V),
    ("provisa_xml_send", xml_send, [_V], "BLOB"),
    ("provisa_xml_well_formed", is_well_formed, [_V, "BOOLEAN"], "BOOLEAN"),
    ("provisa_xml_is_document", is_document, [_V], "BOOLEAN"),
    ("provisa_xmlcomment", xmlcomment, [_V], _V),
    ("provisa_xmltext", xmltext, [_V], _V),
    ("provisa_xmlserialize", xmlserialize, [_V, "BOOLEAN", "BOOLEAN"], _V),
    ("provisa_xpath", xpath, [_V, _V, _V], "VARCHAR[]"),
    ("provisa_xpath_exists", xpath_exists, [_V, _V, _V], "BOOLEAN"),
)

#: UDFs that see a NULL: PostgreSQL's function, or its SQL syntax, is not strict in all arguments.
_NON_STRICT_UDFS: tuple[tuple[str, Any, list[str], str], ...] = (
    ("provisa_xmlconcat2", xmlconcat2, [_V, _V], _V),
    ("provisa_xmlconcat", xmlconcat, ["VARCHAR[]"], _V),
    ("provisa_xmlagg", xmlagg, ["VARCHAR[]"], _V),
    ("provisa_xmlelement", xmlelement, [_V, "VARCHAR[]", "VARCHAR[]", "VARCHAR[]"], _V),
    ("provisa_xmlforest", xmlforest, ["VARCHAR[]", "VARCHAR[]"], _V),
    ("provisa_xmlpi", xmlpi, [_V, _V, "BOOLEAN"], _V),
    ("provisa_xml_value", xml_value, [_V, _V, _V, "BOOLEAN"], _V),
    ("provisa_xmltable", xmltable, [_V, _V, _V, _V], "VARCHAR[][]"),
)

#: A strict UDF over a value, two others its optional: xmlroot's version and standalone.
_ROOT_UDF = ("provisa_xmlroot", xmlroot, [_V, _V, _V], _V)


def _strict_in_first(function: Any) -> Any:
    def xmlroot_of(value: Any, version: Any, standalone: Any) -> Any:
        return None if value is None else function(value, version, standalone)

    return xmlroot_of


_MACROS: tuple[str, ...] = (
    # A value that needs no reading: what query_to_xml wrote (provisa.compiler.xml_mapping).
    "CREATE MACRO provisa_xml(value) AS CAST(value AS VARCHAR)",
    "CREATE MACRO provisa_xml_text(value) AS provisa_xml_value(CAST(value AS VARCHAR), "
    "typeof(value), CASE WHEN typeof(value) LIKE '%[]' THEN CAST(to_json(value) AS VARCHAR) "
    "END, true)",
    "CREATE MACRO provisa_xml_attribute(value) AS provisa_xml_value(CAST(value AS VARCHAR), "
    "typeof(value), CASE WHEN typeof(value) LIKE '%[]' THEN CAST(to_json(value) AS VARCHAR) "
    "END, false)",
    "CREATE MACRO xml_is_well_formed(value) AS provisa_xml_well_formed(value, false)",
    "CREATE MACRO xml_is_well_formed_content(value) AS provisa_xml_well_formed(value, false)",
    "CREATE MACRO xml_is_well_formed_document(value) AS provisa_xml_well_formed(value, true)",
    "CREATE MACRO xml_send(value) AS provisa_xml_send(value)",
    "CREATE MACRO xmlcomment(value) AS provisa_xmlcomment(value)",
    "CREATE MACRO xmltext(value) AS provisa_xmltext(value)",
    "CREATE MACRO xmlconcat2(a, b) AS provisa_xmlconcat2(a, b)",
    "CREATE MACRO xmlexists(path, value) AS provisa_xpath_exists(path, value, '[]')",
    "CREATE MACRO xpath(path, value) AS provisa_xpath(path, value, '[]'), "
    "(path, value, spaces) AS provisa_xpath(path, value, CAST(to_json(spaces) AS VARCHAR))",
    "CREATE MACRO xpath_exists(path, value) AS provisa_xpath_exists(path, value, '[]'), "
    "(path, value, spaces) AS provisa_xpath_exists(path, value, CAST(to_json(spaces) AS VARCHAR))",
    "CREATE MACRO xmlvalidate(value, dtd) AS CASE WHEN value IS NULL OR dtd IS NULL THEN "
    "CAST(NULL AS BOOLEAN) ELSE error('postgresql-error 0A000: xmlvalidate is not implemented') "
    "END",
)


def register(con: Any) -> None:
    """Create the functions on DuckDB connection ``con``."""
    for name, function, parameters, returns in _UDFS:
        con.create_function(name, function, parameters, returns, side_effects=False)
    root_name, root, root_parameters, root_returns = _ROOT_UDF
    special = (
        *_NON_STRICT_UDFS,
        (root_name, _strict_in_first(root), root_parameters, root_returns),
    )
    for name, function, parameters, returns in special:
        con.create_function(
            name, function, parameters, returns, null_handling="special", side_effects=False
        )
    for macro in _MACROS:
        con.execute(macro)
