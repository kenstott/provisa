# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8b4d7a-3c1e-4a9b-8f2d-5e7c9a1b3d4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher query parser. Produces a CypherAST for read-only queries.

Rejects write clauses (CREATE, MERGE, SET, DELETE, DETACH, REMOVE) and APOC
references at parse time. Rejects unbounded variable-length patterns ([*]).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.cypher.expr_parser import CypherExprParseError, parse_expression
from provisa.gql.expr_ast import CypherExpr, Variable

if TYPE_CHECKING:
    from provisa.gql.label_map import CypherLabelMap

from provisa.gql.mistakes import (
    aggregate_misuse,
    expression_misuse,
    projection_misuse,
    undefined_variable,
    variable_misuse,
)
from provisa.gql.translator_types import CypherMistake
from provisa.gql.ir import (
    CreateClause,
    DeleteClause,
    MergeClause,
    RemoveClause,
    RemoveItem,
    SetClause,
    SetItem,
    NodePattern,
    RelPattern,
    PathPattern,
    PathFunction,
    MatchClause,
    WhereClause,
    ReturnItem,
    ReturnClause,
    WithClause,
    UnwindClause,
    MatchStep,
    CallSubquery,
    ProcedureCall,
    OrderItem,
    CypherAST,
)

# Requirements: REQ-345, REQ-346, REQ-347, REQ-348, REQ-571


class CypherParseError(Exception):
    """Raised for invalid or unsupported Cypher syntax."""


class CypherParseMistake(CypherMistake, CypherParseError):
    """REQ-778: a mistake the parse finds, coded and worded for the user; every surface that
    refuses a parse error refuses it as one."""


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

_TOKEN_SPEC = [
    ("COMMENT", r"//[^\n]*"),
    ("STRING", r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\""),
    # Hexadecimal (0x1F), octal (0o17) and decimal numbers, a decimal with or without integer
    # digits (.5) and with an exponent (1e9, 1.5E-3). A dot needs a digit after it, so the range
    # of a variable-length pattern (*1..3) stays two numbers.
    ("NUMBER", r"0[xX][0-9A-Fa-f]+|0o[0-7]+|(?:\d+\.\d+|\.\d+|\d+)(?:[eE][-+]?\d+)?"),
    # A parameter's name may be a number ($0, $1) as well as a name: $3 is parameter "3", never 3.
    ("PARAM", r"\$\w+"),
    ("ARROW_RIGHT", r"->"),
    ("ARROW_LEFT", r"<-"),
    ("DOTDOT", r"\.\."),
    ("DOT", r"\."),
    ("COLON", r":"),
    ("LBRACE", r"\{"),
    ("RBRACE", r"\}"),
    ("LPAREN", r"\("),
    ("RPAREN", r"\)"),
    ("LBRACKET", r"\["),
    ("RBRACKET", r"\]"),
    ("COMMA", r","),
    ("STAR", r"\*"),
    ("PLUS", r"\+"),
    ("MINUS", r"-"),
    ("SLASH", r"/"),
    ("PERCENT", r"%"),
    ("CARET", r"\^"),
    ("REGEX_MATCH", r"=~"),
    ("EQ", r"="),
    ("NEQ", r"<>|!="),
    ("LTE", r"<="),
    ("GTE", r">="),
    ("LT", r"<"),
    ("GT", r">"),
    ("PIPE", r"\|"),
    ("SEMICOLON", r";"),
    ("IDENT", r"[A-Za-z_]\w*"),
    # A name in backticks (`a name`): read as the name it quotes.
    ("QUOTED_IDENT", r"`(?:[^`]|``)+`"),
    ("NEWLINE", r"\n"),
    ("SKIP_WS", r"[ \t]+"),
]

_MASTER_RE = re.compile(
    "|".join(f"(?P<{name}>{pat})" for name, pat in _TOKEN_SPEC),
    re.IGNORECASE,
)

_WRITE_KEYWORDS = {"CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE"}


@dataclass
class Token:
    type: str
    value: str
    pos: int
    #: Where the token ends in the statement: a quoted name's value is shorter than its text.
    end: int = -1

    def __post_init__(self) -> None:
        if self.end < 0:
            self.end = self.pos + len(self.value)


_INTEGER_MAX = 2**63 - 1
_ESCAPE = re.compile(r"\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|[\\'\"bfnrt])|\\(.?)", re.S)


def _integer(text: str) -> int | None:
    """The integer a NUMBER token writes, or None for a float."""
    if text[:2] in ("0x", "0X"):
        return int(text, 16)
    if text[:2] == "0o":
        return int(text[2:], 8)
    return None if any(c in text for c in ".eE") else int(text)


def _refuse_integer_overflow(token: Token, before: Token | None) -> None:
    """An integer literal beyond a 64-bit integer: -2^63 is one only after a minus sign."""
    value = _integer(token.value)
    if value is None or value <= _INTEGER_MAX:
        return
    if value == _INTEGER_MAX + 1 and before is not None and before.type == "MINUS":
        return
    written = f"-{token.value}" if before is not None and before.type == "MINUS" else token.value
    raise CypherParseMistake(
        "cypher.integer_overflow",
        f"{written} is outside the range of an integer, -9223372036854775808 to "
        "9223372036854775807.",
        value=written,
    )


def _refuse_unknown_escape(token: Token) -> None:
    """A backslash in a string that starts no escape Cypher reads (\\uH)."""
    for m in _ESCAPE.finditer(token.value[1:-1]):
        if m.group(1) is None:
            escape = "\\" + m.group(2)
            raise CypherParseMistake(
                "cypher.invalid_unicode_escape",
                f"The string escape {escape} is not one Cypher reads. A Unicode escape is \\u "
                "and four hexadecimal digits, e.g. \\u00e9.",
                escape=escape,
            )


def _tokenize(text: str) -> tuple[list[Token], list[str]]:
    tokens: list[Token] = []
    comments: list[str] = []
    end = 0
    for m in _MASTER_RE.finditer(text):
        if m.start() != end:
            # A character no token reads is refused where it stands, never skipped.
            raise CypherParseError(f"Unexpected character {text[end]!r} at position {end}")
        end = m.end()
        kind = m.lastgroup
        # A semicolon ends the one statement a query holds (refuse_multiple_statements).
        if kind in ("SKIP_WS", "NEWLINE", "SEMICOLON"):
            continue
        if kind == "COMMENT":
            comments.append(m.group()[2:].strip())  # strip leading //
            continue
        assert kind is not None
        if kind == "QUOTED_IDENT":
            # A compound label a Provisa graph names (`Shop:Orders`) is its two parts, as written
            # unquoted; any other quoted name is one name.
            parts = m.group()[1:-1].replace("``", "`").split(":")
            for n, part in enumerate(parts):
                if n:
                    tokens.append(Token("COLON", ":", m.start(), m.end()))
                tokens.append(Token("IDENT", part, m.start(), m.end()))
            continue
        token = Token(kind, m.group(), m.start())
        if kind == "NUMBER":
            _refuse_integer_overflow(token, tokens[-1] if tokens else None)
        elif kind == "STRING":
            _refuse_unknown_escape(token)
        tokens.append(token)
    if end != len(text):
        raise CypherParseError(f"Unexpected character {text[end]!r} at position {end}")
    return tokens, comments


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


#: The prefix of the name an anonymous pattern with an inline property map is given.
INLINE_PREFIX = "__inline"


def _in_scope(pipeline: list) -> list[str]:
    """The variables in scope after ``pipeline``: what its last WITH carries, and what each clause
    after it binds, in the order bound."""
    from provisa.gql.mistakes import _names

    scope: list[str] = []
    for step in pipeline:
        if isinstance(step, WithClause):
            scope = []
            for item in step.items:
                if item.alias:
                    scope.append(item.alias)
                elif isinstance(item.expression, Variable):
                    scope.append(item.expression.name)
            continue
        bound: set[str] = set()
        _names(step, bound, [])
        scope.extend(sorted(n for n in bound if n not in scope))
    return scope


# REQ-2168: APOC's path expanders, compiled to the variable-length pattern they walk -- and so to
# the same recursive SQL as a variable-length path -- not called as commands.
_PATH_PROCEDURES = {"apoc.path.expand": "path", "apoc.path.subgraphnodes": "node"}


def _literal_arg(call: ProcedureCall, position: int, what: str) -> Any:
    from provisa.gql.expr_ast import Literal, MapLiteral, Unary

    arg = call.args[position]
    if isinstance(arg, Literal):
        return arg.value
    if isinstance(arg, Unary) and arg.op == "-" and isinstance(arg.operand, Literal):
        return -arg.operand.value  # type: ignore[operator]
    if isinstance(arg, MapLiteral) and all(isinstance(v, Literal) for _, v in arg.entries):
        return {k: v.value for k, v in arg.entries}  # type: ignore[attr-defined]
    raise CypherParseError(
        f"CALL {call.name}: its {what} (argument {position + 1}) is written as a value"
    )


def _relationship_filter(call: ProcedureCall, spec: str) -> tuple[list[str], str]:
    """``A>|<B|C`` as (types, direction): every type is walked one way, out (``>``), in (``<``)
    or either (no mark)."""
    types: list[str] = []
    directions: set[str] = set()
    for part in (p.strip() for p in spec.split("|")) if spec else ():
        if part.endswith(">"):
            directions.add("right")
            part = part[:-1]
        elif part.startswith("<"):
            directions.add("left")
            part = part[1:]
        else:
            directions.add("none")
        if part:
            types.append(part)
    if len(directions) > 1:
        raise CypherParseError(
            f"CALL {call.name}: relationshipFilter {spec!r} walks its types in different "
            "directions; one pattern walks every type one way"
        )
    return types, directions.pop() if directions else "none"


def _label_filter(call: ProcedureCall, spec: str) -> list[str]:
    """``+Label`` (or ``>Label``) as the label the walk ends at."""
    spec = spec.strip()
    if not spec:
        raise CypherParseError(
            f"CALL {call.name}: labelFilter names the label the walk ends at (+Label)"
        )
    label = spec[1:] if spec[0] in "+>" else spec
    if not label.isidentifier():
        raise CypherParseError(
            f"CALL {call.name}: labelFilter {spec!r} is read as one label a walk ends at "
            "(+Label); blacklists, termination filters and several labels are not"
        )
    return [label]


def _start_labels(call: ProcedureCall, start: str, pipeline: list) -> list[str]:
    """The labels the statement matched the walk's start node with."""
    for step in pipeline:
        for clause in getattr(step, "matches", ()):
            for node in getattr(clause.pattern, "nodes", ()):
                if node.variable == start and node.labels:
                    return list(node.labels)
    raise CypherParseError(
        f"CALL {call.name}: its start {start!r} is a node the statement matches with a label "
        f"before the call, as MATCH ({start}:Label)"
    )


def _path_procedure(call: ProcedureCall, serial: int, pipeline: list) -> MatchStep:
    """``CALL apoc.path.expand(start, relFilter, labelFilter, min, max) YIELD path`` or
    ``CALL apoc.path.subgraphNodes(start, {relationshipFilter, labelFilter, minLevel, maxLevel})
    YIELD node`` as the variable-length MATCH it walks."""
    from provisa.gql.expr_ast import Variable as _Var

    column = _PATH_PROCEDURES[call.name.lower()]
    expand = column == "path"
    if len(call.args) != (5 if expand else 2) or not isinstance(call.args[0], _Var):
        raise CypherParseError(
            f"CALL {call.name} takes "
            + (
                "(startNode, relationshipFilter, labelFilter, minLevel, maxLevel)"
                if expand
                else "(startNode, config)"
            )
            + ", its start a node the statement has matched"
        )
    if [col for col, _ in call.yields] != [column]:
        raise CypherParseError(f"CALL {call.name} yields {column}")
    start = call.args[0].name
    if expand:
        rel_spec = _literal_arg(call, 1, "relationshipFilter") or ""
        label_spec = _literal_arg(call, 2, "labelFilter") or ""
        low, high = _literal_arg(call, 3, "minLevel"), _literal_arg(call, 4, "maxLevel")
    else:
        config = _literal_arg(call, 1, "config")
        rel_spec = config.get("relationshipFilter", "")
        label_spec = config.get("labelFilter", "")
        low, high = config.get("minLevel", 0), config.get("maxLevel")
    if not isinstance(high, int) or high < 0:
        raise CypherParseError(
            f"CALL {call.name}: maxLevel bounds the walk; an unbounded one is not read"
        )
    types, direction = _relationship_filter(call, rel_spec)
    labels = _label_filter(call, label_spec)
    start_labels = _start_labels(call, start, pipeline)
    if direction == "none" and labels != start_labels:
        # Either way is walked recursively from a label back to the same label; between two
        # labels the pattern is a fixed join, not a walk.
        raise CypherParseError(
            f"CALL {call.name}: relationshipFilter {rel_spec!r} walks either way only from a "
            f"label to the same label ({':'.join(start_labels)}); mark each type out (TYPE>) or "
            "in (<TYPE)"
        )
    variable = call.yields[0][1]
    end = variable if not expand else f"_apoc_end{serial}"
    left, right = {"left": ("<-", "-"), "right": ("-", "->"), "none": ("-", "-")}[direction]
    rel = f"[{':' + '|'.join(types) if types else ''}*{low}..{high}]"
    node = f"({end}{''.join(':' + label for label in labels)})"
    head = f"({start}{''.join(':' + label for label in start_labels)})"
    pattern = parse_pattern(f"{head}{left}{rel}{right}{node}")
    return MatchStep(
        matches=[MatchClause(pattern=pattern, variable=variable if expand else None)],
        where=call.where,
    )


def _where(text: str | None) -> WhereClause | None:
    """The IR's WHERE: the text a statement writes, parsed into its expression tree."""
    return None if text is None else WhereClause(expression=parse_expression(text))


def with_inline_conditions(matches: list[MatchClause], where: str | None) -> str | None:
    """``where`` with each inline property map of ``matches``' patterns as the equality it means
    in a read: ``(n {k: v})`` is ``WHERE n.k = v``, its value written as the statement wrote it,
    a ``$param`` included. A condition on a variable that only an OPTIONAL MATCH binds is moved
    into that match's join as any of its WHERE conditions is, so the map constrains the optional
    pattern and drops no row of the required ones."""
    conditions: list[str] = []
    for clause in matches:
        pattern = (
            clause.pattern.pattern if isinstance(clause.pattern, PathFunction) else clause.pattern
        )
        assert isinstance(pattern, PathPattern)  # Cypher's own patterns: it writes no triple
        for part in [*pattern.nodes, *pattern.rels]:
            for key, value in part.property_text.items():
                conditions.append(f"{part.variable}.{key} = {value}")
    if not conditions:
        return where
    if where is not None:
        conditions.append(f"({where})")
    return " AND ".join(conditions)


#: The clauses an expression of a write clause ends at (REQ-2176).
_CLAUSE_KEYWORDS = frozenset(
    {
        "RETURN",
        "WITH",
        "MATCH",
        "OPTIONAL",
        "UNWIND",
        "CALL",
        "UNION",
        "WHERE",
        "ORDER",
        "SKIP",
        "LIMIT",
        "CREATE",
        "MERGE",
        "SET",
        "REMOVE",
        "DELETE",
        "DETACH",
        "ON",
    }
)


class _Parser:  # REQ-345, REQ-346, REQ-347, REQ-348, REQ-571
    def __init__(
        self,
        tokens: list[Token],
        raw: str,
        comments: list[str] | None = None,
        *,
        writes: bool = False,
    ) -> None:
        self._tokens = tokens
        self._pos = 0
        self._raw = raw
        self._comments: list[str] = comments or []
        self._inline = 0  # anonymous patterns named for their inline property maps
        # REQ-2176: whether write clauses are read (parse_cypher_statement); a write pattern's
        # inline properties are then expressions, and its anonymous nodes stay anonymous.
        self._writes = writes
        self._write_pattern = False

    # --- helpers ---

    def _peek(self) -> Token | None:
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return None

    def _peek_val(self) -> str:
        t = self._peek()
        return t.value.upper() if t else ""

    def _advance(self) -> Token:
        t = self._tokens[self._pos]
        self._pos += 1
        return t

    def _row_count(self, clause: str) -> int:
        """The whole number of rows SKIP or LIMIT takes. REQ-778: anything else -- a fraction, a
        negative, a string, a name -- is the user's mistake, refused naming what was written and
        the fix (a fraction was once cut to its whole part without a word)."""
        t = self._peek()
        written = t.value if t is not None else ""
        if t is not None and t.type == "MINUS":
            self._advance()
            following = self._peek()
            written = "-" + (following.value if following is not None else "")
        elif t is not None and t.type == "NUMBER" and t.value.isdigit():
            return int(self._advance().value)
        raise CypherParseMistake(
            f"cypher.invalid_{clause.lower()}",
            f"{clause} {written} is not a whole number of rows: {clause} takes a count of 0 or "
            f"more, e.g. {clause} 10.",
            clause=clause,
            value=written,
        )

    def _expect(self, ttype: str, value: str | None = None) -> Token:
        t = self._peek()
        if t is None:
            raise CypherParseError(f"Expected {ttype!r} but reached end of input")
        if t.type != ttype:
            raise CypherParseError(
                f"Expected {ttype!r}, got {t.type!r} ({t.value!r}) at pos {t.pos}"
            )
        if value is not None and t.value.upper() != value.upper():
            raise CypherParseError(f"Expected {value!r}, got {t.value!r} at pos {t.pos}")
        return self._advance()

    def _match_keyword(self, *kws: str) -> bool:
        t = self._peek()
        if t and t.type == "IDENT" and t.value.upper() in {k.upper() for k in kws}:
            return True
        return False

    def _consume_keyword(self, kw: str) -> Token:
        t = self._peek()
        if t is None or t.type != "IDENT" or t.value.upper() != kw.upper():
            raise CypherParseError(f"Expected keyword {kw!r}, got {repr(t.value) if t else 'EOF'}")
        return self._advance()

    def _opt_keyword(self, kw: str) -> bool:
        if self._match_keyword(kw):
            self._advance()
            return True
        return False

    # --- parse entry ---

    def parse(self) -> CypherAST:
        pipeline: list = []
        current_matches: list[MatchClause] = []
        current_where: str | None = None  # the WHERE as written, until its match step is built
        return_clause: ReturnClause | None = None
        order_by: list[OrderItem] = []
        skip: int | None = None
        limit: int | None = None
        call_subqueries: list[CallSubquery] = []
        union_parts: list[tuple[CypherAST, bool]] = []

        def _flush_match_step() -> None:
            nonlocal current_matches, current_where
            if current_matches:
                where = with_inline_conditions(current_matches, current_where)
                pipeline.append(MatchStep(matches=list(current_matches), where=_where(where)))
                current_matches = []
                current_where = None

        while self._peek() is not None:
            kw = self._peek_val()

            if kw in ("OPTIONAL", "MATCH") and current_where is not None:
                # A WHERE is the clause's before it: the next MATCH starts a step of its own, so
                # neither WHERE takes the other's place.
                _flush_match_step()

            if kw in ("OPTIONAL",):
                self._advance()
                self._consume_keyword("MATCH")
                # An OPTIONAL MATCH is a step of its own: its inline properties and its WHERE are
                # its condition, apart from the steps before it (REQ-345).
                _flush_match_step()
                current_matches.append(self._parse_match(optional=True))
                # OPTIONAL MATCH a, b: one pattern, matched whole or not at all (REQ-345).
                while (_comma := self._peek()) and _comma.type == "COMMA":
                    self._advance()
                    current_matches.append(self._parse_match(optional=True))

            elif kw == "MATCH":
                self._advance()
                # A MATCH after an OPTIONAL MATCH starts a step: its conditions filter the rows.
                if any(m.optional for m in current_matches):
                    _flush_match_step()
                current_matches.append(self._parse_match(optional=False))
                # MATCH a, b: each comma-separated pattern is a clause of the same step.
                while (_comma := self._peek()) and _comma.type == "COMMA":
                    self._advance()
                    current_matches.append(self._parse_match(optional=False))

            elif kw == "WHERE":
                self._advance()
                current_where = self._parse_where()

            elif kw == "WITH":
                self._advance()
                _flush_match_step()
                pipeline.append(self._parse_with(_in_scope(pipeline)))

            elif kw == "UNWIND":
                self._advance()
                _flush_match_step()
                pipeline.append(self._parse_unwind())

            elif kw == "CALL":
                self._advance()
                _next = self._peek()
                if _next is not None and _next.type == "IDENT":
                    # REQ-2168: a procedure called in the statement, its rows a source.
                    _flush_match_step()
                    pipeline.append(self._parse_procedure_call(pipeline))
                else:
                    call_subqueries.append(self._parse_call_subquery())

            elif kw == "UNION":
                self._advance()
                _flush_match_step()
                is_all = self._opt_keyword("ALL")
                remaining = self._tokens[self._pos :]
                next_ast = _Parser(remaining, self._raw).parse()
                union_parts.append((next_ast, is_all))
                # consume remaining tokens so outer loop exits
                self._pos = len(self._tokens)
                break

            elif kw == "RETURN":
                self._advance()
                _flush_match_step()
                return_clause = self._parse_return(_in_scope(pipeline))

            elif self._writes and kw in _WRITE_KEYWORDS:
                _flush_match_step()
                pipeline.append(self._parse_write_clause(kw))

            elif kw == "ORDER":
                self._advance()
                self._consume_keyword("BY")
                order_by = self._parse_order_by()

            elif kw == "SKIP":
                self._advance()
                skip = self._row_count("SKIP")

            elif kw == "LIMIT":
                self._advance()
                limit = self._row_count("LIMIT")

            else:
                _bad = self._peek()
                raise CypherParseError(
                    # Neo4j's words for a syntax error, which its clients read.
                    f"Invalid input {_bad.value!r} at pos {_bad.pos}"
                    if _bad
                    else "Unexpected end of input"
                )

        # flush any remaining match clauses (e.g. no RETURN yet flushed them)
        _flush_match_step()

        if return_clause is None and not call_subqueries and not self._has_write(pipeline):
            raise CypherParseError("Missing RETURN clause")

        return CypherAST(
            pipeline=pipeline,
            return_clause=return_clause,
            order_by=order_by,
            skip=skip,
            limit=limit,
            call_subqueries=call_subqueries,
            union_parts=union_parts,
            comments=self._comments,
        )

    # --- writes (REQ-2176) ---

    @staticmethod
    def _has_write(pipeline: list) -> bool:
        return any(
            isinstance(s, (CreateClause, SetClause, RemoveClause, DeleteClause, MergeClause))
            for s in pipeline
        )

    def _parse_write_clause(
        self, kw: str
    ) -> CreateClause | SetClause | RemoveClause | DeleteClause | MergeClause:
        start = self._advance().pos
        if kw == "CREATE":
            patterns = [self._parse_write_pattern()]
            while (_c := self._peek()) and _c.type == "COMMA":
                self._advance()
                patterns.append(self._parse_write_pattern())
            return CreateClause(patterns=patterns, start=start)
        if kw == "MERGE":
            merge = MergeClause(pattern=self._parse_write_pattern(), start=start)
            while self._match_keyword("ON"):
                self._advance()
                on_create = self._opt_keyword("CREATE")
                if not on_create:
                    self._consume_keyword("MATCH")
                self._consume_keyword("SET")
                (merge.on_create if on_create else merge.on_match).extend(self._set_items())
            return merge
        if kw == "SET":
            return SetClause(items=self._set_items(), start=start)
        if kw == "REMOVE":
            return RemoveClause(items=self._remove_items(), start=start)
        detach = kw == "DETACH"
        if detach:
            self._consume_keyword("DELETE")
        deleted = [self._write_expression()]
        while (_c := self._peek()) and _c.type == "COMMA":
            self._advance()
            deleted.append(self._write_expression())
        return DeleteClause(expressions=deleted, detach=detach, start=start)

    def _parse_write_pattern(self) -> PathPattern:
        self._write_pattern = True
        try:
            return self._parse_path_pattern()
        finally:
            self._write_pattern = False

    def _write_expression(self, stop_types: frozenset[str] = frozenset({"COMMA"})) -> CypherExpr:
        """The expression up to the next ``stop_types`` token or clause at its own depth."""
        read: list[Token] = []
        depth = 0
        while t := self._peek():
            if t.type in ("LBRACKET", "LPAREN", "LBRACE"):
                depth += 1
            elif t.type in ("RBRACKET", "RPAREN", "RBRACE"):
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and (
                t.type in stop_types or (t.type == "IDENT" and t.value.upper() in _CLAUSE_KEYWORDS)
            ):
                break
            read.append(self._advance())
        if not read:
            _bad = self._peek()
            raise CypherParseError(
                f"Invalid input {_bad.value!r} at pos {_bad.pos}"
                if _bad
                else "Unexpected end of input"
            )
        return parse_expression(self._raw[read[0].pos : read[-1].end])

    def _labels(self) -> list[str]:
        labels: list[str] = []
        while (_t := self._peek()) and _t.type == "COLON":
            self._advance()
            labels.append(self._expect("IDENT").value)
        return labels

    def _set_items(self) -> list[SetItem]:
        items = [self._set_item()]
        while (_c := self._peek()) and _c.type == "COMMA":
            self._advance()
            items.append(self._set_item())
        return items

    def _set_item(self) -> SetItem:
        variable = self._expect("IDENT").value
        t = self._peek()
        if t is not None and t.type == "COLON":
            return SetItem(variable=variable, labels=self._labels())
        if t is not None and t.type == "DOT":
            self._advance()
            prop = self._expect("IDENT").value
            self._expect("EQ")
            return SetItem(variable=variable, property=prop, value=self._write_expression())
        merge = t is not None and t.type == "PLUS"
        if merge:
            self._advance()
        self._expect("EQ")
        return SetItem(variable=variable, value=self._write_expression(), merge=merge)

    def _remove_items(self) -> list[RemoveItem]:
        items: list[RemoveItem] = []
        while True:
            variable = self._expect("IDENT").value
            if (_t := self._peek()) and _t.type == "DOT":
                self._advance()
                items.append(RemoveItem(variable=variable, property=self._expect("IDENT").value))
            else:
                labels = self._labels()
                if not labels:
                    _bad = self._peek()
                    raise CypherParseError(
                        f"Invalid input {_bad.value!r} at pos {_bad.pos}"
                        if _bad
                        else "Unexpected end of input"
                    )
                items.append(RemoveItem(variable=variable, labels=labels))
            if (_c := self._peek()) and _c.type == "COMMA":
                self._advance()
                continue
            return items

    # --- MATCH ---

    def _parse_match(self, optional: bool) -> MatchClause:
        path_var: str | None = None

        # Check for path variable assignment: p = shortestPath(...)
        _tok0 = self._peek()
        if (
            _tok0
            and _tok0.type == "IDENT"
            and self._pos + 1 < len(self._tokens)
            and self._tokens[self._pos + 1].type == "EQ"
        ):
            path_var = self._advance().value
            self._advance()  # consume "="

        # Check for path functions
        if self._match_keyword("shortestPath", "allShortestPaths", "allPaths"):
            func_name = self._advance().value.lower()
            self._expect("LPAREN")
            pattern = self._parse_path_pattern()
            self._expect("RPAREN")
            return MatchClause(
                pattern=PathFunction(func_name=func_name, pattern=pattern),
                variable=path_var,
                optional=optional,
            )

        # Handle p=((a)-[...]-(b)) — outer parens group the entire pattern.
        # Detect: next token is LPAREN and the token after is also LPAREN (node start).
        _tok1 = self._peek()
        has_grouping_paren = (
            path_var is not None
            and _tok1 is not None
            and _tok1.type == "LPAREN"
            and self._pos + 1 < len(self._tokens)
            and self._tokens[self._pos + 1].type == "LPAREN"
        )
        if has_grouping_paren:
            self._advance()  # consume outer (
            pattern = self._parse_path_pattern()
            self._expect("RPAREN")  # consume outer )
        else:
            pattern = self._parse_path_pattern()
        return MatchClause(pattern=pattern, variable=path_var, optional=optional)

    def _parse_path_pattern(self) -> PathPattern:
        nodes: list[NodePattern] = []
        rels: list[RelPattern] = []

        nodes.append(self._parse_node())

        while True:
            t = self._peek()
            if t is None:
                break
            if t.type in ("MINUS", "ARROW_LEFT"):
                rel, next_node = self._parse_rel_and_node()
                rels.append(rel)
                nodes.append(next_node)
            else:
                break

        return PathPattern(nodes=nodes, rels=rels)

    def _parse_node(self) -> NodePattern:
        self._expect("LPAREN")
        variable: str | None = None
        labels: list[str] = []
        props: dict[str, Any] = {}

        t = self._peek()
        if (
            t
            and t.type == "IDENT"
            and t.value.upper()
            not in {
                "WHERE",
                "RETURN",
                "MATCH",
                "WITH",
                "ORDER",
                "SKIP",
                "LIMIT",
                "CALL",
            }
        ):
            # Could be variable or label
            next_t = self._tokens[self._pos + 1] if self._pos + 1 < len(self._tokens) else None
            if next_t and next_t.type in ("COLON", "RPAREN", "LBRACE", "COMMA"):
                variable = self._advance().value

        label_alternation = False
        while (_tn := self._peek()) and _tn.type == "COLON":
            self._advance()
            labels.append(self._expect("IDENT").value)
            # Support Cypher 5 label alternation: (n:A|B)
            while (_tnp := self._peek()) and _tnp.type == "PIPE":
                self._advance()  # consume |
                labels.append(self._expect("IDENT").value)
                label_alternation = True

        text: dict[str, str] = {}
        if (_tnlb := self._peek()) and _tnlb.type == "LBRACE":
            props, text = self._parse_map_literal()
            if variable is None and not self._write_pattern:
                variable = self._inline_variable()

        self._expect("RPAREN")
        return NodePattern(
            variable=variable,
            labels=labels,
            properties=props,
            label_alternation=label_alternation,
            property_text=text,
        )

    def _parse_rel_and_node(self) -> tuple[RelPattern, NodePattern]:
        direction = "right"
        t = self._peek()

        if t and t.type == "ARROW_LEFT":
            # <- rel ->  means left direction for start of rel
            self._advance()
            _tb = self._peek()
            if _tb and _tb.type in ("MINUS", "ARROW_RIGHT"):  # <-- or <--> : no brackets
                self._advance()
                rel = RelPattern(
                    variable=None, types=[], min_hops=None, max_hops=None, direction="none"
                )
                rel.direction = "left" if _tb.type == "MINUS" else "none"
                return rel, self._parse_node()
            rel = self._parse_rel_pattern()
            _tr = self._peek()
            if _tr and _tr.type == "ARROW_RIGHT":
                self._advance()
                direction = "none"  # <-[r]-> bidirectional? treat as none
            else:
                self._expect("MINUS")
                direction = "left"
        else:
            # - or ->
            self._expect("MINUS")
            _tlb = self._peek()
            if _tlb and _tlb.type == "LBRACKET":
                rel = self._parse_rel_pattern()
                _tar = self._peek()
                if _tar and _tar.type == "ARROW_RIGHT":
                    self._advance()
                    direction = "right"
                else:
                    self._expect("MINUS")
                    direction = "none"
            elif (_tar2 := self._peek()) and _tar2.type == "ARROW_RIGHT":
                self._advance()
                rel = RelPattern(
                    variable=None, types=[], min_hops=None, max_hops=None, direction="right"
                )
                direction = "right"
            else:
                # -- means no direction
                self._expect("MINUS")
                rel = RelPattern(
                    variable=None, types=[], min_hops=None, max_hops=None, direction="none"
                )
                direction = "none"

        rel.direction = direction
        node = self._parse_node()
        return rel, node

    def _parse_rel_pattern(self) -> RelPattern:
        self._expect("LBRACKET")
        variable: str | None = None
        types: list[str] = []
        min_hops: int | None = None
        max_hops: int | None = None
        variable_length = False

        t = self._peek()
        if t and t.type == "IDENT":
            variable = self._advance().value

        while (_trc := self._peek()) and _trc.type == "COLON":
            self._advance()
            types.append(self._expect("IDENT").value)
            # REQ-1586: type alternation is written both ways — `:A|B` is the spelling Cypher 5
            # documents and the one a generated pattern uses; `:A|:B` is the older spelling. After a
            # pipe the next type may therefore arrive with or without its own colon.
            while (_trp := self._peek()) and _trp.type == "PIPE":
                self._advance()
                if (_trc2 := self._peek()) and _trc2.type == "COLON":
                    break
                types.append(self._expect("IDENT").value)

        if (_trs := self._peek()) and _trs.type == "STAR":
            variable_length = True
            self._advance()
            if (_trdd := self._peek()) and _trdd.type == "DOTDOT":
                # *..n
                self._advance()
                if not ((_trm2 := self._peek()) and _trm2.type == "NUMBER"):
                    # [*..], unbounded written out: refused as [*] is (REQ-348)
                    raise CypherParseError(
                        "Unbounded variable-length pattern [*] is not allowed. "
                        "Specify a depth limit, e.g. [*..5]."
                    )
                t = self._expect("NUMBER")
                max_hops = int(float(t.value))
                min_hops = 1
            elif (_trn := self._peek()) and _trn.type == "NUMBER":
                n = int(float(self._advance().value))
                if (_trdd2 := self._peek()) and _trdd2.type == "DOTDOT":
                    self._advance()
                    if (_trn2 := self._peek()) and _trn2.type == "NUMBER":
                        max_hops = int(float(self._advance().value))
                    min_hops = n
                else:
                    min_hops = n
                    max_hops = n
            else:
                # unbounded [*] — reject
                raise CypherParseError(
                    "Unbounded variable-length pattern [*] is not allowed. "
                    "Specify a depth limit, e.g. [*..5]."
                )

        props: dict[str, Any] = {}
        text: dict[str, str] = {}
        if (_trm := self._peek()) and _trm.type == "LBRACE":
            if variable_length:
                raise CypherParseError(
                    "an inline property map on a variable-length relationship is not supported: "
                    "state the condition in WHERE"
                )
            props, text = self._parse_map_literal()
            if variable is None and not self._write_pattern:
                variable = self._inline_variable()
        self._expect("RBRACKET")
        return RelPattern(
            variable=variable,
            types=types,
            min_hops=min_hops,
            max_hops=max_hops,
            direction="none",
            variable_length=variable_length,
            properties=props,
            property_text=text,
        )

    def _parse_map_literal(self) -> tuple[dict[str, Any], dict[str, str]]:
        """A ``{key: value, ...}`` map: each value, and its text as the statement writes it."""
        self._expect("LBRACE")
        props: dict[str, Any] = {}
        text: dict[str, str] = {}
        while (_tmp := self._peek()) and _tmp.type != "RBRACE":
            key = self._expect("IDENT").value
            self._expect("COLON")
            written = self._peek()
            if self._write_pattern:
                # REQ-2176: a value a write writes is any expression.
                props[key] = self._write_expression(frozenset({"COMMA", "RBRACE"}))
                if written is not None:
                    text[key] = self._raw[written.pos : self._tokens[self._pos - 1].end]
            else:
                props[key] = self._parse_value()
                if written is not None:
                    text[key] = written.value
            if (_tmpc := self._peek()) and _tmpc.type == "COMMA":
                self._advance()
        self._expect("RBRACE")
        return props, text

    def _inline_variable(self) -> str:
        """A name for an anonymous pattern that carries an inline property map, so its conditions
        can name it. No statement writes it: the tokenizer gives no identifier this prefix."""
        self._inline += 1
        return f"{INLINE_PREFIX}{self._inline}"

    def _parse_value(self) -> Any:
        t = self._peek()
        if t is None:
            raise CypherParseError("Expected value, reached EOF")
        if t.type == "NUMBER":
            self._advance()
            return float(t.value) if "." in t.value else int(t.value)
        if t.type == "STRING":
            self._advance()
            return t.value[1:-1]
        if t.type == "PARAM":
            self._advance()
            return t.value  # keep as $param string
        if t.type == "IDENT" and t.value.upper() in ("TRUE", "FALSE", "NULL"):
            self._advance()
            return {"TRUE": True, "FALSE": False, "NULL": None}[t.value.upper()]
        raise CypherParseError(f"Unexpected value token {t.value!r}")

    # --- WHERE ---

    def _parse_where(self) -> str:
        _clause_kws = {
            "RETURN",
            "WITH",
            "ORDER",
            "SKIP",
            "LIMIT",
            "MATCH",
            "OPTIONAL",
            "CALL",
            "UNWIND",
        }
        if self._writes:
            _clause_kws |= _WRITE_KEYWORDS  # REQ-2176: a write clause ends it too
        # WITH is a stop keyword but must not stop STARTS WITH / ENDS WITH predicates
        _string_pred_prefixes = {"STARTS", "ENDS"}
        parts: list[str] = []
        depth = 0  # track { } depth for EXISTS/COUNT/COLLECT { } subqueries
        while t := self._peek():
            if t.type == "LBRACE":
                depth += 1
            elif t.type == "RBRACE":
                if depth > 0:
                    depth -= 1
                else:
                    break  # unmatched } — stop
            elif depth == 0:
                kw = t.value.upper() if t.type == "IDENT" else ""
                if kw in _clause_kws:
                    if kw == "WITH" and parts and parts[-1].upper() in _string_pred_prefixes:
                        pass  # part of a string predicate — keep consuming
                    else:
                        break
            parts.append(self._advance().value)
        return " ".join(parts)

    # --- WITH ---

    def _parse_with(self, scope: list[str]) -> WithClause:
        """``scope``: the variables in scope before it, which ``WITH *`` carries on."""
        distinct = self._opt_keyword("DISTINCT")
        items = self._star_or_items(scope)
        # Its ORDER BY, SKIP and LIMIT are the WITH's own: they pick the rows it carries on.
        order_by: list[OrderItem] = []
        if self._match_keyword("ORDER"):
            self._advance()
            self._consume_keyword("BY")
            order_by = self._parse_order_by()
        skip: int | None = None
        if self._match_keyword("SKIP"):
            self._advance()
            skip = self._row_count("SKIP")
        limit: int | None = None
        if self._match_keyword("LIMIT"):
            self._advance()
            limit = self._row_count("LIMIT")
        where: WhereClause | None = None
        if self._match_keyword("WHERE"):
            self._advance()
            where = _where(self._parse_where())
        return WithClause(
            items=items, where=where, order_by=order_by, skip=skip, limit=limit, distinct=distinct
        )

    # --- RETURN ---

    def _parse_return(self, scope: list[str]) -> ReturnClause:
        """``scope``: the variables in scope before it, which ``RETURN *`` returns."""
        distinct = self._opt_keyword("DISTINCT")
        return ReturnClause(items=self._star_or_items(scope), distinct=distinct)

    def _star_or_items(self, scope: list[str]) -> list[ReturnItem]:
        """``*`` -- every variable in ``scope``, then any items after a comma -- or the items."""
        if (t := self._peek()) is None or t.type != "STAR":
            return self._parse_return_items()
        self._advance()
        # By name, as openCypher orders the columns RETURN * returns.
        items = [ReturnItem(expression=Variable(name=n), name=n) for n in sorted(scope)]
        if (t := self._peek()) is not None and t.type == "COMMA":
            self._advance()
            items.extend(self._parse_return_items())
        return items

    def _parse_return_items(self) -> list[ReturnItem]:
        items: list[ReturnItem] = []
        _stop_kws = {
            "ORDER",
            "SKIP",
            "LIMIT",
            "WHERE",
            "MATCH",
            "RETURN",
            "WITH",
            "OPTIONAL",
            "UNION",
            "UNWIND",
        }
        if self._writes:
            _stop_kws |= _WRITE_KEYWORDS  # REQ-2176: a write clause ends them too
        while self._peek() and self._peek_val() not in _stop_kws:
            expr_parts: list[str] = []
            read: list[Token] = []
            depth = 0
            while t := self._peek():
                if t.type in ("LBRACKET", "LPAREN", "LBRACE"):
                    depth += 1
                    read.append(self._advance())
                    expr_parts.append(read[-1].value)
                    continue
                if t.type in ("RBRACKET", "RPAREN", "RBRACE"):
                    if depth > 0:
                        depth -= 1
                        read.append(self._advance())
                        expr_parts.append(read[-1].value)
                        continue
                    break
                if depth == 0:
                    # STARTS WITH and ENDS WITH are operators, not a WITH clause.
                    in_operator = (
                        t.value.upper() == "WITH"
                        and bool(read)
                        and read[-1].value.upper() in ("STARTS", "ENDS")
                    )
                    stops = t.type == "COMMA" or t.value.upper() in (_stop_kws | {"AS"})
                    if stops and not in_operator:
                        break
                read.append(self._advance())
                expr_parts.append(read[-1].value)
            expr = " ".join(expr_parts)
            alias: str | None = None
            if self._opt_keyword("AS"):
                alias = self._expect("IDENT").value
            # The item as the statement writes it (min(x), count(a) > 1): an unaliased item's
            # column is named by it, as Cypher names it.
            written = self._raw[read[0].pos : read[-1].end] if read else expr
            items.append(ReturnItem(expression=parse_expression(expr), alias=alias, name=written))
            if (_tri := self._peek()) and _tri.type == "COMMA":
                self._advance()
            else:
                break
        return items

    # --- UNWIND ---

    def _parse_unwind(self) -> "UnwindClause":
        """Parse `UNWIND <expr> AS <var>`."""
        parts: list[str] = []
        depth = 0
        while t := self._peek():
            if t.type in ("LBRACKET", "LPAREN", "LBRACE"):
                depth += 1
                parts.append(self._advance().value)
                continue
            if t.type in ("RBRACKET", "RPAREN", "RBRACE"):
                if depth > 0:
                    depth -= 1
                    parts.append(self._advance().value)
                    continue
                break
            if depth == 0 and t.type == "IDENT" and t.value.upper() == "AS":
                break
            parts.append(self._advance().value)
        expr = " ".join(parts)
        self._consume_keyword("AS")
        variable = self._expect("IDENT").value
        return UnwindClause(expression=parse_expression(expr), variable=variable)

    # --- CALL procedure ---

    def _parse_procedure_call(self, pipeline: list) -> "ProcedureCall | MatchStep":  # REQ-2168
        """Parse ``ns.name(args) YIELD col [AS var], … [WHERE …]`` after CALL. A call composed
        in a statement names what it yields: its rows are a source the rest of it reads."""
        name = [self._expect("IDENT").value]
        while (t := self._peek()) is not None and t.type == "DOT":
            self._advance()
            name.append(self._expect("IDENT").value)
        proc = ".".join(name)
        self._expect("LPAREN")
        args: list[str] = []
        current: list[str] = []
        depth = 0
        while True:
            t = self._peek()
            if t is None:
                raise CypherParseError(f"CALL {proc}: the argument list is not closed")
            if t.type in ("LPAREN", "LBRACKET", "LBRACE"):
                depth += 1
            elif t.type in ("RPAREN", "RBRACKET", "RBRACE"):
                if depth == 0:
                    self._advance()
                    break
                depth -= 1
            elif t.type == "COMMA" and depth == 0:
                self._advance()
                args.append(" ".join(current))
                current = []
                continue
            current.append(self._advance().value)
        if current or args:
            args.append(" ".join(current))
        if not self._opt_keyword("YIELD"):
            raise CypherParseError(
                f"CALL {proc}: name the columns it returns with YIELD: "
                f"CALL {proc}(…) YIELD column [AS variable], …"
            )
        yields: list[tuple[str, str]] = []
        while True:
            if (t := self._peek()) is not None and t.type == "STAR":
                raise CypherParseError(
                    f"CALL {proc} … YIELD * is not read in a larger statement; name the columns"
                )
            column = self._expect("IDENT").value
            variable = self._expect("IDENT").value if self._opt_keyword("AS") else column
            yields.append((column, variable))
            if (t := self._peek()) is None or t.type != "COMMA":
                break
            self._advance()
        where = _where(self._parse_where()) if self._opt_keyword("WHERE") else None
        call = ProcedureCall(
            name=proc,
            args=[parse_expression(a) for a in args],
            yields=yields,
            where=where,
        )
        if proc.lower() in _PATH_PROCEDURES:
            return _path_procedure(call, self._pos, pipeline)
        return call

    # --- CALL subquery ---

    def _parse_call_subquery(self) -> "CallSubquery":
        self._expect("LBRACE")
        inner_tokens: list[Token] = []
        depth = 1
        while (t := self._peek()) is not None:
            if t.type == "LBRACE":
                depth += 1
            elif t.type == "RBRACE":
                depth -= 1
                if depth == 0:
                    self._advance()  # consume closing }
                    break
            inner_tokens.append(self._advance())
        if depth != 0:
            raise CypherParseError("Unterminated CALL { subquery — missing closing '}'")

        # Detect correlated form: first tokens are WITH <ident>, <ident> MATCH ...
        # These bare identifiers are outer-scope variable imports, not a pipeline WITH.
        # Keywords are also tokenized as IDENT, so stop at any known Cypher keyword.
        _CYPHER_KEYWORDS = {
            "MATCH",
            "OPTIONAL",
            "WHERE",
            "RETURN",
            "WITH",
            "UNWIND",
            "ORDER",
            "SKIP",
            "LIMIT",
            "CALL",
            "UNION",
            "CREATE",
            "DELETE",
            "SET",
            "REMOVE",
            "MERGE",
            "FOREACH",
        }
        imported_vars: list[str] = []
        body_tokens = inner_tokens
        if inner_tokens and inner_tokens[0].value.upper() == "WITH":
            i = 1
            while i < len(inner_tokens):
                tok = inner_tokens[i]
                if tok.type == "IDENT" and tok.value.upper() not in _CYPHER_KEYWORDS:
                    imported_vars.append(tok.value)
                    i += 1
                elif tok.type == "COMMA":
                    i += 1
                else:
                    break
            # Only treat as correlated if followed by a statement-starting keyword
            _BODY_START = {"MATCH", "OPTIONAL", "UNWIND", "WITH", "CALL"}
            if (
                imported_vars
                and i < len(inner_tokens)
                and inner_tokens[i].value.upper() in _BODY_START
            ):
                body_tokens = inner_tokens[i:]  # strip the leading WITH <vars>
            else:
                imported_vars = []  # not correlated — reset

        inner_parser = _Parser(body_tokens, self._raw)
        inner_ast = inner_parser.parse()
        return CallSubquery(body=inner_ast, imported_vars=imported_vars)

    # --- ORDER BY ---

    def _parse_order_by(self) -> list[OrderItem]:
        items: list[OrderItem] = []
        _stop_kws = {
            "SKIP",
            "LIMIT",
            "RETURN",
            "MATCH",
            "WITH",
            "WHERE",
            "OPTIONAL",
            "UNWIND",
            "CALL",
            "UNION",
        }
        while self._peek() and self._peek_val() not in _stop_kws:
            expr_parts: list[str] = []
            while (
                (t := self._peek())
                and t.type != "COMMA"
                and self._peek_val() not in (_stop_kws | {"ASC", "DESC"})
            ):
                expr_parts.append(self._advance().value)
            expr = " ".join(expr_parts)
            direction = "ASC"
            if self._match_keyword("ASC", "DESC"):
                direction = self._advance().value.upper()
            items.append(OrderItem(expression=parse_expression(expr), direction=direction))
            if (_tob := self._peek()) and _tob.type == "COMMA":
                self._advance()
            else:
                break
        return items


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _normalize_legacy_params(text: str) -> str:
    """Rewrite legacy {param} syntax to $param."""
    return re.sub(r"\{([A-Za-z_]\w*)\}", r"$\1", text)


def refuse_cypher_definition(query: str) -> None:
    """Raise the pipeline's own refusal for a Cypher statement that defines structure.

    Cypher lowers to SQL and its data writes pass the semantic layer's guard as INSERT / UPDATE /
    DELETE. An index or a constraint has no SQL it lowers to here, so it never reaches that
    guard: it is recognised before lowering and answered with the same refusal, in the same
    words, as a SQL definition (provisa/compiler/definitions.py). A data write is not caught:
    ``CREATE`` is followed there by a pattern, ``(``, not by a word."""
    from provisa.compiler.definitions import refuse_definition_text

    refuse_definition_text(query)


def split_cypher_statements(query: str) -> list[str]:
    """The statements ``query`` holds, each its own text, in order -- split on ``;`` as Neo4j
    Browser's editor splits them, by the tokenizer: a semicolon inside a string or a comment
    separates nothing, and a part holding only comments or blanks is no statement."""
    statements: list[str] = []
    start = 0
    holds = False
    for m in _MASTER_RE.finditer(query):
        kind = m.lastgroup
        if kind == "SEMICOLON":
            if holds:
                statements.append(query[start : m.start()].strip())
            start, holds = m.end(), False
        elif kind not in ("SKIP_WS", "NEWLINE", "COMMENT"):
            holds = True
    if holds:
        statements.append(query[start:].strip())
    return statements


def refuse_multiple_statements(query: str) -> None:
    """Raise for a query that holds more than one statement, in Neo4j's own words.

    A query is one statement, as Neo4j takes it; a client that runs several (Neo4j Browser's
    editor) splits them on ``;`` itself and sends each as its own query
    (:func:`split_cypher_statements`). One that only ends the statement is allowed."""
    statements = len(split_cypher_statements(query))
    if statements > 1:
        raise CypherParseError(f"Expected exactly one statement per query but got: {statements}")


def parse_cypher(  # REQ-345, REQ-346, REQ-571
    query: str, label_map: CypherLabelMap | None = None
) -> CypherAST:
    """Parse a read-only Cypher statement into a CypherAST (``_parse_statement``), refusing a
    variable nothing in it binds or an aggregate where none is taken (REQ-778): the user's mistake, said before any translation. A
    statement inside it (EXISTS { }, COUNT { }, COLLECT { }) reads its outer variables, so it is
    checked as part of the whole, never on its own."""
    ast = _parse_statement(query)
    # With the role's label map, an unbound name that is a bound variable's property is offered.
    mistakes = (
        undefined_variable(ast, label_map),
        variable_misuse(ast),
        aggregate_misuse(ast),
        projection_misuse(ast),
        expression_misuse(ast),
    )
    for mistake in mistakes:
        if mistake is not None:
            raise CypherParseMistake(mistake.code, str(mistake), **mistake.params)
    return ast


def writes_in(query: str) -> bool:
    """Whether the statement holds a write clause's keyword outside a string (REQ-2176): a read
    holds none, and is then never parsed as a write."""
    tokens, _ = _tokenize(_normalize_legacy_params(query))
    return any(t.type == "IDENT" and t.value.upper() in _WRITE_KEYWORDS for t in tokens)


def parse_cypher_statement(  # REQ-2176, REQ-2135
    query: str, label_map: CypherLabelMap | None = None
) -> CypherAST:
    """Parse a Cypher statement, a read or a write, into the graph IR: its write clauses (CREATE,
    MERGE, SET, REMOVE, DELETE) steps of its pipeline. A read parses as parse_cypher parses it."""
    query = _normalize_legacy_params(query)
    tokens, comments = _tokenize(query)
    if not any(t.type == "IDENT" and t.value.upper() in _WRITE_KEYWORDS for t in tokens):
        return parse_cypher(query, label_map)
    refuse_multiple_statements(query)
    refuse_cypher_definition(query)
    try:
        return _Parser(tokens, query, comments, writes=True).parse()
    except CypherExprParseError as exc:
        raise CypherParseError(str(exc)) from exc


def _parse_statement(query: str) -> CypherAST:
    """Parse a read-only Cypher query into a CypherAST.

    Raises CypherParseError for write clauses or unbounded
    variable-length patterns.
    """
    refuse_multiple_statements(query)
    refuse_cypher_definition(query)
    # A write reached the read parser: the write path did not read it. Its refusal says why --
    # the form it is in, or MERGE / DETACH DELETE / REMOVE -- never that writes are unsupported.
    tokens_raw = re.findall(r"\b\w+\b", query)
    written = next((tok.upper() for tok in tokens_raw if tok.upper() in _WRITE_KEYWORDS), None)
    if written is not None:
        from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write

        try:
            parse_cypher_write(query)
        except CypherWriteParseError as exc:
            raise CypherParseError(str(exc)) from exc
        raise CypherParseError(f"{written} is a write: it runs on the write path, not as a read")

    query = _normalize_legacy_params(query)
    tokens, comments = _tokenize(query)
    parser = _Parser(tokens, query, comments)
    try:
        ast = parser.parse()
        return ast
    except CypherExprParseError as exc:
        # An expression the statement holds that is not Cypher: a refusal of the statement,
        # said as such, never a server error.
        raise CypherParseError(str(exc)) from exc


def parse_pattern(text: str) -> PathPattern:
    """A graph pattern written on its own (a pattern comprehension's), as the IR's path pattern."""
    tokens, comments = _tokenize(text)
    parser = _Parser(tokens, text, comments)
    pattern = parser._parse_path_pattern()
    if parser._peek() is not None:
        raise CypherParseError(f"Unexpected {parser._peek_val()!r} after the pattern {text!r}")
    return pattern


def subquery_statement(kind: str, body: str) -> CypherAST:
    """An EXISTS / COUNT / COLLECT body as the statement it is: a body that is a pattern alone
    matches it, and one without a RETURN returns 1 (EXISTS, COUNT) or its whole row (COLLECT)."""
    text = body.strip()
    if text.startswith("("):
        text = "MATCH " + text
    returns_one = kind in ("EXISTS", "COUNT") and "RETURN" not in text.upper()
    if returns_one:
        text = text + " RETURN 1"
    elif kind == "COLLECT" and "RETURN" not in text.upper():
        text = text + " RETURN *"
    ast = _parse_statement(text)
    if returns_one:
        assert ast.return_clause is not None
        ast.return_clause.items[0].name = ""  # a RETURN the statement does not write names nothing
    return ast


def extract_parameters(query: str) -> list[str]:  # REQ-571
    """Return ordered list of $param names from the query string."""
    seen: list[str] = []
    for m in re.finditer(r"\$([A-Za-z_]\w*)", query):
        name = m.group(1)
        if name not in seen:
            seen.append(name)
    return seen
