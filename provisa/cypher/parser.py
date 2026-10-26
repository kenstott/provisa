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

from provisa.cypher.expr_parser import parse_expression
from provisa.gql.expr_ast import Variable

if TYPE_CHECKING:
    from provisa.gql.label_map import CypherLabelMap

from provisa.gql.mistakes import aggregate_misuse, undefined_variable
from provisa.gql.translator_types import CypherMistake
from provisa.gql.ir import (
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
    ("NUMBER", r"\d+(?:\.\d+)?"),
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


def _tokenize(text: str) -> tuple[list[Token], list[str]]:
    tokens: list[Token] = []
    comments: list[str] = []
    for m in _MASTER_RE.finditer(text):
        kind = m.lastgroup
        # A semicolon ends the one statement a query holds (refuse_multiple_statements).
        if kind in ("SKIP_WS", "NEWLINE", "SEMICOLON"):
            continue
        if kind == "COMMENT":
            comments.append(m.group()[2:].strip())  # strip leading //
            continue
        assert kind is not None
        tokens.append(Token(kind, m.group(), m.start()))
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
    if direction == "none":
        raise CypherParseError(
            f"CALL {call.name}: relationshipFilter {rel_spec!r} walks each type one way: mark it "
            "out (TYPE>) or in (<TYPE)"
        )
    labels = _label_filter(call, label_spec)
    start_labels = _start_labels(call, start, pipeline)
    variable = call.yields[0][1]
    end = variable if not expand else f"_apoc_end{serial}"
    left, right = ("<-", "-") if direction == "left" else ("-", "->")
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


class _Parser:  # REQ-345, REQ-346, REQ-347, REQ-348, REQ-571
    def __init__(self, tokens: list[Token], raw: str, comments: list[str] | None = None) -> None:
        self._tokens = tokens
        self._pos = 0
        self._raw = raw
        self._comments: list[str] = comments or []
        self._inline = 0  # anonymous patterns named for their inline property maps

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
                current_matches.append(self._parse_match(optional=True))

            elif kw == "MATCH":
                self._advance()
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
                return_clause = self._parse_return()

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
                    f"Unexpected token {_bad.value!r} at pos {_bad.pos}"
                    if _bad
                    else "Unexpected end of input"
                )

        # flush any remaining match clauses (e.g. no RETURN yet flushed them)
        _flush_match_step()

        if return_clause is None and not call_subqueries:
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
            if variable is None:
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
                # plain - means no direction
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
            if variable is None:
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
        items: list[ReturnItem] = []
        if (t := self._peek()) is not None and t.type == "STAR":
            self._advance()
            items = [ReturnItem(expression=Variable(name=n), name=n) for n in scope]
            if (t := self._peek()) is not None and t.type == "COMMA":
                self._advance()
                items.extend(self._parse_return_items())
        else:
            items = self._parse_return_items()
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
        return WithClause(items=items, where=where, order_by=order_by, skip=skip, limit=limit)

    # --- RETURN ---

    def _parse_return(self) -> ReturnClause:
        distinct = self._opt_keyword("DISTINCT")
        items = self._parse_return_items()
        return ReturnClause(items=items, distinct=distinct)

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
        while self._peek() and self._peek_val() not in _stop_kws:
            expr_parts: list[str] = []
            depth = 0
            while t := self._peek():
                if t.type in ("LBRACKET", "LPAREN", "LBRACE"):
                    depth += 1
                    expr_parts.append(self._advance().value)
                    continue
                if t.type in ("RBRACKET", "RPAREN", "RBRACE"):
                    if depth > 0:
                        depth -= 1
                        expr_parts.append(self._advance().value)
                        continue
                    break
                if depth == 0:
                    if t.type == "COMMA" or t.value.upper() in (_stop_kws | {"AS"}):
                        break
                expr_parts.append(self._advance().value)
            expr = " ".join(expr_parts)
            alias: str | None = None
            if self._opt_keyword("AS"):
                alias = self._expect("IDENT").value
            items.append(ReturnItem(expression=parse_expression(expr), alias=alias, name=expr))
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
    for mistake in (undefined_variable(ast, label_map), aggregate_misuse(ast)):
        if mistake is not None:
            raise CypherParseMistake(mistake.code, str(mistake), **mistake.params)
    return ast


def _parse_statement(query: str) -> CypherAST:
    """Parse a read-only Cypher query into a CypherAST.

    Raises CypherParseError for write clauses or unbounded
    variable-length patterns.
    """
    refuse_multiple_statements(query)
    refuse_cypher_definition(query)
    # Reject write clauses (case-insensitive)
    tokens_raw = re.findall(r"\b\w+\b", query)
    for tok in tokens_raw:
        if tok.upper() in _WRITE_KEYWORDS:
            raise CypherParseError(
                f"Write clause {tok.upper()!r} is not supported. "
                "Provisa Cypher executes CREATE, DELETE, and SET as direct table "
                "writes (governed like any other write); MERGE, DETACH DELETE, and "
                "REMOVE are not supported."
            )

    query = _normalize_legacy_params(query)
    tokens, comments = _tokenize(query)
    parser = _Parser(tokens, query, comments)
    return parser.parse()


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
    if kind in ("EXISTS", "COUNT") and "RETURN" not in text.upper():
        text = text + " RETURN 1"
    elif kind == "COLLECT" and "RETURN" not in text.upper():
        text = text + " RETURN *"
    return _parse_statement(text)


def extract_parameters(query: str) -> list[str]:  # REQ-571
    """Return ordered list of $param names from the query string."""
    seen: list[str] = []
    for m in re.finditer(r"\$([A-Za-z_]\w*)", query):
        name = m.group(1)
        if name not in seen:
            seen.append(name)
    return seen
