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
from dataclasses import dataclass, field
from typing import Any


class CypherParseError(Exception):
    """Raised for invalid or unsupported Cypher syntax."""


# ---------------------------------------------------------------------------
# AST dataclasses
# ---------------------------------------------------------------------------

@dataclass
class NodePattern:
    variable: str | None
    labels: list[str]
    properties: dict[str, Any]
    label_alternation: bool = False


@dataclass
class RelPattern:
    variable: str | None
    types: list[str]
    min_hops: int | None  # None = exactly 1 unless variable-length
    max_hops: int | None
    direction: str  # "right" | "left" | "none"
    variable_length: bool = False


@dataclass
class PathPattern:
    nodes: list[NodePattern]
    rels: list[RelPattern]


@dataclass
class PathFunction:
    func_name: str  # "shortestPath" | "allShortestPaths"
    pattern: PathPattern


@dataclass
class MatchClause:
    pattern: PathPattern | PathFunction
    variable: str | None  # path variable (p = shortestPath(...))
    optional: bool = False


@dataclass
class WhereClause:
    expression: str  # raw expression text


@dataclass
class ReturnItem:
    expression: str
    alias: str | None = None


@dataclass
class ReturnClause:
    items: list[ReturnItem]
    distinct: bool = False


@dataclass
class WithClause:
    items: list[ReturnItem]
    where: WhereClause | None = None


@dataclass
class UnwindClause:
    expression: str  # raw expression text (list literal, param, or property)
    variable: str    # AS variable name


@dataclass
class MatchStep:
    """One or more MATCH/OPTIONAL MATCH clauses and their associated WHERE."""
    matches: list[MatchClause]
    where: WhereClause | None = None


@dataclass
class CallSubquery:
    body: "CypherAST"
    imported_vars: list[str] = field(default_factory=list)  # vars from outer scope via WITH


@dataclass
class OrderItem:
    expression: str
    direction: str = "ASC"


@dataclass
class CypherAST:
    pipeline: list["MatchStep | WithClause"]
    return_clause: ReturnClause | None
    order_by: list[OrderItem]
    skip: int | None
    limit: int | None
    call_subqueries: list[CallSubquery] = field(default_factory=list)
    # UNION / UNION ALL: list of (sub_ast, is_all)
    union_parts: list[tuple["CypherAST", bool]] = field(default_factory=list)

    @property
    def match_clauses(self) -> list[MatchClause]:
        return [m for s in self.pipeline if isinstance(s, MatchStep) for m in s.matches]

    @property
    def with_clauses(self) -> list[WithClause]:
        return [s for s in self.pipeline if isinstance(s, WithClause)]

    @property
    def where(self) -> WhereClause | None:
        for s in self.pipeline:
            if isinstance(s, MatchStep) and s.where is not None:
                return s.where
        return None


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

_TOKEN_SPEC = [
    ("COMMENT",     r"//[^\n]*"),
    ("STRING",      r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\""),
    ("NUMBER",      r"\d+(?:\.\d+)?"),
    ("PARAM",       r"\$[A-Za-z_]\w*"),
    ("ARROW_RIGHT", r"->"),
    ("ARROW_LEFT",  r"<-"),
    ("DOTDOT",      r"\.\."),
    ("DOT",         r"\."),
    ("COLON",       r":"),
    ("LBRACE",      r"\{"),
    ("RBRACE",      r"\}"),
    ("LPAREN",      r"\("),
    ("RPAREN",      r"\)"),
    ("LBRACKET",    r"\["),
    ("RBRACKET",    r"\]"),
    ("COMMA",       r","),
    ("STAR",        r"\*"),
    ("PLUS",        r"\+"),
    ("MINUS",       r"-"),
    ("SLASH",       r"/"),
    ("PERCENT",     r"%"),
    ("REGEX_MATCH", r"=~"),
    ("EQ",          r"="),
    ("NEQ",         r"<>|!="),
    ("LTE",         r"<="),
    ("GTE",         r">="),
    ("LT",          r"<"),
    ("GT",          r">"),
    ("PIPE",        r"\|"),
    ("IDENT",       r"[A-Za-z_]\w*"),
    ("NEWLINE",     r"\n"),
    ("SKIP_WS",     r"[ \t]+"),
]

_MASTER_RE = re.compile(
    "|".join(f"(?P<{name}>{pat})" for name, pat in _TOKEN_SPEC),
    re.IGNORECASE,
)

_WRITE_KEYWORDS = {"CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE"}
_APOC_RE = re.compile(r"\bapoc\s*\.", re.IGNORECASE)


@dataclass
class Token:
    type: str
    value: str
    pos: int


def _tokenize(text: str) -> list[Token]:
    tokens: list[Token] = []
    for m in _MASTER_RE.finditer(text):
        kind = m.lastgroup
        if kind in ("SKIP_WS", "NEWLINE", "COMMENT"):
            continue
        tokens.append(Token(kind, m.group(), m.start()))
    return tokens


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class _Parser:
    def __init__(self, tokens: list[Token], raw: str) -> None:
        self._tokens = tokens
        self._pos = 0
        self._raw = raw

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

    def _expect(self, ttype: str, value: str | None = None) -> Token:
        t = self._peek()
        if t is None:
            raise CypherParseError(f"Expected {ttype!r} but reached end of input")
        if t.type != ttype:
            raise CypherParseError(f"Expected {ttype!r}, got {t.type!r} ({t.value!r}) at pos {t.pos}")
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
        current_where: WhereClause | None = None
        return_clause: ReturnClause | None = None
        order_by: list[OrderItem] = []
        skip: int | None = None
        limit: int | None = None
        call_subqueries: list[CallSubquery] = []
        union_parts: list[tuple[CypherAST, bool]] = []

        def _flush_match_step() -> None:
            nonlocal current_matches, current_where
            if current_matches:
                pipeline.append(MatchStep(matches=list(current_matches), where=current_where))
                current_matches = []
                current_where = None

        while self._peek() is not None:
            kw = self._peek_val()

            if kw in ("OPTIONAL",):
                self._advance()
                self._consume_keyword("MATCH")
                current_matches.append(self._parse_match(optional=True))

            elif kw == "MATCH":
                self._advance()
                current_matches.append(self._parse_match(optional=False))

            elif kw == "WHERE":
                self._advance()
                current_where = self._parse_where()

            elif kw == "WITH":
                self._advance()
                _flush_match_step()
                pipeline.append(self._parse_with())

            elif kw == "UNWIND":
                self._advance()
                _flush_match_step()
                pipeline.append(self._parse_unwind())

            elif kw == "CALL":
                self._advance()
                call_subqueries.append(self._parse_call_subquery())

            elif kw == "UNION":
                self._advance()
                _flush_match_step()
                is_all = self._opt_keyword("ALL")
                remaining = self._tokens[self._pos:]
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
                t = self._expect("NUMBER")
                skip = int(float(t.value))

            elif kw == "LIMIT":
                self._advance()
                t = self._expect("NUMBER")
                limit = int(float(t.value))

            else:
                raise CypherParseError(f"Unexpected token {self._peek().value!r} at pos {self._peek().pos}")

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
        )

    # --- MATCH ---

    def _parse_match(self, optional: bool) -> MatchClause:
        path_var: str | None = None

        # Check for path variable assignment: p = shortestPath(...)
        if (
            self._peek() and self._peek().type == "IDENT"
            and self._pos + 1 < len(self._tokens)
            and self._tokens[self._pos + 1].type == "EQ"
        ):
            path_var = self._advance().value
            self._advance()  # consume "="

        # Check for path functions
        if self._match_keyword("shortestPath", "allShortestPaths"):
            func_name = self._advance().value.lower()
            self._expect("LPAREN")
            pattern = self._parse_path_pattern()
            self._expect("RPAREN")
            return MatchClause(
                pattern=PathFunction(func_name=func_name, pattern=pattern),
                variable=path_var,
                optional=optional,
            )

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
        if t and t.type == "IDENT" and t.value.upper() not in {
            "WHERE", "RETURN", "MATCH", "WITH", "ORDER", "SKIP", "LIMIT", "CALL",
        }:
            # Could be variable or label
            next_t = self._tokens[self._pos + 1] if self._pos + 1 < len(self._tokens) else None
            if next_t and next_t.type in ("COLON", "RPAREN", "LBRACE", "COMMA"):
                variable = self._advance().value

        label_alternation = False
        while self._peek() and self._peek().type == "COLON":
            self._advance()
            labels.append(self._expect("IDENT").value)
            # Support Cypher 5 label alternation: (n:A|B)
            while self._peek() and self._peek().type == "PIPE":
                self._advance()  # consume |
                labels.append(self._expect("IDENT").value)
                label_alternation = True

        if self._peek() and self._peek().type == "LBRACE":
            props = self._parse_map_literal()

        self._expect("RPAREN")
        return NodePattern(variable=variable, labels=labels, properties=props,
                           label_alternation=label_alternation)

    def _parse_rel_and_node(self) -> tuple[RelPattern, NodePattern]:
        direction = "right"
        t = self._peek()

        if t and t.type == "ARROW_LEFT":
            # <- rel ->  means left direction for start of rel
            self._advance()
            rel = self._parse_rel_pattern()
            if self._peek() and self._peek().type == "ARROW_RIGHT":
                self._advance()
                direction = "none"  # <-[r]-> bidirectional? treat as none
            else:
                self._expect("MINUS")
                direction = "left"
        else:
            # - or ->
            self._expect("MINUS")
            if self._peek() and self._peek().type == "LBRACKET":
                rel = self._parse_rel_pattern()
                if self._peek() and self._peek().type == "ARROW_RIGHT":
                    self._advance()
                    direction = "right"
                else:
                    self._expect("MINUS")
                    direction = "none"
            elif self._peek() and self._peek().type == "ARROW_RIGHT":
                self._advance()
                rel = RelPattern(variable=None, types=[], min_hops=None, max_hops=None, direction="right")
                direction = "right"
            else:
                # plain - means no direction
                rel = RelPattern(variable=None, types=[], min_hops=None, max_hops=None, direction="none")
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

        while self._peek() and self._peek().type == "COLON":
            self._advance()
            types.append(self._expect("IDENT").value)
            if self._peek() and self._peek().type == "PIPE":
                self._advance()

        if self._peek() and self._peek().type == "STAR":
            variable_length = True
            self._advance()
            if self._peek() and self._peek().type == "DOTDOT":
                # *..n
                self._advance()
                t = self._expect("NUMBER")
                max_hops = int(float(t.value))
                min_hops = 1
            elif self._peek() and self._peek().type == "NUMBER":
                n = int(float(self._advance().value))
                if self._peek() and self._peek().type == "DOTDOT":
                    self._advance()
                    if self._peek() and self._peek().type == "NUMBER":
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

        self._expect("RBRACKET")
        return RelPattern(
            variable=variable,
            types=types,
            min_hops=min_hops,
            max_hops=max_hops,
            direction="none",
            variable_length=variable_length,
        )

    def _parse_map_literal(self) -> dict[str, Any]:
        self._expect("LBRACE")
        props: dict[str, Any] = {}
        while self._peek() and self._peek().type != "RBRACE":
            key = self._expect("IDENT").value
            self._expect("COLON")
            props[key] = self._parse_value()
            if self._peek() and self._peek().type == "COMMA":
                self._advance()
        self._expect("RBRACE")
        return props

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

    def _parse_where(self) -> WhereClause:
        _clause_kws = {"RETURN", "WITH", "ORDER", "SKIP", "LIMIT", "MATCH", "OPTIONAL", "CALL", "UNWIND"}
        # WITH is a stop keyword but must not stop STARTS WITH / ENDS WITH predicates
        _string_pred_prefixes = {"STARTS", "ENDS"}
        parts: list[str] = []
        depth = 0  # track { } depth for EXISTS/COUNT/COLLECT { } subqueries
        while self._peek():
            t = self._peek()
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
        return WhereClause(expression=" ".join(parts))

    # --- WITH ---

    def _parse_with(self) -> WithClause:
        items = self._parse_return_items()
        where: WhereClause | None = None
        if self._match_keyword("WHERE"):
            self._advance()
            where = self._parse_where()
        return WithClause(items=items, where=where)

    # --- RETURN ---

    def _parse_return(self) -> ReturnClause:
        distinct = self._opt_keyword("DISTINCT")
        items = self._parse_return_items()
        return ReturnClause(items=items, distinct=distinct)

    def _parse_return_items(self) -> list[ReturnItem]:
        items: list[ReturnItem] = []
        _stop_kws = {"ORDER", "SKIP", "LIMIT", "WHERE", "MATCH", "RETURN", "WITH", "OPTIONAL", "UNION", "UNWIND"}
        while self._peek() and self._peek_val() not in _stop_kws:
            expr_parts: list[str] = []
            depth = 0
            while self._peek():
                t = self._peek()
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
            items.append(ReturnItem(expression=expr, alias=alias))
            if self._peek() and self._peek().type == "COMMA":
                self._advance()
            else:
                break
        return items

    # --- UNWIND ---

    def _parse_unwind(self) -> "UnwindClause":
        """Parse `UNWIND <expr> AS <var>`."""
        parts: list[str] = []
        depth = 0
        while self._peek():
            t = self._peek()
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
        return UnwindClause(expression=expr, variable=variable)

    # --- CALL subquery ---

    def _parse_call_subquery(self) -> "CallSubquery":
        self._expect("LBRACE")
        inner_tokens: list[Token] = []
        depth = 1
        while self._peek() is not None:
            t = self._peek()
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
            "MATCH", "OPTIONAL", "WHERE", "RETURN", "WITH", "UNWIND",
            "ORDER", "SKIP", "LIMIT", "CALL", "UNION", "CREATE", "DELETE",
            "SET", "REMOVE", "MERGE", "FOREACH",
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
            # Only treat as correlated if followed immediately by MATCH
            if imported_vars and i < len(inner_tokens) and inner_tokens[i].value.upper() == "MATCH":
                body_tokens = inner_tokens[i:]  # strip the leading WITH <vars>
            else:
                imported_vars = []  # not correlated — reset

        inner_parser = _Parser(body_tokens, self._raw)
        inner_ast = inner_parser.parse()
        return CallSubquery(body=inner_ast, imported_vars=imported_vars)

    # --- ORDER BY ---

    def _parse_order_by(self) -> list[OrderItem]:
        items: list[OrderItem] = []
        _stop_kws = {"SKIP", "LIMIT", "RETURN", "MATCH", "WITH"}
        while self._peek() and self._peek_val() not in _stop_kws:
            expr_parts: list[str] = []
            while (
                self._peek()
                and self._peek_val() not in (_stop_kws | {"COMMA", "ASC", "DESC"})
            ):
                expr_parts.append(self._advance().value)
            expr = " ".join(expr_parts)
            direction = "ASC"
            if self._match_keyword("ASC", "DESC"):
                direction = self._advance().value.upper()
            items.append(OrderItem(expression=expr, direction=direction))
            if self._peek() and self._peek().type == "COMMA":
                self._advance()
            else:
                break
        return items


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def _normalize_legacy_params(text: str) -> str:
    """Rewrite legacy {param} syntax to $param."""
    return re.sub(r'\{([A-Za-z_]\w*)\}', r'$\1', text)


def parse_cypher(query: str) -> CypherAST:
    """Parse a read-only Cypher query into a CypherAST.

    Raises CypherParseError for write clauses, APOC references, or unbounded
    variable-length patterns.
    """
    # Reject write clauses (case-insensitive)
    tokens_raw = re.findall(r"\b\w+\b", query)
    for tok in tokens_raw:
        if tok.upper() in _WRITE_KEYWORDS:
            raise CypherParseError(
                f"Write clause {tok.upper()!r} is not allowed. "
                "Provisa Cypher supports read-only queries only."
            )

    # Reject APOC references
    if _APOC_RE.search(query):
        raise CypherParseError(
            "APOC procedure references are not allowed in Provisa Cypher queries."
        )

    query = _normalize_legacy_params(query)
    tokens = _tokenize(query)
    parser = _Parser(tokens, query)
    return parser.parse()


def extract_parameters(query: str) -> list[str]:
    """Return ordered list of $param names from the query string."""
    seen: list[str] = []
    for m in re.finditer(r"\$([A-Za-z_]\w*)", query):
        name = m.group(1)
        if name not in seen:
            seen.append(name)
    return seen
