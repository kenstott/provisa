# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pyparsing grammar that parses a Cypher expression string into a ``CypherExpr`` AST (REQ-913).

This replaces the regex text-rewrite pipeline in the translator: expressions are parsed once into an
AST, which a visitor then lowers node-to-node into ``sqlglot.exp`` — no SQL text is ever string-edited.
pyparsing is already a project dependency; ``infix_notation`` gives correct operator precedence and
associativity for free (operators listed tightest-binding first).
"""

from __future__ import annotations

from typing import Any, cast

import pyparsing as pp

from provisa.cypher.expr_ast import (
    Binary,
    Case,
    CypherExpr,
    FunctionCall,
    Index,
    IsNull,
    LabelPredicate,
    ListComprehension,
    ListLiteral,
    Literal,
    MapLiteral,
    MapProjection,
    Paren,
    Parameter,
    PatternComprehension,
    Property,
    Quantifier,
    Reduce,
    Slice,
    SubqueryExpr,
    Unary,
    Variable,
)

pp.ParserElement.enable_packrat()


class CypherExprParseError(Exception):
    """Raised when an expression fragment cannot be parsed into a CypherExpr."""


def _val(x: Any) -> Any:
    """Unwrap a single-element ``ParseResults`` to the element (pyparsing wraps named results)."""
    if isinstance(x, pp.ParseResults):
        return x[0] if len(x) == 1 else x
    return x


def _e(x: Any) -> CypherExpr:
    """Typed boundary: an unwrapped pyparsing token that is known to be a CypherExpr node."""
    return cast(CypherExpr, _val(x))


def _opt_e(x: Any) -> CypherExpr | None:
    v = _val(x)
    return cast("CypherExpr | None", v) if v is not None else None


# --- lexical atoms -------------------------------------------------------- #

_LPAR, _RPAR = pp.Suppress("("), pp.Suppress(")")
_LBRK, _RBRK = pp.Suppress("["), pp.Suppress("]")
_LBRC, _RBRC = pp.Suppress("{"), pp.Suppress("}")

# Keywords that may not be read as identifiers/function names.
_KW = pp.MatchFirst(
    pp.CaselessKeyword(w)
    for w in (
        "AND",
        "OR",
        "XOR",
        "NOT",
        "IN",
        "IS",
        "NULL",
        "TRUE",
        "FALSE",
        "STARTS",
        "ENDS",
        "CONTAINS",
        "WITH",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "DISTINCT",
        "WHERE",
        "EXISTS",
        "COUNT",
        "COLLECT",
    )
)

_ident = ~_KW + pp.Word(pp.alphas + "_", pp.alphanums + "_")
_ident_any = pp.Word(pp.alphas + "_", pp.alphanums + "_")  # allows keyword-like fn names


def _mk_number(t: pp.ParseResults) -> Literal:
    s = str(t[0])
    return Literal(float(s) if "." in s else int(s), "number")


_number = pp.Regex(r"\d+\.\d+|\d+").set_parse_action(_mk_number)
_sstring = pp.QuotedString("'", esc_char="\\", unquote_results=True)
_dstring = pp.QuotedString('"', esc_char="\\", unquote_results=True)
_string = (_sstring | _dstring).set_parse_action(lambda t: Literal(t[0], "string"))
_true = pp.CaselessKeyword("TRUE").set_parse_action(lambda: Literal(True, "boolean"))
_false = pp.CaselessKeyword("FALSE").set_parse_action(lambda: Literal(False, "boolean"))
_null = pp.CaselessKeyword("NULL").set_parse_action(lambda: Literal(None, "null"))
_param = pp.Combine(
    pp.Suppress("$") + pp.Word(pp.alphas + "_", pp.alphanums + "_")
).set_parse_action(lambda t: Parameter(str(t[0])))
_variable = _ident.copy().set_parse_action(lambda t: Variable(str(t[0])))


def _build_grammar() -> pp.ParserElement:
    expr = pp.Forward()

    # -- primary atoms ----------------------------------------------------- #

    # function call: name( [DISTINCT] (args | *) )
    _star = pp.Literal("*")
    arg_list = pp.Optional(pp.DelimitedList(expr))
    func_call = (
        _ident_any("name")
        + _LPAR
        + pp.Group(pp.Optional(pp.CaselessKeyword("DISTINCT"))("distinct") + (_star | arg_list))(
            "body"
        )
        + _RPAR
    )

    def _mk_func(t):
        body = list(t["body"])
        distinct = bool(body and str(body[0]).upper() == "DISTINCT")
        if distinct:
            body = body[1:]
        star = len(body) == 1 and body[0] == "*"
        args = [] if star else list(body)
        return FunctionCall(name=t["name"], args=args, distinct=distinct, star=star)

    func_call.set_parse_action(_mk_func)

    # quantifier: all/any/none/single( var IN source [WHERE predicate] )
    quant = (
        (
            pp.CaselessKeyword("ALL")
            | pp.CaselessKeyword("ANY")
            | pp.CaselessKeyword("NONE")
            | pp.CaselessKeyword("SINGLE")
        )("kind")
        + _LPAR
        + _ident("var")
        + pp.CaselessKeyword("IN").suppress()
        + expr("source")
        + pp.Optional(pp.CaselessKeyword("WHERE").suppress() + expr("pred"))
        + _RPAR
    )

    def _mk_quant(t):
        return Quantifier(
            kind=str(t["kind"]).upper(),
            var=str(_val(t["var"])),
            source=_e(t["source"]),
            predicate=_opt_e(t.get("pred")),
        )

    quant.set_parse_action(_mk_quant)

    # reduce( acc = init, var IN source | step )
    reduce_expr = (
        pp.CaselessKeyword("REDUCE").suppress()
        + _LPAR
        + _ident("acc")
        + pp.Suppress("=")
        + expr("init")
        + pp.Suppress(",")
        + _ident("var")
        + pp.CaselessKeyword("IN").suppress()
        + expr("source")
        + pp.Suppress("|")
        + expr("step")
        + _RPAR
    )

    def _mk_reduce(t):
        return Reduce(
            accumulator=str(_val(t["acc"])),
            init=_e(t["init"]),
            var=str(_val(t["var"])),
            source=_e(t["source"]),
            step=_e(t["step"]),
        )

    reduce_expr.set_parse_action(_mk_reduce)

    # CASE [subject] (WHEN cond THEN result)+ [ELSE default] END
    when = pp.Group(
        pp.CaselessKeyword("WHEN").suppress() + expr + pp.CaselessKeyword("THEN").suppress() + expr
    )
    case_expr = (
        pp.CaselessKeyword("CASE").suppress()
        + pp.Optional(~pp.CaselessKeyword("WHEN") + expr)("subject")
        + pp.Group(pp.OneOrMore(when))("whens")
        + pp.Optional(pp.CaselessKeyword("ELSE").suppress() + expr)("default")
        + pp.CaselessKeyword("END").suppress()
    )

    def _mk_case(t):
        whens = [(w[0], w[1]) for w in t["whens"]]
        return Case(subject=_val(t.get("subject")), whens=whens, default=_val(t.get("default")))

    case_expr.set_parse_action(_mk_case)

    # subquery expression: EXISTS/COUNT/COLLECT { ...balanced... }
    subq_body = pp.original_text_for(pp.nested_expr("{", "}"))
    subq = (
        pp.CaselessKeyword("EXISTS") | pp.CaselessKeyword("COUNT") | pp.CaselessKeyword("COLLECT")
    )("kind") + subq_body("body")

    def _mk_subq(t):
        raw = t["body"].strip()
        inner = raw[1:-1].strip() if raw.startswith("{") else raw  # drop the outer braces
        return SubqueryExpr(kind=t["kind"].upper(), body=inner)

    subq.set_parse_action(_mk_subq)

    # list comprehension: [ var IN source [WHERE pred] [| projection] ]
    list_comp = (
        _LBRK
        + _ident("var")
        + pp.CaselessKeyword("IN").suppress()
        + expr("source")
        + pp.Optional(pp.CaselessKeyword("WHERE").suppress() + expr("pred"))
        + pp.Optional(pp.Suppress("|") + expr("proj"))
        + _RBRK
    )

    def _mk_list_comp(t):
        return ListComprehension(
            var=str(_val(t["var"])),
            source=_val(t["source"]),
            predicate=_val(t.get("pred")),
            projection=_val(t.get("proj")),
        )

    list_comp.set_parse_action(_mk_list_comp)

    # pattern comprehension: [ <pattern> [WHERE pred] | projection ]
    # The graph pattern is captured as raw text (up to WHERE or |) for the path translator; SkipTo
    # stops at the first top-level WHERE/| so the predicate is never swallowed into the pattern.
    pattern_text = pp.SkipTo(pp.CaselessKeyword("WHERE") | pp.Literal("|"))
    pattern_comp = (
        _LBRK
        + pp.Optional(_ident("pathvar") + pp.Suppress("="))  # optional path binding: [p = (…) | …]
        + pp.FollowedBy(pp.Literal("("))
        + pattern_text("pattern")
        + pp.Optional(pp.CaselessKeyword("WHERE").suppress() + expr("pred"))
        + pp.Suppress("|")
        + expr("proj")
        + _RBRK
    )

    def _mk_pattern_comp(t):
        return PatternComprehension(
            pattern=t["pattern"].strip(),
            path_var=str(_val(t["pathvar"])) if "pathvar" in t else None,
            predicate=t["pred"] if "pred" in t else None,
            projection=t["proj"],
        )

    pattern_comp.set_parse_action(_mk_pattern_comp)

    # list literal: [ expr, expr, ... ]
    list_lit = (_LBRK + pp.Optional(pp.DelimitedList(expr)) + _RBRK).set_parse_action(
        lambda t: ListLiteral([_e(x) for x in t])
    )

    # map literal: { key: value, ... }
    map_entry = pp.Group((_ident | _sstring) + pp.Suppress(":") + expr)
    map_lit = (_LBRC + pp.Optional(pp.DelimitedList(map_entry)) + _RBRC).set_parse_action(
        lambda t: MapLiteral([(str(e[0]), _e(e[1])) for e in t])
    )

    paren = (_LPAR + expr + _RPAR).set_parse_action(lambda t: Paren(inner=_e(t[0])))

    atom = (
        _number
        | _string
        | _true
        | _false
        | _null
        | _param
        | case_expr
        | subq
        | quant
        | reduce_expr
        | func_call
        | list_comp
        | pattern_comp
        | list_lit
        | map_lit
        | paren
        | _variable
    )

    # -- postfix chain: property .name, index/slice [..], map projection {..}, label :L -- #

    prop_suffix = pp.Group(pp.Suppress(".") + (_ident_any | _sstring))("prop")
    slice_suffix = pp.Group(
        _LBRK + pp.Optional(expr)("start") + pp.Suppress("..") + pp.Optional(expr)("stop") + _RBRK
    )("slice")
    index_suffix = pp.Group(_LBRK + expr("idx") + _RBRK)("index")
    label_suffix = pp.Group(pp.Suppress(":") + pp.DelimitedList(_ident_any, delim=":"))("label")
    mapproj_suffix = pp.Group(
        _LBRC
        + pp.DelimitedList(
            pp.Group(pp.Suppress(".") + _star)  # .*
            | pp.Group(pp.Suppress(".") + _ident_any)  # .prop
            | pp.Group(_ident + pp.Suppress(":") + expr)  # alias: expr
        )
        + _RBRC
    )("mapproj")

    primary = pp.Group(
        atom("atom")
        + pp.ZeroOrMore(mapproj_suffix | prop_suffix | slice_suffix | index_suffix | label_suffix)(
            "suffixes"
        )
    )

    def _mk_primary(t):
        g = t[0]
        node: CypherExpr = _e(g["atom"])  # MatchFirst nesting may wrap the atom — _e unwraps it
        for suf in g.get("suffixes", []):
            name = suf.get_name()
            if name == "prop":
                node = Property(obj=node, name=str(suf[0]))
            elif name == "slice":
                node = Slice(obj=node, start=_opt_e(suf.get("start")), stop=_opt_e(suf.get("stop")))
            elif name == "index":
                node = Index(obj=node, index=_e(suf["idx"]))
            elif name == "label":
                node = LabelPredicate(operand=node, labels=[str(x) for x in suf])
            elif name == "mapproj":
                node = _mk_map_projection(node, suf)
        return node

    primary.set_parse_action(_mk_primary)

    # -- operator precedence (tightest first) ------------------------------ #

    def _fold_binary(t):
        seq = list(t[0])
        node = seq[0]
        i = 1
        while i < len(seq):
            node = Binary(op=str(seq[i]).upper(), left=node, right=seq[i + 1])
            i += 2
        return node

    def _fold_unary(t):
        op, operand = t[0][0], t[0][1]
        return Unary(op=str(op).upper(), operand=operand)

    starts_with = (pp.CaselessKeyword("STARTS") + pp.CaselessKeyword("WITH")).set_parse_action(
        lambda: "STARTS WITH"
    )
    ends_with = (pp.CaselessKeyword("ENDS") + pp.CaselessKeyword("WITH")).set_parse_action(
        lambda: "ENDS WITH"
    )
    cmp_op = (
        pp.one_of("<> != <= >= = < > =~")
        | starts_with
        | ends_with
        | pp.CaselessKeyword("CONTAINS")
        | pp.CaselessKeyword("IN")
    )

    # A postfix predicate bound directly onto the primary: IS [NOT] NULL, or the labeled-node test
    # IS [NOT] :Label (the verbose form of `n:Label`; labels are Provisa's fixed domain/object types).
    is_label = pp.Suppress(":") + pp.DelimitedList(_ident_any, delim=":")
    is_suffix = pp.Group(
        pp.CaselessKeyword("IS").suppress()
        + pp.Optional(pp.CaselessKeyword("NOT"))("neg")
        + (pp.CaselessKeyword("NULL")("isnull") | is_label("labels"))
    )("issuffix")
    predicate = primary + pp.Optional(is_suffix)

    def _mk_predicate(t):
        base = _e(t[0])
        if "issuffix" not in t:
            return base
        suf = t["issuffix"]
        negated = "neg" in suf
        if "isnull" in suf:
            return IsNull(operand=base, negated=negated)
        node: CypherExpr = LabelPredicate(operand=base, labels=[str(x) for x in suf["labels"]])
        return Unary(op="NOT", operand=node) if negated else node

    predicate.set_parse_action(_mk_predicate)

    expr <<= pp.infix_notation(
        predicate,
        [
            (pp.Literal("-"), 1, pp.OpAssoc.RIGHT, _fold_unary),
            (pp.Literal("^"), 2, pp.OpAssoc.LEFT, _fold_binary),  # power (BNF: arithmetic factor)
            (pp.one_of("* / %"), 2, pp.OpAssoc.LEFT, _fold_binary),
            (pp.one_of("+ -"), 2, pp.OpAssoc.LEFT, _fold_binary),
            (cmp_op, 2, pp.OpAssoc.LEFT, _fold_binary),
            (pp.CaselessKeyword("NOT"), 1, pp.OpAssoc.RIGHT, _fold_unary),
            (pp.CaselessKeyword("AND"), 2, pp.OpAssoc.LEFT, _fold_binary),
            (pp.CaselessKeyword("XOR"), 2, pp.OpAssoc.LEFT, _fold_binary),
            (pp.CaselessKeyword("OR"), 2, pp.OpAssoc.LEFT, _fold_binary),
        ],
    )
    return expr


def _mk_map_projection(node: CypherExpr, suf) -> MapProjection:
    if not isinstance(node, Variable):
        raise CypherExprParseError("map projection must apply to a variable")
    proj = MapProjection(var=node.name)
    for entry in suf:
        items = list(entry)
        if len(items) == 1 and items[0] == "*":
            proj.all_props = True
        elif len(items) == 1:
            proj.properties.append(str(items[0]))
        else:
            proj.literal_entries.append((str(items[0]), items[1]))
    return proj


_GRAMMAR = _build_grammar()


def parse_expression(text: str) -> CypherExpr:
    """Parse a Cypher expression fragment into a ``CypherExpr`` AST.

    Raises ``CypherExprParseError`` on malformed input — the caller decides whether to fall back.
    """
    try:
        result = _GRAMMAR.parse_string(text, parse_all=True)
    except pp.ParseBaseException as exc:  # noqa: BLE001 — normalize to our error type
        raise CypherExprParseError(f"cannot parse Cypher expression {text!r}: {exc}") from exc
    return _e(result[0])
