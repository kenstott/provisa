# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9d41e7-7a35-4e60-b8f2-5d16a3c0e947
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements of the text search family replayed against PostgreSQL 17 (REQ-2167, REQ-2171):
every overload's NULL call, the calls function_support makes of it, and values past those samples."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from itertools import product

#: Each overload's argument types, as pg_proc lists them, OUT arguments left out.
OVERLOADS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("array_to_tsvector", ("text[]",)),
    ("get_current_ts_config", ()),
    ("json_to_tsvector", ("json", "jsonb")),
    ("json_to_tsvector", ("regconfig", "json", "jsonb")),
    ("jsonb_to_tsvector", ("jsonb", "jsonb")),
    ("jsonb_to_tsvector", ("regconfig", "jsonb", "jsonb")),
    ("numnode", ("tsquery",)),
    ("phraseto_tsquery", ("regconfig", "text")),
    ("phraseto_tsquery", ("text",)),
    ("plainto_tsquery", ("regconfig", "text")),
    ("plainto_tsquery", ("text",)),
    ("querytree", ("tsquery",)),
    ("setweight", ("tsvector", '"char"')),
    ("setweight", ("tsvector", '"char"', "text[]")),
    ("to_tsquery", ("regconfig", "text")),
    ("to_tsquery", ("text",)),
    ("to_tsvector", ("json",)),
    ("to_tsvector", ("jsonb",)),
    ("to_tsvector", ("regconfig", "json")),
    ("to_tsvector", ("regconfig", "jsonb")),
    ("to_tsvector", ("regconfig", "text")),
    ("to_tsvector", ("text",)),
    ("ts_debug", ("regconfig", "text")),
    ("ts_debug", ("text",)),
    ("ts_delete", ("tsvector", "text")),
    ("ts_delete", ("tsvector", "text[]")),
    ("ts_filter", ("tsvector", '"char"[]')),
    ("ts_headline", ("jsonb", "tsquery")),
    ("ts_headline", ("jsonb", "tsquery", "text")),
    ("ts_headline", ("json", "tsquery")),
    ("ts_headline", ("json", "tsquery", "text")),
    ("ts_headline", ("regconfig", "jsonb", "tsquery")),
    ("ts_headline", ("regconfig", "jsonb", "tsquery", "text")),
    ("ts_headline", ("regconfig", "json", "tsquery")),
    ("ts_headline", ("regconfig", "json", "tsquery", "text")),
    ("ts_headline", ("regconfig", "text", "tsquery")),
    ("ts_headline", ("regconfig", "text", "tsquery", "text")),
    ("ts_headline", ("text", "tsquery")),
    ("ts_headline", ("text", "tsquery", "text")),
    ("ts_lexize", ("regdictionary", "text")),
    ("ts_match_qv", ("tsquery", "tsvector")),
    ("ts_match_tq", ("text", "tsquery")),
    ("ts_match_tt", ("text", "text")),
    ("ts_match_vq", ("tsvector", "tsquery")),
    ("ts_parse", ("text", "text")),
    ("ts_parse", ("oid", "text")),
    ("ts_rank", ("real[]", "tsvector", "tsquery")),
    ("ts_rank", ("real[]", "tsvector", "tsquery", "integer")),
    ("ts_rank", ("tsvector", "tsquery")),
    ("ts_rank", ("tsvector", "tsquery", "integer")),
    ("ts_rank_cd", ("real[]", "tsvector", "tsquery")),
    ("ts_rank_cd", ("real[]", "tsvector", "tsquery", "integer")),
    ("ts_rank_cd", ("tsvector", "tsquery")),
    ("ts_rank_cd", ("tsvector", "tsquery", "integer")),
    ("ts_rewrite", ("tsquery", "text")),
    ("ts_rewrite", ("tsquery", "tsquery", "tsquery")),
    ("ts_stat", ("text",)),
    ("ts_stat", ("text", "text")),
    ("tsq_mcontained", ("tsquery", "tsquery")),
    ("tsq_mcontains", ("tsquery", "tsquery")),
    ("tsquery_and", ("tsquery", "tsquery")),
    ("tsquery_cmp", ("tsquery", "tsquery")),
    ("tsquery_eq", ("tsquery", "tsquery")),
    ("tsquery_ge", ("tsquery", "tsquery")),
    ("tsquery_gt", ("tsquery", "tsquery")),
    ("tsquery_le", ("tsquery", "tsquery")),
    ("tsquery_lt", ("tsquery", "tsquery")),
    ("tsquery_ne", ("tsquery", "tsquery")),
    ("tsquery_not", ("tsquery",)),
    ("tsquery_or", ("tsquery", "tsquery")),
    ("tsquery_phrase", ("tsquery", "tsquery")),
    ("tsquery_phrase", ("tsquery", "tsquery", "integer")),
    ("tsvector_cmp", ("tsvector", "tsvector")),
    ("tsvector_concat", ("tsvector", "tsvector")),
    ("tsvector_eq", ("tsvector", "tsvector")),
    ("tsvector_ge", ("tsvector", "tsvector")),
    ("tsvector_gt", ("tsvector", "tsvector")),
    ("tsvector_le", ("tsvector", "tsvector")),
    ("tsvector_lt", ("tsvector", "tsvector")),
    ("tsvector_ne", ("tsvector", "tsvector")),
    ("tsvector_to_array", ("tsvector",)),
    ("websearch_to_tsquery", ("regconfig", "text")),
    ("websearch_to_tsquery", ("text",)),
)

_TEXT = ("abc", "Hello World", "12")
#: What function_support calls each type with, in its order.
SAMPLES: dict[str, tuple[str, ...]] = {
    "text": _TEXT,
    "tsvector": ("cat:1 fat:2", "a b c", "rat:3A"),
    "tsquery": ("cat", "a & b", "fat | rat"),
    "regconfig": ("english", "simple", "english"),
    "regdictionary": ("english_stem", "simple", "english_stem"),
    "json": ('{"a": 1, "b": [1, 2]}', '[1, "two", null]', "3"),
    "jsonb": ('{"a": 1, "b": [1, 2]}', '[1, "two", null]', "3"),
    "text[]": ("{a,b,c}", "{}", "{x,NULL,z}"),
    "real[]": ("{0.25,0.5}", "{}", "{-1.5,100}"),
    '"char"': ("a", "B", "1"),
    '"char"[]': ("{a,B}", "{}", "{1}"),
    "oid": ("12345", "0", "1"),
    "integer": ("1", "0", "(-7)"),
}
_NUMBERS = frozenset({"integer", "oid"})
VECTORS = (
    "cat:1 fat:2", "a b c", "rat:3A", "b a", "a:1,2 b:3B,4C", "a:1 a:2", "a a", "", "a:2,1",
    "zebra:5 apple:2A,7", "it''s:1", "'it''''s':1", "'a b':1 c", "é:1 z:2", "cat", "a:1 b:2 c:3",
    "cat:1A fat:2B rat:3C mat:4", "a:1A,1B", "a:0", "a:16384", "1:1 -2:2", "Fat:1 fat:2",
    "'':1", "a:", "a:1,", ":1",
)  # fmt: skip
QUERIES = (
    "cat", "a & b", "fat | rat", "!a", "a & !b", "(a | b) & c", "a | b & c", "a <-> b", "a <2> b",
    "a:A", "a:AB", "a:*", "a:AB*", "", "a & b & c", "a | b | c", "!(a & b)", "!!a", "(a & b) <-> c",
    "a <-> (b | c)", "a <0> b", "a & (b | (c & d))", "'a b'", "'it''s'", "A", "cat & rat", "a <-> b <-> c",
    "(", "a &", "a b",
)  # fmt: skip
#: A word's reading: the default parser's token types, and English's stop words and stems.
TEXTS = (
    "abc", "Hello World", "12", "", " ", "The Fat Rats", "a fat cat sat on a mat", "cats running",
    "The quick brown foxes jumped over the lazy dogs", "the and of", "fat-cat", "foo@example.com",
    "http://example.com/a/b.html", "1.5 -3 2e4", "don't", "Müller café", "a1b2", "hello, world!",
    "one two  three", "<b>bold</b> text", "/usr/local/bin", "example.com", "x" * 3, "multi\nline",
    "generously organization universities flying happily relational conditional sensibly",
    "caresses ponies ties caress cats feed agreed plastered motoring sing conflated troubled sized",
    "hopping tanned falling hissing fizzed failing filing happy sky",
    "generalizations oscillators triplicate formative formalize electricity electrical hopeful",
    "goodness revival allowance inference airliner gyroscopic adjustable defensible irritant",
    "replacement adjustment dependent adoption homologous communism activate angularity homologous",
    "effective bowdlerize probate rate cease controll roll skies dying lying tying news inning",
    "outing canning howe proceed exceed succeed idly gently ugly early only singly",
    "commun gener arsen consign consigned consigning consignment consist consisted consistency",
    "knack knackeries knacks kneel knelt knock knocked knocking knot apology philosophy",
)  # fmt: skip
#: A query's text, for the functions that read one word by word.
SEARCHES = (
    "fat & rat", "fat | rat", "!fat", "fat <-> rat", "fat:A", "fat:*", "The & Fat", "the", "(fat | rat) & cat",
    "cats", "running & foxes", "", "fat &", "'fat rats'", "'the fat'", "a <2> b",
)  # fmt: skip
WEB_SEARCHES = (
    "fat rat", "\"fat rat\"", "fat or rat", "fat -rat", "\"the fat\" or rat", "-fat", "or", "\"\"",
    "signal -\"segmentation fault\"", "fat & rat", "fat:A (rat)",
)  # fmt: skip
DOCUMENTS = (
    '{"a": "The Fat Rats", "b": ["cats", 12, true, null], "c": {"d": "running"}}',
    '["fat", "rat"]', '"plain string"', "3", "true", "null", "{}", "[]",
    '{"k1": "v1", "k2": 2.5, "k3": false}',
)  # fmt: skip
#: json_to_tsvector's filter.
FILTERS = (
    '"all"', '"string"', '"numeric"', '"boolean"', '"key"', '["string", "numeric"]', '["key"]',
    '"bad"', "[]", "3",
)  # fmt: skip
HEADLINE_OPTIONS = ("MaxWords=2, MinWords=1", "ShortWord=1", "", "StartSel=<<, StopSel=>>")
CONFIGS = ("english", "simple")
DICTIONARIES = ("english_stem", "simple", "english_stop")
WEIGHTS = ("a", "B", "c", "d", "A", "1", "x")


def literal(value: str, type_name: str) -> str:
    """``value`` as the argument of a call: the same spelling as function_support's."""
    if type_name == "integer" or value.startswith("("):
        return f"{value}::{type_name}"
    if type_name in _NUMBERS:
        return f"{value}::{type_name}"
    return "'" + value.replace("'", "''") + f"'::{type_name}"


def call(name: str, argtypes: tuple[str, ...], values: tuple[str, ...]) -> str:
    args = ", ".join(literal(v, t) for v, t in zip(values, argtypes))
    return f'SELECT pg_catalog."{name}"({args}) AS v'


def null_call(name: str, argtypes: tuple[str, ...]) -> str:
    args = ", ".join(f"NULL::{t}" for t in argtypes)
    return f'SELECT pg_catalog."{name}"({args}) AS v'


def _extras(name: str, argtypes: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    """Each position's values past the samples."""
    choices: list[tuple[str, ...]] = []
    for position, argtype in enumerate(argtypes):
        match argtype:
            case "tsvector":
                pool = VECTORS if len(argtypes) == 1 or position == 0 else VECTORS[:10]
            case "tsquery":
                pool = QUERIES if len(argtypes) == 1 or position == 0 else QUERIES[:12]
            case "regconfig":
                pool = CONFIGS
            case "regdictionary":
                pool = DICTIONARIES
            case "json" | "jsonb":
                pool = (
                    FILTERS
                    if position == len(argtypes) - 1
                    and "to_tsvector" in name
                    and len(argtypes) > 1
                    and argtypes[-1] == "jsonb"
                    and name != "to_tsvector"
                    else DOCUMENTS
                )  # noqa: E501
            case '"char"':
                pool = WEIGHTS
            case '"char"[]':
                pool = ("{a,B}", "{}", "{1}", "{a}", "{D}", "{a,b,c,d}", "{NULL}", "{ab}")
            case "text[]":
                pool = (
                    "{a,b,c}",
                    "{}",
                    "{x,NULL,z}",
                    "{a}",
                    "{cat,rat}",
                    "{a,a}",
                    '{""}',
                    "{it's}",
                )
            case "real[]":
                pool = (
                    "{0.1,0.2,0.4,1.0}",
                    "{1,1,1,1}",
                    "{0.25,0.5}",
                    "{}",
                    "{-1.5,100}",
                    "{0,0,0,0}",
                )
            case "integer":
                pool = ("0", "1", "2", "4", "8", "16", "32", "(-7)", "3", "16384", "16385")
            case "text":
                pool = {
                    "ts_delete": ("a", "cat", "zzz", ""),
                    "ts_stat": ("SELECT 'a b'::tsvector", "SELECT 1", "not sql"),
                    "ts_rewrite": ("SELECT 'a'::tsquery, 'b'::tsquery", "not sql"),
                    "ts_parse": ("default", "nope"),
                    "to_tsquery": SEARCHES,
                    "plainto_tsquery": TEXTS,
                    "phraseto_tsquery": TEXTS,
                    "websearch_to_tsquery": WEB_SEARCHES,
                }.get(name, TEXTS)
                if name == "ts_stat" and position == 1:
                    pool = ("a", "ab", "")
                if name == "ts_parse" and position == 1:
                    pool = TEXTS
                if name == "ts_headline" and position == len(argtypes) - 1:
                    pool = HEADLINE_OPTIONS
                if name == "ts_match_tt" and position == 1:
                    pool = SEARCHES
            case _:
                pool = SAMPLES[argtype]
        choices.append(pool)
    return tuple(product(*choices))


#: The most statements of one overload past its samples: a product of pools is wide.
_MOST = 48


def statements() -> list[str]:
    """Every statement, once, in a stable order."""
    seen: dict[str, None] = {}
    for name, argtypes in OVERLOADS:
        seen[null_call(name, argtypes)] = None
        if not argtypes:
            continue
        for row in range(3):
            values = tuple(SAMPLES[t][row] for t in argtypes)
            if name == "ts_headline" and len(argtypes) in (3, 4) and argtypes[-1] == "text":
                values = (*values[:-1], HEADLINE_OPTIONS[row])
            seen[call(name, argtypes, values)] = None
        extras = _extras(name, argtypes)
        step = max(1, len(extras) // _MOST)
        for values in extras[::step]:
            seen[call(name, argtypes, values)] = None
    return list(seen)
