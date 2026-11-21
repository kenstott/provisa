# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b0c57-d1a4-4f86-92c3-7b5e1d08a6f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A document's reading on the engine, row by row (REQ-2167, REQ-2171): the default parser of
provisa.core.pg_tsearch_parse and the english_stem dictionary of provisa.core.pg_tsearch_english,
each step of theirs a step of a form, so a text column has to_tsvector's answer.

The parser is one extraction, leftmost-first, of the shapes in their order. A shape the machine
reads only before a space or the end consumes that character where pg_tsearch_parse looks ahead,
since RE2 has no lookahead; what follows a shape is a blank or nothing, else the text is one the
parser does not read, and the extraction then leaves a character out. So a text is read exactly
when its tokens, joined, are the text; any other is refused by name, row by row."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from provisa.core.pg_error import END, MARK
from provisa.core.pg_tsearch_english import STOP_WORDS, UNSTEMMED_BYTES
from provisa.transpiler.pg_forms_types import REFUSED

if TYPE_CHECKING:
    from provisa.transpiler.pg_forms_textsearch import Engine

#: pg_tsearch_parse._BLANK, its \s each of Python's spaces.
_SPACES = "\\x09-\\x0D\\x1C-\\x20\\x85\\xA0  -     　"
_BLANK = f"[{_SPACES},;:!?'\"()\\[\\]{{}}=+*~^%$#|`]"
#: pg_tsearch_parse._BOUNDARY: what a shape may be followed by, here consumed.
_AFTER = f"(?:{_BLANK}|\\z)"
_DOMAIN = r"[A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z][A-Za-z0-9]*)*\.[A-Za-z]{2,}"
_TAG = r"</?[A-Za-z][A-Za-z0-9]*>"
#: pg_tsearch_parse._SHAPES after the tag, in their order, each with the part of a document it is.
_URL, _HYPHENED, _STEMMED, _PLAIN = "url", "hyphened", "stemmed", "plain"
_SHAPES = (
    (rf"[A-Za-z][A-Za-z0-9]*@{_DOMAIN}", _PLAIN),
    (rf"https?://{_DOMAIN}(?:/[A-Za-z0-9._-]+)+/?", _URL),
    (r"(?:/[A-Za-z0-9_-]+)+", _PLAIN),
    (_DOMAIN, _PLAIN),
    (r"[A-Za-z]+(?:-[A-Za-z]+)+", _HYPHENED),
    (r"[0-9]+[eE][0-9]+", _PLAIN),
    (r"-?[0-9]+\.[0-9]+", _PLAIN),
    (r"-[0-9]+", _PLAIN),
    (r"[0-9]+", _PLAIN),
    (r"[A-Za-z]+[0-9][A-Za-z0-9]*", _PLAIN),
    (r"[A-Za-z]+", _STEMMED),
    (r"[\p{L}\p{Nl}\p{No}]+", _STEMMED),  # [^\W\d_]
)
_TOKEN = "|".join([_TAG, *(f"{shape}{_AFTER}" for shape, _ in _SHAPES), f"{_BLANK}+"])
#: MAXSTRLEN: a longer token is left out of a document, with no position (IGNORE_LONGLEXEME).
_LONGEST = 2047
#: MAXENTRYPOS - 1, the last position, and MAXNUMPOS - 1, the most positions uniqueWORD keeps.
_LAST_POSITION, _MOST_POSITIONS = 16383, 255

_VOWEL, _OTHER = "[aeiouy]", "[^aeiouy]"
_EXCEPTIONS = {
    "skis": "ski", "skies": "sky", "dying": "die", "lying": "lie", "tying": "tie", "idly": "idl",
    "gently": "gentl", "ugly": "ugli", "early": "earli", "only": "onli", "singly": "singl",
    "sky": "sky", "news": "news", "howe": "howe", "atlas": "atlas", "cosmos": "cosmos",
    "bias": "bias", "andes": "andes",
}  # fmt: skip
_AFTER_STEP_1A = (
    "inning",
    "outing",
    "canning",
    "herring",
    "earring",
    "proceed",
    "exceed",
    "succeed",
)
_R1_PREFIXES = ("gener", "commun", "arsen")
_STEP_2 = (
    ("ization", "ize"), ("ational", "ate"), ("fulness", "ful"), ("ousness", "ous"),
    ("iveness", "ive"), ("tional", "tion"), ("biliti", "ble"), ("lessli", "less"),
    ("entli", "ent"), ("ation", "ate"), ("alism", "al"), ("aliti", "al"), ("ousli", "ous"),
    ("iviti", "ive"), ("fulli", "ful"), ("enci", "ence"), ("anci", "ance"), ("abli", "able"),
    ("izer", "ize"), ("ator", "ate"), ("alli", "al"), ("bli", "ble"), ("ogi", "og"), ("li", ""),
)  # fmt: skip
_STEP_3 = (
    ("ational", "ate"), ("tional", "tion"), ("alize", "al"), ("icate", "ic"), ("iciti", "ic"),
    ("ative", ""), ("ical", "ic"), ("ness", ""), ("ful", ""),
)  # fmt: skip
_STEP_4 = (
    "ement", "ance", "ence", "able", "ible", "ment", "ant", "ent", "ism", "ate", "iti", "ous",
    "ive", "ize", "ion", "al", "er", "ic",
)  # fmt: skip
_DOUBLES = ("bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt")


def _in(e: Engine, value: str, items: tuple[str, ...] | frozenset[str]) -> str:
    return f"{value} IN ({', '.join(e.text(i) for i in sorted(items))})"


def _ends(e: Engine, word: str, *suffixes: str) -> str:
    return "(" + " OR ".join(f"{word} LIKE {e.text('%' + s)}" for s in suffixes) + ")"


def _length(e: Engine, word: str) -> str:
    return e.chars.format(word)


def _cut(e: Engine, word: str, count: int) -> str:
    """``word`` less its last ``count`` characters."""
    return e.prefix.format(word, f"greatest({_length(e, word)} - {count}, 0)")


def _rest(e: Engine, word: str, suffix: str) -> str:
    """The length of ``word`` before ``suffix``."""
    return f"({_length(e, word)} - {len(suffix)})"


def _has(e: Engine, word: str, pattern: str) -> str:
    return e.matches.format(word, e.text(pattern))


def _short(e: Engine, word: str) -> str:
    """_short_syllable of the whole of ``word``."""
    return _has(e, word, rf"(?:\A{_VOWEL}{_OTHER}|{_OTHER}{_VOWEL}[^aeiouywxY])\z")


def _after(e: Engine, word: str, start: str) -> str:
    """_regions.after: past the first non-vowel that follows a vowel, from ``start``."""
    run = e.find.format(e.since.format(word, f"{start} + 1"), e.text(rf"\A({_OTHER}*{_VOWEL}*)"))
    return f"least({start} + {_length(e, run)} + 1, {_length(e, word)})"


def _r1(e: Engine, word: str) -> str:
    known = " ".join(f"WHEN {word} LIKE {e.text(p + '%')} THEN {len(p)}" for p in _R1_PREFIXES)
    return f"CASE {known} ELSE {_after(e, word, '0')} END"


def _step_1a(e: Engine, w: str) -> str:
    return (
        f"CASE WHEN {_ends(e, w, 'sses')} THEN {_cut(e, w, 2)} "
        f"WHEN {_ends(e, w, 'ied', 'ies')} "
        f"THEN CASE WHEN {_length(e, w)} > 4 THEN {_cut(e, w, 2)} ELSE {_cut(e, w, 1)} END "
        f"WHEN {_ends(e, w, 'us', 'ss')} THEN {w} "
        f"WHEN {_ends(e, w, 's')} AND {_has(e, _cut(e, w, 2), _VOWEL)} THEN {_cut(e, w, 1)} "
        f"ELSE {w} END"
    )


def _after_1b(e: Engine, x: str, r1: str) -> str:
    return (
        f"CASE WHEN {_ends(e, x, 'at', 'bl', 'iz')} THEN {x} || 'e' "
        f"WHEN {_ends(e, x, *_DOUBLES)} THEN {_cut(e, x, 1)} "
        f"WHEN {_length(e, x)} <= {r1} AND {_short(e, x)} THEN {x} || 'e' "
        f"ELSE {x} END"
    )


def _step_1b(e: Engine, w: str, r1: str) -> str:
    def eed(suffix: str) -> str:
        return (
            f"WHEN {_ends(e, w, suffix)} THEN CASE WHEN {_rest(e, w, suffix)} >= {r1} "
            f"THEN {_cut(e, w, len(suffix))} || 'ee' ELSE {w} END"
        )

    cut = " ".join(f"WHEN {_ends(e, w, s)} THEN {len(s)}" for s in ("ingly", "edly", "ing"))
    stemmed = e.prefix.format(w, f"greatest({_length(e, w)} - CASE {cut} ELSE 2 END, 0)")
    after = f"CASE WHEN NOT {_has(e, '_x', _VOWEL)} THEN {w} ELSE {_after_1b(e, '_x', r1)} END"
    return (
        f"CASE {eed('eedly')} {eed('eed')} "
        f"WHEN {_ends(e, w, 'ingly', 'edly', 'ing', 'ed')} THEN {e.let.format('_x', stemmed, after)} "
        f"ELSE {w} END"
    )


def _step_1c(e: Engine, w: str) -> str:
    last = _has(e, w, rf"{_OTHER}[yY]\z")
    return f"CASE WHEN {_length(e, w)} > 2 AND {last} THEN {_cut(e, w, 1)} || 'i' ELSE {w} END"


def _replaced(e: Engine, w: str, suffix: str, written: str) -> str:
    return f"{_cut(e, w, len(suffix))} || {e.text(written)}"


def _step_2(e: Engine, w: str, r1: str) -> str:
    whens = []
    for suffix, written in _STEP_2:
        held = f"{_rest(e, w, suffix)} < {r1}"
        if suffix == "ogi":
            held += f" OR NOT {_ends(e, w, 'logi')}"
        if suffix == "li":
            held += f" OR NOT {_has(e, w, r'[cdeghkmnrt]li\z')}"
        whens.append(
            f"WHEN {_ends(e, w, suffix)} THEN CASE WHEN {held} THEN {w} "
            f"ELSE {_replaced(e, w, suffix, written)} END"
        )
    return f"CASE {' '.join(whens)} ELSE {w} END"


def _step_3(e: Engine, w: str, r1: str, r2: str) -> str:
    whens = [
        f"WHEN {_ends(e, w, suffix)} THEN CASE WHEN {_rest(e, w, suffix)} < "
        f"{r2 if suffix == 'ative' else r1} THEN {w} ELSE {_replaced(e, w, suffix, written)} END"
        for suffix, written in _STEP_3
    ]
    return f"CASE {' '.join(whens)} ELSE {w} END"


def _step_4(e: Engine, w: str, r2: str) -> str:
    whens = []
    for suffix in _STEP_4:
        held = f"{_rest(e, w, suffix)} < {r2}"
        if suffix == "ion":
            held += f" OR NOT {_has(e, w, r'[st]ion\z')}"
        whens.append(
            f"WHEN {_ends(e, w, suffix)} THEN CASE WHEN {held} THEN {w} "
            f"ELSE {_cut(e, w, len(suffix))} END"
        )
    return f"CASE {' '.join(whens)} ELSE {w} END"


def _step_5(e: Engine, w: str, r1: str, r2: str) -> str:
    rest = f"({_length(e, w)} - 1)"
    shorter = _cut(e, w, 1)
    e_dropped = f"{rest} >= {r2} OR ({rest} >= {r1} AND NOT {_short(e, shorter)})"
    return (
        f"CASE WHEN {_ends(e, w, 'e')} THEN CASE WHEN {e_dropped} THEN {shorter} ELSE {w} END "
        f"WHEN {_ends(e, w, 'l')} THEN CASE WHEN {rest} >= {r2} AND {_ends(e, w, 'll')} "
        f"THEN {shorter} ELSE {w} END ELSE {w} END"
    )


def _marked(e: Engine, w: str) -> str:
    """Each y that begins the word or follows a vowel, as Y."""
    first = e.replace_all.format(w, e.text(r"\Ay"), "'Y'")
    return e.replace_all.format(first, e.text(r"([aeiouy])y"), e.text(f"{e.group}Y"))


def _step_0(e: Engine, w: str) -> str:
    cases = " ".join(f"WHEN {_ends(e, w, s)} THEN {_cut(e, w, len(s))}" for s in ("'s'", "'s", "'"))
    return f"CASE {cases} ELSE {w} END"


def _stage(e: Engine, words: str, step: Callable[[str, str, str], str], done: str = "") -> str:
    """One step of the stemmer over ``words``, each ``[word, R1, R2, whether it is done]``: a
    list a step at a time keeps each lambda one deep, as a nested let per step is not."""
    r1, r2 = e.number.format("_y[2]"), e.number.format("_y[3]")
    # the word bound once: a step names it scores of times
    word = f"CASE WHEN _y[4] = '1' THEN _y[1] ELSE {e.let.format('_w', '_y[1]', step('_w', r1, r2))} END"
    finished = f"CASE WHEN _y[4] = '1' OR {done} THEN '1' ELSE '0' END" if done else "_y[4]"
    return e.map.format(words, "_y", e.array.format(f"{word}, _y[2], _y[3], {finished}"))


def _first(e: Engine, lexeme: str, read: str) -> str:
    """A part ``[text, '1' where english_stem reads it]`` as a word of the stemmer: done already
    where it is not read, a stop word, of two letters at most, or an exception."""
    low = e.lower.format("_x[1]")
    exceptions = " ".join(
        f"WHEN {low} = {e.text(k)} THEN {e.text(v)}" for k, v in _EXCEPTIONS.items()
    )
    unquoted = e.replace_all.format(low, e.text(r"\A'"), "''")
    # dict_snowball's own lexeme: a word of two letters at most or over UNSTEMMED_BYTES, or one
    # whose stem is empty, which only step 0 can make it.
    kept = (
        f"{_length(e, low)} <= 2 OR {e.bytes.format(low)} > {UNSTEMMED_BYTES} "
        f"OR ({_step_0(e, unquoted)}) = ''"
    )
    told = (
        f"_x[2] <> '1' OR {_in(e, low, STOP_WORDS)} OR {kept} OR {_in(e, low, tuple(_EXCEPTIONS))}"
    )
    word = (
        f"CASE WHEN _x[2] <> '1' THEN {lexeme} WHEN {_in(e, low, STOP_WORDS)} THEN '' "
        f"WHEN {kept} THEN {low} {exceptions} ELSE {_marked(e, unquoted)} END"
    )
    return e.map.format(
        read, "_x", e.array.format(f"{word}, '0', '0', CASE WHEN {told} THEN '1' ELSE '0' END")
    )


def lexemes_of(e: Engine, parts: str, english: bool) -> str:
    """Each part ``[text, '1' where english_stem reads it]`` as its lexeme: english_stem's, ''
    for a stop word, else the text in lower case."""
    low = e.lower.format("_x[1]")
    if not english:
        return e.map.format(parts, "_x", low)
    words = _first(e, low, parts)
    regions = e.map.format(
        words,
        "_y",
        e.array.format(
            f"_y[1], {e.digits.format(_r1(e, '_y[1]'))}, "
            f"{e.digits.format(_after(e, '_y[1]', _r1(e, '_y[1]')))}, _y[4]"
        ),
    )
    # step 0 a stage of its own: step 1a names its word a score of times, and each would write it
    words = _stage(e, regions, lambda w, r1, r2: _step_0(e, w))
    words = _stage(e, words, lambda w, r1, r2: _step_1a(e, w))
    words = _stage(e, words, lambda w, r1, r2: w, _in(e, "_y[1]", _AFTER_STEP_1A))
    words = _stage(e, words, lambda w, r1, r2: _step_1b(e, w, r1))
    words = _stage(e, words, lambda w, r1, r2: _step_1c(e, w))
    words = _stage(e, words, lambda w, r1, r2: _step_2(e, w, r1))
    words = _stage(e, words, lambda w, r1, r2: _step_3(e, w, r1, r2))
    words = _stage(e, words, lambda w, r1, r2: _step_4(e, w, r2))
    words = _stage(e, words, lambda w, r1, r2: f"replace({_step_5(e, w, r1, r2)}, 'Y', 'y')")
    return e.map.format(words, "_y", "_y[1]")


def _kind(e: Engine) -> str:
    """The token ``_t``'s shape, the first that is the whole of it: '' for a tag or a blank."""
    kinds = " ".join(
        f"WHEN {_has(e, '_t', rf'\A(?:{shape}){_BLANK}?\z')} THEN {e.text(kind)}"
        for shape, kind in _SHAPES
    )
    return f"CASE WHEN {_has(e, '_t', rf'\A{_TAG}\z')} THEN '' {kinds} ELSE '' END"


def _parts(e: Engine) -> str:
    """The tokens the parser gives of the token ``_u`` of the shape ``_k``, each indexed with a
    position: a URL's address, host and path, a hyphenated word and each of its parts, any other
    word itself."""
    url = e.array.format(
        ", ".join(
            e.find.format("_u", e.text(p))
            for p in (r"://(.*)\z", r"://([^/]*)", r"://[^/]*(/.*)\z")
        )
    )
    hyphened = e.concat.format(e.array.format("_u"), e.split.format("_u", "'-'"))
    # searched: ClickHouse writes CASE _k WHEN as transform, which takes no array
    return (
        f"CASE WHEN _k = '' THEN {e.no_texts} WHEN _k = {e.text(_URL)} THEN {url} "
        f"WHEN _k = {e.text(_HYPHENED)} THEN {hyphened} ELSE {e.array.format('_u')} END"
    )


def _lexemes(e: Engine, config: str) -> str:
    """Each indexed token of ``s`` as its lexeme, '' for a stop word, in order of position."""
    stemmed = f"CASE WHEN _k IN ({e.text(_HYPHENED)}, {e.text(_STEMMED)}) THEN '1' ELSE '0' END"
    each = e.map.format(
        e.keep.format(_parts(e), "_p", f"{e.bytes.format('_p')} < {_LONGEST}"),
        "_p",
        e.array.format(f"_p, {stemmed}"),
    )
    core = e.replace_all.format("_t", e.text(f"{_BLANK}\\z"), "''")
    read = e.let.format("_k", _kind(e), e.let.format("_u", core, each))
    parts = e.flatten.format(e.map.format(e.find_all.format("s", e.text(_TOKEN)), "_t", read))
    return lexemes_of(e, parts, config == "english")


def _numbers(e: Engine, count: str) -> str:
    """The numbers 1 .. ``count``, none for 0: Trino's sequence refuses an empty one."""
    return e.head.format(e.upto.format(f"greatest({count}, 1)"), count)


def _read(e: Engine, function: str, answer: str) -> str:
    """``answer`` of the text ``s`` the parser reads, NULL of NULL, else refused by name."""
    tokens = e.join.format(e.find_all.format("s", e.text(_TOKEN)), "''")
    message = e.text(f"{MARK} 0A000: {function} {REFUSED} {END}")
    guard = e.fails.format(f"coalesce({tokens} <> s, false)", message)
    return f"CASE WHEN s IS NULL THEN NULL ELSE {e.guarded.format(guard, answer)} END"


def _quoted(e: Engine, lexeme: str) -> str:
    backslash = f"replace({lexeme}, {e.text(chr(92))}, {e.text(chr(92) * 2)})"
    return f"'''' || replace({backslash}, '''', '''''') || ''''"


def document(e: Engine, config: str) -> str:
    """to_tsvector of the text ``s``: each lexeme's positions, as make_tsvector writes them."""
    held = e.keep.format(_numbers(e, e.size.format("_x")), "_k", "_x[_k] = _l")
    positions = e.sort.format(
        e.distinct.format(e.map.format(held, "_k", f"least(_k, {_LAST_POSITION})"))
    )
    marks = e.join.format(
        e.map.format(e.head.format(positions, _MOST_POSITIONS), "_k", e.digits.format("_k")), "','"
    )
    entries = e.map.format(
        e.sort.format(e.distinct.format(e.keep.format("_x", "_l", "_l <> ''"))),
        "_l",
        f"{_quoted(e, '_l')} || ':' || {marks}",
    )
    return _read(
        e, "to_tsvector", e.let.format("_x", _lexemes(e, config), e.join.format(entries, "' '"))
    )


def plain_query(e: Engine, config: str) -> str:
    """plainto_tsquery of ``s``: its lexemes, each joined by &."""
    kept = e.keep.format("_x", "_l", "_l <> ''")
    written = e.join.format(e.map.format(kept, "_l", _quoted(e, "_l")), "' & '")
    return _read(e, "plainto_tsquery", e.let.format("_x", _lexemes(e, config), written))


def phrase_query(e: Engine, config: str) -> str:
    """phraseto_tsquery of ``s``: its lexemes, each two apart by the positions between them."""
    at = e.keep.format(_numbers(e, e.size.format("_x")), "_k", "_x[_k] <> ''")
    gap = f"least(_y[_j], {_LAST_POSITION}) - least(_y[_j - 1], {_LAST_POSITION})"
    joint = f"CASE WHEN {gap} = 1 THEN ' <-> ' ELSE ' <' || {e.digits.format(gap)} || '> ' END"
    each = f"CASE WHEN _j = 1 THEN '' ELSE {joint} END || {_quoted(e, '_x[_y[_j]]')}"
    written = e.join.format(e.map.format(_numbers(e, e.size.format("_y")), "_j", each), "''")
    return _read(
        e,
        "phraseto_tsquery",
        e.let.format("_x", _lexemes(e, config), e.let.format("_y", at, written)),
    )


def lexized(e: Engine, dictionary: str) -> str:
    """ts_lexize of the word ``s``: english_stem's, nothing for a stop word, or simple's."""
    part = e.array.format(e.array.format("s, '1'"))
    found = e.first.format(lexemes_of(e, part, dictionary != "simple"))
    rejected = (
        f"s = '' OR {_in(e, e.lower.format('s'), STOP_WORDS)}"
        if dictionary != "simple"
        else "s = ''"
    )
    one = f"CASE WHEN {rejected} THEN {e.no_texts} ELSE {e.array.format(found)} END"
    if e.nullable_lists:
        return f"CASE WHEN s IS NULL THEN NULL ELSE {one} END"
    message = e.text(f"{MARK} 0A000: ts_lexize {REFUSED} {END}")
    return e.guarded_list.format(e.fails.format("s IS NULL", message), one)
