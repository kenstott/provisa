# Copyright (c) 2026 Kenneth Stott
# Canary: e3b7f418-29ac-4d15-96c0-7a1d58e2f063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's english_stem dictionary (REQ-2171): the Snowball English stemmer of
src/backend/snowball/libstemmer/stem_UTF_8_english.c and the stop words of english.stop."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

STOP_WORDS = frozenset(
    """
    i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she
    her hers herself it its itself they them their theirs themselves what which who whom this that
    these those am is are was were be been being have has had having do does did doing a an the and
    but if or because as until while of at by for with about against between into through during
    before after above below to from up down in out on off over under again further then once here
    there when where why how all any both each few more most other some such no nor not only own
    same so than too very s t can will just don should now
    """.split()
)
_VOWELS = "aeiouy"
_DOUBLES = ("bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt")
_LI_ENDINGS = "cdeghkmnrt"
_EXCEPTIONS = {
    "skis": "ski", "skies": "sky", "dying": "die", "lying": "lie", "tying": "tie", "idly": "idl",
    "gently": "gentl", "ugly": "ugli", "early": "earli", "only": "onli", "singly": "singl",
    "sky": "sky", "news": "news", "howe": "howe", "atlas": "atlas", "cosmos": "cosmos",
    "bias": "bias", "andes": "andes",
}  # fmt: skip
_AFTER_STEP_1A = frozenset(
    {"inning", "outing", "canning", "herring", "earring", "proceed", "exceed", "succeed"}
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


def _vowel(char: str) -> bool:
    return char in _VOWELS


def _regions(word: str) -> tuple[int, int]:
    """R1 and R2, the offsets past the first and second non-vowel after a vowel."""

    def after(start: int) -> int:
        at = start
        while at < len(word) and not _vowel(word[at]):
            at += 1
        while at < len(word) and _vowel(word[at]):
            at += 1
        return min(at + 1, len(word))

    r1 = next((len(p) for p in _R1_PREFIXES if word.startswith(p)), None)
    r1 = after(0) if r1 is None else r1
    return r1, after(r1)


def _short_syllable(word: str, end: int) -> bool:
    """Whether ``word[:end]`` ends in a short syllable."""
    if end == 2:
        return _vowel(word[0]) and not _vowel(word[1])
    if end < 3:
        return False
    last, middle, first = word[end - 1], word[end - 2], word[end - 3]
    return not _vowel(last) and last not in "wxY" and _vowel(middle) and not _vowel(first)


#: dict_snowball passes no longer word to the stemmer: it is its own lexeme.
UNSTEMMED_BYTES = 1000


def stem(word: str) -> str:
    """The stem of a lower case word; dict_snowball keeps a word whose stem is empty."""
    if len(word) <= 2 or len(word.encode()) > UNSTEMMED_BYTES:
        return word
    if word in _EXCEPTIONS:
        return _EXCEPTIONS[word]
    if not _step_0(word.removeprefix("'")):
        return word
    word = word.removeprefix("'")
    chars = list(word)
    for at, char in enumerate(chars):
        if char == "y" and (at == 0 or chars[at - 1] in _VOWELS):
            chars[at] = "Y"
    word = "".join(chars)
    r1, r2 = _regions(word)
    word = _step_0(word)
    word = _step_1a(word)
    if word in _AFTER_STEP_1A:
        return word
    word = _step_1b(word, r1)
    word = _step_1c(word)
    word = _step_2(word, r1)
    word = _step_3(word, r1, r2)
    word = _step_4(word, r2)
    word = _step_5(word, r1, r2)
    return word.replace("Y", "y")


def _step_0(word: str) -> str:
    for suffix in ("'s'", "'s", "'"):
        if word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def _step_1a(word: str) -> str:
    if word.endswith("sses"):
        return word[:-2]
    if word.endswith(("ied", "ies")):
        return word[:-2] if len(word) > 4 else word[:-1]
    if word.endswith(("us", "ss")):
        return word
    if word.endswith("s") and any(_vowel(c) for c in word[:-2]):
        return word[:-1]
    return word


def _step_1b(word: str, r1: int) -> str:
    for suffix in ("eedly", "eed"):
        if word.endswith(suffix):
            return word[: -len(suffix)] + "ee" if len(word) - len(suffix) >= r1 else word
    for suffix in ("ingly", "edly", "ing", "ed"):
        if word.endswith(suffix):
            stemmed = word[: -len(suffix)]
            if not any(_vowel(c) for c in stemmed):
                return word
            return _after_1b(stemmed, r1)
    return word


def _after_1b(word: str, r1: int) -> str:
    if word.endswith(("at", "bl", "iz")):
        return word + "e"
    if word.endswith(_DOUBLES):
        return word[:-1]
    return word + "e" if len(word) <= r1 and _short_syllable(word, len(word)) else word


def _step_1c(word: str) -> str:
    if len(word) > 2 and word[-1] in "yY" and not _vowel(word[-2]):
        return word[:-1] + "i"
    return word


def _step_2(word: str, r1: int) -> str:
    for suffix, written in _STEP_2:
        if not word.endswith(suffix):
            continue
        rest = len(word) - len(suffix)
        if rest < r1:
            return word
        if suffix == "ogi" and not word[:rest].endswith("l"):
            return word
        if suffix == "li" and (not rest or word[rest - 1] not in _LI_ENDINGS):
            return word
        return word[:rest] + written
    return word


def _step_3(word: str, r1: int, r2: int) -> str:
    for suffix, written in _STEP_3:
        if not word.endswith(suffix):
            continue
        rest = len(word) - len(suffix)
        if rest < (r2 if suffix == "ative" else r1):
            return word
        return word[:rest] + written
    return word


def _step_4(word: str, r2: int) -> str:
    for suffix in _STEP_4:
        if not word.endswith(suffix):
            continue
        rest = len(word) - len(suffix)
        if rest < r2 or (suffix == "ion" and word[rest - 1 : rest] not in ("s", "t")):
            return word
        return word[:rest]
    return word


def _step_5(word: str, r1: int, r2: int) -> str:
    rest = len(word) - 1
    if word.endswith("e"):
        if rest >= r2 or (rest >= r1 and not _short_syllable(word, rest)):
            return word[:-1]
    elif word.endswith("l") and rest >= r2 and word[rest - 1 : rest] == "l":
        return word[:-1]
    return word


def lexize(word: str) -> list[str]:
    """english_stem's answer: nothing for a stop word."""
    lowered = word.lower()
    return [] if lowered in STOP_WORDS else [stem(lowered)]
