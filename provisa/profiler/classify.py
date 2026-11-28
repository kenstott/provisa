# Copyright (c) 2026 Kenneth Stott
# Canary: b70d0ed1-de5f-451f-a006-ffa19933628b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Classification kinds: what a column holds, recognised from its sampled values (REQ-2147).

A kind is recognised by a test applied to each of a column's sampled non-null values: for a
sensitive kind a regular expression the whole value must match, with a check where the value has a
checksum or a rule a pattern cannot state (the SSN rules, the Luhn check of a card number, an IBAN's
check digits, the check digit or letter of a national identifier), membership in a shipped
reference list for the others (``reference/``, built by scripts/build_classification_reference.py). Where the share of
values passing reaches the kind's threshold the column gets a suggestion, which a steward accepts
or rejects (``provisa.profiler.classifications``).

The kinds are ``classification_kinds.yaml``. The sampled values are read by the run
(``provisa.profiler.run``), tested here in memory, and never stored: only the counts are.
"""

# Requirements: REQ-2147

from __future__ import annotations

import base64
import collections
import functools
import gzip
import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Sequence

import yaml

CATALOGUE_PATH = Path(__file__).resolve().parent / "classification_kinds.yaml"
REFERENCE_DIR = Path(__file__).resolve().parent / "reference"
REFERENCE_PREFIX = "reference:"
FAMILIES = ("text", "numeric")
# The fewest non-null values a column must have had sampled for a share to suggest anything: a
# share over a handful of values says nothing of the column.
MIN_TESTED = 10


@dataclass(frozen=True)
class Kind:
    id: str
    label: str
    noun: str
    passes: str
    sensitive: bool
    test: str
    pattern: str | None
    contained_pattern: str | None
    check: str | None
    ignore: str | None
    ignore_case: bool
    families: tuple[str, ...]
    threshold: float
    term: str
    mask_preset: str | None
    # REQ-2147 (amended 2026-11-25): an org admin's own kind, stored in the model, not shipped.
    custom: bool = False

    @property
    def reference(self) -> str | None:
        """The reference list the kind tests against; None for a validator kind."""
        return (
            self.test.removeprefix(REFERENCE_PREFIX)
            if self.test.startswith(REFERENCE_PREFIX)
            else None
        )


@functools.cache
def _whole_pattern(pattern: str, ignore_case: bool) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE if ignore_case else 0)


@dataclass(frozen=True)
class Match:
    kind: Kind
    tested: int
    passed: int

    @property
    def share(self) -> float:
        return self.passed / self.tested

    def evidence(self, table: str, column: str) -> str:
        return (
            f"{table}.{column} looks like {self.kind.noun}: {self.share:.0%} of sampled values "
            f"{self.kind.passes}"
        )


def normalise(value: str) -> str:
    """A value as the reference lists hold it: trimmed, case-folded, inner spaces collapsed."""
    return " ".join(value.split()).casefold()


# -- checks ----------------------------------------------------------------------------------------
#
# A kind is a pattern (a regular expression of the value's shape, written without ^ or $ so that it
# can also be searched for inside a longer text; the whole-value test anchors it) and, where the
# value carries a checksum or a rule a pattern cannot state, a check of the same name in CHECKS. A
# check is given the value as the pattern matched it, with the kind's ignored characters removed.

_NON_DIGITS = re.compile(r"\D")


def _digits(value: str) -> str:
    return _NON_DIGITS.sub("", value)


def _luhn(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
    return total % 10 == 0


def us_ssn(value: str) -> bool:
    """Ddd-dd-dddd or 7 to 9 plain digits (a number column drops leading zeros): the area is never
    000, 666 or 900-999, the group never 00, the serial never 0000."""
    digits = value.replace("-", "").zfill(9)
    area, group, serial = digits[:3], digits[3:5], digits[5:]
    return area not in ("000", "666") and area < "900" and group != "00" and serial != "0000"


def payment_card(value: str) -> bool:
    """The Luhn check."""
    return _luhn(value)


def iban(value: str) -> bool:
    """The whole number, its first four characters moved to the end, leaves 1 divided by 97
    (ISO 13616)."""
    moved = value[4:].upper() + value[:4].upper()
    return int("".join(str(int(ch, 36)) for ch in moved)) % 97 == 1


def ca_social_insurance_number(value: str) -> bool:
    """The Luhn check."""
    return _luhn(value)


_DNI_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"


def es_dni(value: str) -> bool:
    """The number mod 23 names the check letter; an NIE's X, Y or Z counts 0-2."""
    raw = value.upper()
    if raw[0] in "XYZ":
        raw = str("XYZ".index(raw[0])) + raw[1:]
    return _DNI_LETTERS[int(raw[:8]) % 23] == raw[8]


def nl_bsn(value: str) -> bool:
    """Eight or nine digits (a number column may drop a leading zero) passing the eleven test."""
    digits = value.zfill(9)
    if digits == "000000000":
        return False
    total = sum(int(d) * w for d, w in zip(digits[:8], range(9, 1, -1))) - int(digits[8])
    return total % 11 == 0


_VERHOEFF_D = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 2, 3, 4, 0, 6, 7, 8, 9, 5),
    (2, 3, 4, 0, 1, 7, 8, 9, 5, 6),
    (3, 4, 0, 1, 2, 8, 9, 5, 6, 7),
    (4, 0, 1, 2, 3, 9, 5, 6, 7, 8),
    (5, 9, 8, 7, 6, 0, 4, 3, 2, 1),
    (6, 5, 9, 8, 7, 1, 0, 4, 3, 2),
    (7, 6, 5, 9, 8, 2, 1, 0, 4, 3),
    (8, 7, 6, 5, 9, 3, 2, 1, 0, 4),
    (9, 8, 7, 6, 5, 4, 3, 2, 1, 0),
)
_VERHOEFF_P = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 5, 7, 6, 2, 8, 3, 0, 9, 4),
    (5, 8, 0, 3, 7, 9, 6, 1, 4, 2),
    (8, 9, 1, 6, 0, 4, 3, 5, 2, 7),
    (9, 4, 5, 3, 1, 2, 6, 8, 7, 0),
    (4, 2, 8, 6, 5, 7, 3, 9, 0, 1),
    (2, 7, 9, 3, 8, 0, 6, 4, 1, 5),
    (7, 0, 4, 6, 9, 1, 3, 2, 5, 8),
)


def in_aadhaar(value: str) -> bool:
    """The Verhoeff check."""
    check = 0
    for i, ch in enumerate(reversed(value)):
        check = _VERHOEFF_D[check][_VERHOEFF_P[i % 8][int(ch)]]
    return check == 0


def us_bank_routing_number(value: str) -> bool:
    """The ABA checksum: 3(d1+d4+d7) + 7(d2+d5+d8) + (d3+d6+d9) is a multiple of 10, not zero."""
    d = [int(c) for c in value]
    total = 3 * (d[0] + d[3] + d[6]) + 7 * (d[1] + d[4] + d[7]) + (d[2] + d[5] + d[8])
    return total > 0 and total % 10 == 0


def email_address(value: str) -> bool:
    """A local part of at most 64 characters with no leading, trailing or doubled dot, in an
    address of at most 254 (RFC 5321)."""
    local = value.rpartition("@")[0]
    return (
        len(local) <= 64
        and len(value) <= 254
        and not local.startswith(".")
        and not local.endswith(".")
        and ".." not in local
    )


def de_steuer_id(value: str) -> bool:
    """Eleven digits: of the first ten exactly one digit occurs two or three times (never three in
    a row) and the rest at most once; the eleventh is the ISO 7064 MOD 11,10 check digit."""
    digits = _digits(value)
    first = digits[:10]
    counts = collections.Counter(first)
    repeated = [d for d, n in counts.items() if n > 1]
    if len(repeated) != 1 or counts[repeated[0]] > 3:
        return False
    if counts[repeated[0]] == 3 and repeated[0] * 3 in first:
        return False
    product = 10
    for ch in first:
        total = (int(ch) + product) % 10 or 10
        product = (2 * total) % 11
    return (11 - product) % 10 == int(digits[10])


def fr_nir(value: str) -> bool:
    """Thirteen characters (Corsica's department 2A or 2B counting as 19 or 18) and their key:
    97 minus the number mod 97."""
    raw = value.upper().replace(" ", "")
    number = int(raw[:13].replace("2A", "19").replace("2B", "18"))
    return 97 - number % 97 == int(raw[13:])


def au_tfn(value: str) -> bool:
    """Nine digits, not all zero, whose weighted sum (1, 4, 3, 7, 5, 8, 6, 9, 10) is a multiple of
    11."""
    weights = (1, 4, 3, 7, 5, 8, 6, 9, 10)
    digits = _digits(value)
    return int(digits) > 0 and sum(int(d) * w for d, w in zip(digits, weights)) % 11 == 0


def br_cpf(value: str) -> bool:
    """Eleven digits, not all alike, whose last two are the mod 11 check digits of the rest."""
    digits = [int(c) for c in _digits(value)]
    if len(set(digits)) == 1:
        return False
    for n in (9, 10):
        total = sum(d * w for d, w in zip(digits[:n], range(n + 1, 1, -1)))
        if digits[n] != (total * 10 % 11) % 10:
            return False
    return True


def uk_nhs_number(value: str) -> bool:
    """Ten digits whose first nine, weighted 10 down to 2, give the check digit by mod 11 (a
    remainder that would make 10 is never issued)."""
    digits = [int(c) for c in _digits(value)]
    check = 11 - sum(d * w for d, w in zip(digits[:9], range(10, 1, -1))) % 11
    return check != 10 and check % 11 == digits[9]


def jwt(value: str) -> bool:
    """The header segment decodes, as base64url, to a JSON object that names an ``alg``."""
    header = value.split(".", 1)[0]
    try:
        decoded = json.loads(base64.urlsafe_b64decode(header + "=" * (-len(header) % 4)))
    except ValueError:
        return False
    return isinstance(decoded, dict) and "alg" in decoded


CHECKS: dict[str, Callable[[str], bool]] = {
    f.__name__: f
    for f in (
        us_ssn,
        payment_card,
        iban,
        ca_social_insurance_number,
        es_dni,
        nl_bsn,
        in_aadhaar,
        us_bank_routing_number,
        email_address,
        de_steuer_id,
        fr_nir,
        au_tfn,
        br_cpf,
        uk_nhs_number,
        jwt,
    )
}


# -- the catalogue ---------------------------------------------------------------------------------


@functools.cache
def reference_list(name: str) -> frozenset[str]:
    path = REFERENCE_DIR / f"{name}.txt.gz"
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return frozenset(line for line in f.read().splitlines() if line)


def _kind(entry: dict, *, custom: bool = False) -> Kind:
    sensitive = entry["sensitive"]
    pattern = entry.get("pattern")
    contained = entry.get("contained_pattern")
    check = entry.get("check")
    ignore = entry.get("ignore")
    test = entry.get("test", entry["id"])
    kind = Kind(
        id=entry["id"],
        label=entry["label"],
        noun=entry["noun"],
        passes=entry["passes"],
        sensitive=sensitive,
        test=test,
        pattern=pattern,
        contained_pattern=contained,
        check=check,
        ignore=ignore,
        ignore_case=bool(entry.get("ignore_case", False)),
        families=tuple(entry["families"]),
        threshold=float(entry["threshold"]),
        term=entry["term"],
        mask_preset=entry.get("mask_preset"),
        custom=custom,
    )
    if kind.reference is not None:
        if pattern or contained or check or ignore:
            raise ValueError(f"classification kind {kind.id!r}: a reference kind has no pattern")
    else:
        if not pattern:
            raise ValueError(f"classification kind {kind.id!r}: no pattern")
        re.compile(pattern)  # a pattern that does not compile raises here, at load
        if contained:
            re.compile(contained)
        if ignore:
            re.compile(ignore)
        if check is not None and check not in CHECKS:
            raise ValueError(f"classification kind {kind.id!r}: no check {check!r}")
    if not set(kind.families) <= set(FAMILIES):
        raise ValueError(f"classification kind {kind.id!r}: families must be among {FAMILIES}")
    if sensitive != (kind.mask_preset is not None):
        raise ValueError(f"classification kind {kind.id!r}: a mask preset is a sensitive kind's")
    if not 0 < kind.threshold <= 1:
        raise ValueError(f"classification kind {kind.id!r}: threshold must be in (0, 1]")
    return kind


@functools.cache
def catalogue() -> tuple[Kind, ...]:
    """Every kind, in the catalogue's order: sensitive kinds first."""
    kinds = tuple(_kind(e) for e in yaml.safe_load(CATALOGUE_PATH.read_text())["kinds"])
    ids = [k.id for k in kinds]
    if len(set(ids)) != len(ids):
        raise ValueError("classification kinds repeat an id")
    return kinds


def kind_of(kind_id: str) -> Kind:
    """A shipped kind, as the catalogue declares it."""
    return kind_in(catalogue(), kind_id)


def kind_in(kinds: Sequence[Kind], kind_id: str) -> Kind:
    """The kind of ``kinds`` with ``kind_id``; an unknown one is refused by name."""
    for k in kinds:
        if k.id == kind_id:
            return k
    raise KeyError(f"unknown classification kind {kind_id!r}")


# -- the org's kinds (REQ-2147, amended 2026-11-25) ------------------------------------------------
#
# A shipped kind keeps its coded test; the org may change its share that must pass, whether it is
# sensitive and the mask it offers. A custom kind is the org's own: a name, a regular expression
# with an optional check of CHECKS, the characters ignored before matching, the share that must
# pass, whether it is sensitive, its mask and the glossary term a confirmed column takes. Both are
# the org's ``classification`` setting, part of the model, so they move with it across
# environments (provisa.core.env_classes carries the setting).

CUSTOM_PREFIX = "custom."
# The settings of a shipped kind an org may change.
CHANGEABLE = ("threshold", "sensitive", "mask_preset")


def custom_id(name: str) -> str:
    """A custom kind's id, from the name it was created with: ``custom.`` and the name in lower
    case, each run of other characters than letters and digits one underscore."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.casefold()).strip("_")
    if not slug:
        raise ValueError(f"classification kind name {name!r} has no letter or digit")
    return CUSTOM_PREFIX + slug


def custom_kind(entry: dict) -> Kind:
    """A custom kind from its stored entry, checked as a shipped one is at load: the pattern (and
    the ignored characters) compile, the check is one of CHECKS, the share is in (0, 1], and a mask
    is a sensitive kind's. A custom kind's test applies to text values and to the whole numbers of
    a number column, as a shipped pattern kind's may."""
    name = entry["label"]
    try:
        return _custom_kind(entry, name)
    except re.error as exc:
        raise ValueError(
            f"classification kind {name!r}: the pattern does not compile: {exc}"
        ) from exc


def _custom_kind(entry: dict, name: str) -> Kind:
    return _kind(
        {
            "id": entry["id"],
            "label": name,
            "noun": name,
            "passes": "pass its test",
            "sensitive": entry["sensitive"],
            "pattern": entry["pattern"],
            "check": entry["check"],
            "ignore": entry["ignore"],
            "families": list(FAMILIES),
            "threshold": entry["threshold"],
            "term": entry["term"],
            "mask_preset": entry["mask_preset"],
        },
        custom=True,
    )


def changed_kind(kind: Kind, change: dict) -> Kind:
    """A shipped ``kind`` with the org's ``change`` (some of :data:`CHANGEABLE`), checked as the
    catalogue's own entries are."""
    unknown = set(change) - set(CHANGEABLE)
    if unknown:
        raise ValueError(
            f"classification kind {kind.id!r}: only {CHANGEABLE} of a shipped kind may change, "
            f"not {sorted(unknown)}"
        )
    changed = replace(kind, **change)
    if changed.sensitive != (changed.mask_preset is not None):
        raise ValueError(f"classification kind {kind.id!r}: a mask preset is a sensitive kind's")
    if not 0 < changed.threshold <= 1:
        raise ValueError(f"classification kind {kind.id!r}: threshold must be in (0, 1]")
    return changed


def org_catalogue(changed: dict[str, dict], custom: Sequence[dict]) -> tuple[Kind, ...]:
    """Every kind of an org: the shipped ones with its changes, then its custom ones, sensitive
    kinds first (where several kinds pass, the first is the column's plausible type)."""
    shipped = {k.id for k in catalogue()}
    unknown = set(changed) - shipped
    if unknown:
        raise ValueError(f"the org changes unknown classification kinds {sorted(unknown)}")
    kinds = [changed_kind(k, changed[k.id]) if k.id in changed else k for k in catalogue()]
    kinds += [custom_kind(e) for e in custom]
    ids = [k.id for k in kinds]
    if len(set(ids)) != len(ids):
        raise ValueError("classification kinds repeat an id")
    return tuple(sorted(kinds, key=lambda k: not k.sensitive))


def matches_whole_value(kind: Kind, value: str) -> bool:
    """Whether ``value`` as a whole is a ``kind`` (not a reference kind): the kind's ignored
    characters removed (surrounding whitespace where it names none), the pattern matching all of
    what is left, and the check, where the kind has one, passing it."""
    assert kind.pattern is not None, kind.id
    text = re.sub(kind.ignore, "", value) if kind.ignore else value.strip()
    if _whole_pattern(kind.pattern, kind.ignore_case).fullmatch(text) is None:
        return False
    return _check_passes(kind, text)


def _check_passes(kind: Kind, text: str) -> bool:
    """Whether ``text``, as the kind's pattern matched it, passes the kind's check (none: it does).
    A shipped kind's pattern only ever hands its check text of the shape it reads. A custom kind
    pairs its own pattern with a shipped check (REQ-2147, amended 2026-11-25), so its pattern may
    match text the check cannot read (letters where it reads digits, fewer characters than it
    counts): such a value does not pass the check, as a value failing its checksum does not."""
    if kind.check is None:
        return True
    if not kind.custom:
        return CHECKS[kind.check](text)
    try:
        return CHECKS[kind.check](text)
    except (ValueError, IndexError):
        return False


@functools.cache
def _contained_pattern(pattern: str, ignore_case: bool) -> re.Pattern[str]:
    # Unanchored, bounded by anything that is not a letter, digit or underscore: a kind inside a
    # longer run of characters (a digit string inside a longer one) is not the kind.
    return re.compile(rf"(?<!\w)(?:{pattern})(?!\w)", re.IGNORECASE if ignore_case else 0)


def contains(kind: Kind, value: str) -> bool:
    """Whether ``value`` holds a ``kind`` (not a reference kind) inside it (REQ-2147 KINDS ARE
    PATTERN RULES; CONTAINED VALUES ARE HINTS): the kind's contained_pattern, else its pattern,
    found anywhere in the text with nothing word-like either side, and the check, where the kind has one, passing the matched text
    with the kind's ignored characters removed. Only whether: the matched text is never returned."""
    pattern = kind.contained_pattern or kind.pattern
    assert pattern is not None, kind.id
    for found in _contained_pattern(pattern, kind.ignore_case).finditer(value):
        text = re.sub(kind.ignore, "", found.group()) if kind.ignore else found.group()
        if _check_passes(kind, text):
            return True
    return False


@dataclass(frozen=True)
class Hint:
    """A kind found inside a column's sampled values: never a classification, never applied."""

    kind: Kind
    tested: int
    matched: int

    @property
    def share(self) -> float:
        return self.matched / self.tested

    def evidence(self) -> str:
        return f"matches {self.kind.noun} in {self.share:.0%} of sampled values"


def contained_hints(values: Sequence[str | None], kinds: tuple[Kind, ...]) -> list[Hint]:
    """The kinds found inside a text column's sampled ``values``, each with the share of values
    holding one. A kind the column classifies as by whole value (:func:`classify`) is that
    classification's and no hint; a reference kind is a whole value or nothing."""
    usable = tuple(k for k in kinds if "text" in k.families and k.reference is None)
    tested = [v for v in values if v is not None]
    if not usable or len(tested) < MIN_TESTED:
        return []
    classified = {m.kind.id for m in classify("text", list(values), usable)}
    out = []
    for kind in usable:
        if kind.id in classified:
            continue
        matched = sum(1 for v in tested if contains(kind, v))
        if matched:
            out.append(Hint(kind, len(tested), matched))
    return out


def _passes(kind: Kind, value: str) -> bool:
    if kind.reference is not None:
        return normalise(value) in reference_list(kind.reference)
    return matches_whole_value(kind, value)


def _tested_value(family: str, value: str) -> str | None:
    """The value a kind tests: a text column's as it is, a number column's when it is a whole
    number (an identifier held as a number), written without a fraction. None: a number that is
    not one, which no kind passes."""
    if family == "text":
        return value
    try:
        number = float(value)
    except ValueError:
        return None
    return str(int(number)) if number.is_integer() and number >= 0 else None


@dataclass(frozen=True)
class Judgement:
    """What one column's sampled values say of it (REQ-2147): ``values``, the non-null values
    sampled; ``matches``, the kinds they reached the threshold of. A column with fewer than
    :data:`MIN_TESTED` values is ``too_few`` -- too few values to classify, which is not the same
    statement as matching no kind."""

    values: int
    matches: tuple[Match, ...]

    @property
    def too_few(self) -> bool:
        return self.values < MIN_TESTED


def judge(family: str, values: Sequence[str | None], kinds: tuple[Kind, ...]) -> Judgement:
    """``values`` (one column's sampled values, of ``family``) judged against ``kinds``."""
    tested = [_tested_value(family, v) for v in values if v is not None]
    usable = [k for k in kinds if family in k.families]
    if len(tested) < MIN_TESTED or not usable:
        return Judgement(len(tested), ())
    out = []
    for kind in usable:
        passed = sum(1 for v in tested if v is not None and _passes(kind, v))
        match = Match(kind, len(tested), passed)
        if match.share >= kind.threshold:
            out.append(match)
    return Judgement(len(tested), tuple(out))


def classify(family: str, values: Sequence[str | None], kinds: tuple[Kind, ...]) -> list[Match]:
    """The kinds ``values`` (one column's sampled values, of ``family``) reach the threshold of."""
    return list(judge(family, values, kinds).matches)


def classified_families() -> frozenset[str]:
    return frozenset(f for k in catalogue() for f in k.families)


# A kind's whole-value test by its ``test`` name: what the catalogue's pattern and check say of a
# value, for every kind that is not a reference list.
VALIDATORS: dict[str, Callable[[str], bool]] = {
    k.test: functools.partial(matches_whole_value, k) for k in catalogue() if k.reference is None
}
