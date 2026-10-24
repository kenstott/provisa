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

A kind is recognised by a test applied to each of a column's sampled non-null values: a validator
for a sensitive kind (the SSN rules, the Luhn check of a card number, an IBAN's check digits, the
check digit or letter of a national identifier), membership in a shipped reference list for the
others (``reference/``, built by scripts/build_classification_reference.py). Where the share of
values passing reaches the kind's threshold the column gets a suggestion, which a steward accepts
or rejects (``provisa.profiler.classifications``).

The kinds are ``classification_kinds.yaml``. The sampled values are read by the run
(``provisa.profiler.run``), tested here in memory, and never stored: only the counts are.
"""

# Requirements: REQ-2147

from __future__ import annotations

import functools
import gzip
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

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
    families: tuple[str, ...]
    threshold: float
    term: str
    mask_preset: str | None

    @property
    def reference(self) -> str | None:
        """The reference list the kind tests against; None for a validator kind."""
        return (
            self.test.removeprefix(REFERENCE_PREFIX)
            if self.test.startswith(REFERENCE_PREFIX)
            else None
        )


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


# -- validators ------------------------------------------------------------------------------------

_SEPARATORS = re.compile(r"[\s\-.]")


def _digits(value: str) -> str:
    return _SEPARATORS.sub("", value)


def _luhn(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
    return total % 10 == 0


def us_ssn(value: str) -> bool:
    """Nine digits, written ddd-dd-dddd or plain (a number column drops leading zeros): the area
    is never 000, 666 or 900-999, the group never 00, the serial never 0000."""
    raw = value.strip()
    if re.fullmatch(r"\d{3}-\d{2}-\d{4}", raw):
        digits = raw.replace("-", "")
    elif re.fullmatch(r"\d{7,9}", raw):
        digits = raw.zfill(9)
    else:
        return False
    area, group, serial = digits[:3], digits[3:5], digits[5:]
    return area not in ("000", "666") and area < "900" and group != "00" and serial != "0000"


def payment_card(value: str) -> bool:
    """13 to 19 digits, spaces or dashes between groups, passing the Luhn check."""
    digits = _digits(value)
    return digits.isdigit() and 13 <= len(digits) <= 19 and _luhn(digits)


def iban(value: str) -> bool:
    """A country code, two check digits and up to 30 letters and digits, the whole number of which
    leaves 1 divided by 97 (ISO 13616)."""
    raw = value.replace(" ", "").upper()
    if not re.fullmatch(r"[A-Z]{2}\d{2}[A-Z0-9]{11,30}", raw):
        return False
    moved = raw[4:] + raw[:4]
    return int("".join(str(int(ch, 36)) for ch in moved)) % 97 == 1


_NINO = re.compile(r"(?!BG|GB|NK|KN|TN|NT|ZZ)[A-CEGHJ-PR-TW-Z][A-CEGHJ-NPR-TW-Z]\d{6}[A-D]")


def uk_national_insurance_number(value: str) -> bool:
    """Two prefix letters of the allowed set and not a reserved pair, six digits, a suffix A-D."""
    return bool(_NINO.fullmatch(value.replace(" ", "").upper()))


def ca_social_insurance_number(value: str) -> bool:
    """Nine digits, the first not 0 or 8, passing the Luhn check."""
    digits = _digits(value)
    return digits.isdigit() and len(digits) == 9 and digits[0] not in "08" and _luhn(digits)


_DNI_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"


def es_dni(value: str) -> bool:
    """Eight digits and their check letter (the number mod 23); an NIE's X, Y or Z counts 0-2."""
    raw = value.replace(" ", "").replace("-", "").upper()
    if re.fullmatch(r"[XYZ]\d{7}[A-Z]", raw):
        raw = str("XYZ".index(raw[0])) + raw[1:]
    if not re.fullmatch(r"\d{8}[A-Z]", raw):
        return False
    return _DNI_LETTERS[int(raw[:8]) % 23] == raw[8]


def nl_bsn(value: str) -> bool:
    """Nine digits (a number column may drop a leading zero) passing the eleven test."""
    digits = _digits(value)
    if not digits.isdigit() or len(digits) not in (8, 9):
        return False
    digits = digits.zfill(9)
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
    """Twelve digits, the first 2-9, passing the Verhoeff check."""
    digits = _digits(value)
    if not digits.isdigit() or len(digits) != 12 or digits[0] in "01":
        return False
    check = 0
    for i, ch in enumerate(reversed(digits)):
        check = _VERHOEFF_D[check][_VERHOEFF_P[i % 8][int(ch)]]
    return check == 0


def us_passport(value: str) -> bool:
    """A letter and eight digits, the format of US passport books issued since 2021."""
    return bool(re.fullmatch(r"[A-Z]\d{8}", value.strip().upper()))


VALIDATORS: dict[str, Callable[[str], bool]] = {
    f.__name__: f
    for f in (
        us_ssn,
        payment_card,
        iban,
        uk_national_insurance_number,
        ca_social_insurance_number,
        es_dni,
        nl_bsn,
        in_aadhaar,
        us_passport,
    )
}


# -- the catalogue ---------------------------------------------------------------------------------


@functools.cache
def reference_list(name: str) -> frozenset[str]:
    path = REFERENCE_DIR / f"{name}.txt.gz"
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return frozenset(line for line in f.read().splitlines() if line)


def _kind(entry: dict) -> Kind:
    sensitive = entry["sensitive"]
    kind = Kind(
        id=entry["id"],
        label=entry["label"],
        noun=entry["noun"],
        passes=entry["passes"],
        sensitive=sensitive,
        test=entry["test"],
        families=tuple(entry["families"]),
        threshold=float(entry["threshold"]),
        term=entry["term"],
        mask_preset=entry.get("mask_preset"),
    )
    if kind.reference is None and kind.test not in VALIDATORS:
        raise ValueError(f"classification kind {kind.id!r}: no validator {kind.test!r}")
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
    for k in catalogue():
        if k.id == kind_id:
            return k
    raise KeyError(f"unknown classification kind {kind_id!r}")


def _passes(kind: Kind, value: str) -> bool:
    if kind.reference is not None:
        return normalise(value) in reference_list(kind.reference)
    return VALIDATORS[kind.test](value)


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


def classify(family: str, values: list[str | None], kinds: tuple[Kind, ...]) -> list[Match]:
    """The kinds ``values`` (one column's sampled values, of ``family``) reach the threshold of."""
    usable = [k for k in kinds if family in k.families]
    if not usable:
        return []
    tested = [_tested_value(family, v) for v in values if v is not None]
    if len(tested) < MIN_TESTED:
        return []
    out = []
    for kind in usable:
        passed = sum(1 for v in tested if v is not None and _passes(kind, v))
        match = Match(kind, len(tested), passed)
        if match.share >= kind.threshold:
            out.append(match)
    return out


def classified_families() -> frozenset[str]:
    return frozenset(f for k in catalogue() for f in k.families)
