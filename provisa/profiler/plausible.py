# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6a3f92-71b8-4e5d-9a24-c8e1b7d5f063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's PLAUSIBLE TYPE, inferred from its profile, name and tags (REQ-1934).

A label, not a value: what the column most plausibly holds, with a confidence and the evidence. The
table editor's fill-fake-attributes action (REQ-1494) reads it to choose each column's kind of fake.
Rules run in order and the first that matches wins; each states its evidence in words.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from provisa.profiler.statement import ColumnAggregates

PLAUSIBLE_TYPES: tuple[str, ...] = (
    "email",
    "person_name_first",
    "person_name_last",
    "person_name_full",
    "phone",
    "address_street",
    "address_city",
    "address_region",
    "address_postal_code",
    "address_country",
    "url",
    "identifier",
    "category",
    "free_text",
    "numeric_distribution",
    "temporal",
    "boolean",
    "unknown",
)

# The share of the column's non-null rows, over its recorded shapes, that a shape rule needs.
_SHAPE_MAJORITY = 0.8

_PHONE_SHAPE = re.compile(r"^[9 ()+\-.]+$")
_URL_SHAPE = re.compile(r"^a{3,5}s?://")
_NAME_WORD = re.compile(r"^Aa+$")
_FULL_NAME = re.compile(r"^Aa+( Aa+)+$")

_ADDRESS_NAMES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("address_postal_code", ("zip", "zipcode", "postal", "postcode", "postal_code")),
    ("address_street", ("street", "address", "address1", "address2", "addr", "line1", "line2")),
    ("address_city", ("city", "town")),
    ("address_region", ("state", "province", "region", "county")),
    ("address_country", ("country",)),
)


@dataclass(frozen=True)
class PlausibleType:
    plausible_type: str
    confidence: float
    evidence: str


def _tokens(name: str) -> set[str]:
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()
    parts = [p for p in re.split(r"[^a-z0-9]+", snake) if p]
    return set(parts) | {"_".join(parts)}


def _shape_share(agg: ColumnAggregates, test) -> float:
    if agg.non_null == 0:
        return 0.0
    return sum(count for shape, count in agg.shapes if shape is not None and test(shape)) / (
        agg.non_null
    )


def infer(agg: ColumnAggregates, tags: set[str], low_cardinality_max: int) -> PlausibleType:
    """``low_cardinality_max`` is the profiler's run default: a text column with no more distinct
    values, few against its rows, is a category."""
    name = agg.spec.name
    family = agg.spec.family
    tokens = _tokens(name)
    tag_note = f"; tagged {', '.join(sorted(tags))}" if tags else ""

    if family == "boolean":
        return PlausibleType("boolean", 0.99, f"boolean data type{tag_note}")
    if family == "temporal":
        return PlausibleType("temporal", 0.99, f"{agg.spec.data_type} data type{tag_note}")
    if agg.non_null == 0:
        return PlausibleType("unknown", 0.0, f"no non-null values profiled{tag_note}")
    distinct_ratio = agg.distinct / agg.non_null

    if family == "numeric":
        whole = agg.integers == agg.non_null
        if agg.distinct == 2 and {v for v, _ in agg.values if v is not None} <= {
            "0",
            "1",
            "0.0",
            "1.0",
        }:
            return PlausibleType("boolean", 0.8, f"two distinct values, 0 and 1{tag_note}")
        if (
            whole
            and distinct_ratio >= 0.95
            and ("id" in tokens or any(t.endswith("_id") for t in tokens))
        ):
            return PlausibleType(
                "identifier",
                0.85,
                f"whole numbers, {distinct_ratio:.0%} distinct, named {name}{tag_note}",
            )
        return PlausibleType(
            "numeric_distribution", 0.9, f"{agg.spec.data_type} data type{tag_note}"
        )
    if family != "text":
        return PlausibleType("unknown", 0.2, f"{agg.spec.data_type} data type{tag_note}")

    email = _shape_share(agg, lambda s: "@" in s and "." in s.split("@")[-1])
    if email >= _SHAPE_MAJORITY or ("email" in tokens and email > 0):
        return PlausibleType(
            "email", max(email, 0.6), f"{email:.0%} of values shaped like an email{tag_note}"
        )
    url = _shape_share(agg, lambda s: bool(_URL_SHAPE.match(s)))
    if url >= _SHAPE_MAJORITY:
        return PlausibleType("url", url, f"{url:.0%} of values shaped like a URL{tag_note}")
    phone = _shape_share(agg, lambda s: bool(_PHONE_SHAPE.match(s)) and 7 <= s.count("9") <= 15)
    if phone >= _SHAPE_MAJORITY and tokens & {"phone", "mobile", "tel", "telephone", "fax", "cell"}:
        return PlausibleType("phone", phone, f"named {name}, {phone:.0%} phone-shaped{tag_note}")
    for kind, names in _ADDRESS_NAMES:
        if tokens & set(names):
            return PlausibleType(kind, 0.7, f"named {name}{tag_note}")
    word = _shape_share(agg, lambda s: bool(_NAME_WORD.match(s)))
    full = _shape_share(agg, lambda s: bool(_FULL_NAME.match(s)))
    if tokens & {"first_name", "firstname", "given_name", "forename"}:
        return PlausibleType(
            "person_name_first",
            0.6 + 0.3 * word,
            f"named {name}, {word:.0%} single capitalised words{tag_note}",
        )
    if tokens & {"last_name", "lastname", "surname", "family_name"}:
        return PlausibleType(
            "person_name_last",
            0.6 + 0.3 * word,
            f"named {name}, {word:.0%} single capitalised words{tag_note}",
        )
    if (tokens & {"full_name", "fullname", "name", "customer_name", "person_name"}) and full >= 0.5:
        return PlausibleType(
            "person_name_full",
            0.5 + 0.4 * full,
            f"named {name}, {full:.0%} capitalised word pairs{tag_note}",
        )
    no_space = _shape_share(agg, lambda s: " " not in s)
    top_shape = agg.shapes[0] if agg.shapes else None
    if (
        distinct_ratio >= 0.9
        and no_space >= _SHAPE_MAJORITY
        and top_shape is not None
        and top_shape[1] / agg.non_null >= 0.5
    ):
        return PlausibleType(
            "identifier",
            0.8,
            f"{distinct_ratio:.0%} distinct, {top_shape[1] / agg.non_null:.0%} shaped "
            f"{top_shape[0]}{tag_note}",
        )
    if agg.distinct <= low_cardinality_max and distinct_ratio < 0.5:
        return PlausibleType(
            "category", 0.8, f"{agg.distinct} distinct values over {agg.non_null} rows{tag_note}"
        )
    if agg.length_max is not None and agg.length_max > 50 and no_space < 0.5:
        return PlausibleType(
            "free_text", 0.7, f"up to {agg.length_max} characters, mostly with spaces{tag_note}"
        )
    return PlausibleType("unknown", 0.2, f"no rule matched{tag_note}")
