# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b1d82-9a47-4c15-8e2b-6d0c5a71f94e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Branded sources carried by the OpenAPI source (REQ-1923).

A brand is a named system whose vendor publishes a well-typed OpenAPI spec -- Stripe is the
first. It adds to the generic OpenAPI source only what is particular to that system: its spec,
pinned and shipped with Provisa rather than fetched at registration, how a credential is
presented, and a call that succeeds only with a working one. Everything else -- mapping the spec
to tables and commands, proposing each list's paging, reading, governing -- is the generic
source's.

Adding a branded source registers no tables. Every table the spec offers is available, and the
steward registers the ones wanted, as for any source.
"""

# Requirements: REQ-1923
from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_SPEC_DIR = Path(__file__).parent / "_known_specs"

#: How a branded source's row names its spec (``sources.path``): the shipped one of its brand.
SPEC_SCHEME = "brand:"


@dataclass(frozen=True)
class Brand:
    id: str  # the value the source picker sends and the source row records
    label: str  # what the user sees
    spec_url: str  # where the vendor publishes the spec (scripts/bake_openapi_brand_spec.py)
    verify_path: str  # a GET that succeeds only with a working credential

    @property
    def spec_path(self) -> str:
        return f"{SPEC_SCHEME}{self.id}"

    def auth(self, token: str) -> dict:
        return {"type": "bearer", "token": token}

    def spec(self) -> dict:
        return brand_spec(self.id)


BRANDS: dict[str, Brand] = {
    "stripe": Brand(
        id="stripe",
        label="Stripe",
        spec_url="https://raw.githubusercontent.com/stripe/openapi/master/openapi/spec3.json",
        verify_path="/v1/balance",
    ),
}


@lru_cache(maxsize=None)
def brand_spec(brand_id: str) -> dict:
    """The spec a brand ships with. Callers read it and never change it."""
    if brand_id not in BRANDS:
        raise FileNotFoundError(f"No branded OpenAPI source {brand_id!r}")
    with gzip.open(_SPEC_DIR / f"{brand_id}.json.gz", "rb") as fh:
        return json.load(fh)


def spec_brand(spec_path: str) -> str | None:
    """The brand ``spec_path`` names, or None for a spec of the steward's own."""
    return spec_path[len(SPEC_SCHEME) :] if spec_path.startswith(SPEC_SCHEME) else None
