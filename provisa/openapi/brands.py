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
pinned and shipped with Provisa rather than fetched at registration, the fields its remote
answers that the spec leaves out, how a credential is presented, and a call that succeeds only
with a working one. Everything else -- mapping the spec
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
    # The header that names the API version a call is answered in, where the vendor has one.
    # It carries the shipped spec's version, so an answer has the shape the spec describes
    # whatever version the account itself defaults to.
    version_header: str | None = None

    @property
    def spec_path(self) -> str:
        return f"{SPEC_SCHEME}{self.id}"

    def auth(self, token: str) -> dict:
        return {"type": "bearer", "token": token}

    def spec(self) -> dict:
        return brand_spec(self.id)

    def headers(self) -> dict[str, str]:
        """What every call to the brand carries beside its credential."""
        if self.version_header is None:
            return {}
        return {self.version_header: self.spec()["info"]["version"]}


BRANDS: dict[str, Brand] = {
    "stripe": Brand(
        id="stripe",
        label="Stripe",
        spec_url="https://raw.githubusercontent.com/stripe/openapi/master/openapi/spec3.json",
        verify_path="/v1/balance",
        version_header="Stripe-Version",
    ),
}


@lru_cache(maxsize=None)
def brand_spec(brand_id: str) -> dict:
    """The spec a brand ships with: the vendor's published one, pinned, with the brand's
    additions (:func:`_add_fields`). Callers read it and never change it."""
    if brand_id not in BRANDS:
        raise FileNotFoundError(f"No branded OpenAPI source {brand_id!r}")
    with gzip.open(_SPEC_DIR / f"{brand_id}.json.gz", "rb") as fh:
        spec = json.load(fh)
    additions = _SPEC_DIR / f"{brand_id}.additions.json"
    if additions.exists():
        _add_fields(brand_id, spec, json.loads(additions.read_text()))
    return spec


def _add_fields(brand_id: str, spec: dict, additions: dict) -> None:
    """Add to ``spec`` the fields the brand's remote answers and its published spec does not
    declare (REQ-1923): ``additions`` names, by schema, the properties to add. The vendor's file
    is kept as published, so it is baked again unchanged; an addition the published spec has
    come to declare, or one for a schema it no longer has, fails the load until it is removed."""
    schemas = spec["components"]["schemas"]
    for name, fields in additions.items():
        if name.startswith("_"):
            continue  # a note to the reader of the file
        if name not in schemas:
            raise ValueError(f"{brand_id}: additions name a schema the spec does not have: {name}")
        declared = schemas[name].setdefault("properties", {})
        already = sorted(set(fields) & set(declared))
        if already:
            raise ValueError(
                f"{brand_id}: the published spec now declares {name}.{', '.join(already)}; "
                "remove it from the brand's additions"
            )
        declared.update(fields)


def spec_brand(spec_path: str) -> str | None:
    """The brand ``spec_path`` names, or None for a spec of the steward's own."""
    return spec_path[len(SPEC_SCHEME) :] if spec_path.startswith(SPEC_SCHEME) else None


def spec_headers(spec_path: str | None) -> dict[str, str]:
    """What every call to the source of ``spec_path`` carries beside its credential: its
    brand's headers, none for a spec of the steward's own."""
    brand = spec_brand(spec_path or "")
    return {} if brand is None else BRANDS[brand].headers()
