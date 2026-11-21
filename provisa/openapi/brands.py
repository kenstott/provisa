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

A brand may instead pass through settings the generic source already takes (REQ-2265): a base URL
its setup asks for (a system each customer runs at its own host), a spec read from that running
instance (whose spec changes between versions) rather than shipped, and how its credential is
presented, including OAuth client credentials.
"""

# Requirements: REQ-1923, REQ-2265
from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

from provisa.graphql_remote.brands import SetupField, check_setup

_SPEC_DIR = Path(__file__).parent / "_known_specs"

#: How a branded source's row names its spec (``sources.path``): the shipped one of its brand.
SPEC_SCHEME = "brand:"


#: A base URL a brand's setup asks for (REQ-2265): an http(s) address without a trailing slash,
#: so the paths a brand writes after it (``{base_url}/api``) join it once.
BASE_URL_PATTERN = r"https?://[^\s{}/]+(/[^\s{}]*[^\s{}/])?"


#: How a brand's credential is presented: a bearer token, or an OAuth client credentials grant
#: (the generic source's ``oauth2_client_credentials`` auth, provisa.core.auth_models).
BrandAuthType = Literal["bearer", "oauth2_client_credentials"]


class CredentialMissing(ValueError):
    """A brand's registration lacks a part of the credential its auth type takes. ``field``:
    which part."""

    def __init__(self, brand: str, field: str):
        super().__init__(f"{brand} needs its {field}")
        self.field = field


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
    # REQ-2265: where the source is called. Empty: the spec's first server -- the shipped spec's,
    # or, for a spec read from the instance, the running instance's, a relative one read against
    # where the spec was read from. A brand each customer runs at its own host writes it as a
    # template whose ``{name}`` parts are filled from its setup fields (``"{base_url}"``).
    url: str = ""
    # What setup asks for besides the credential (the GraphQL brands' SetupField).
    setup_fields: tuple[SetupField, ...] = ()
    # REQ-2265: where the running instance publishes its spec, a template filled from the setup
    # fields as ``url`` is (``"{base_url}/swagger.json"``): an instance may serve its spec beside
    # its API rather than under it (OpenMetadata: the API under /api, the spec at the host's
    # root). Empty: the spec ships with Provisa (``_known_specs/<id>.json.gz``). Set: the
    # source's spec location is that address, read at registration and again at every refresh,
    # as a steward's own spec URL is.
    instance_spec_url: str = ""
    # REQ-2265: the credential check as a whole address, a template filled from the setup fields,
    # for a brand whose releases write the same call under different paths of different servers.
    # Empty: ``verify_path`` under the source's address.
    verify_at: str = ""
    auth_type: BrandAuthType = "bearer"
    # The token endpoint of a client credentials brand, a template filled from its setup fields;
    # and the scope it asks for, None for none.
    token_url: str = ""
    scope: str | None = None

    def __post_init__(self) -> None:
        if self.instance_spec_url and self.version_header is not None:
            raise ValueError(
                f"{self.label}: a version header carries the shipped spec's version, and a spec "
                "read from the instance is not shipped"
            )
        if (self.auth_type == "oauth2_client_credentials") != bool(self.token_url):
            raise ValueError(f"{self.label}: a token_url goes with client credentials auth only")

    @property
    def ships_spec(self) -> bool:
        return not self.instance_spec_url

    @property
    def address_from_spec(self) -> bool:
        """Whether its calls go where its running instance's spec says (no ``url`` of its own)."""
        return not self.url and not self.ships_spec

    @property
    def spec_path(self) -> str:
        """The shipped spec's location (``brand:<id>``), for a brand that ships one."""
        if not self.ships_spec:
            raise ValueError(f"{self.label} reads its spec from the instance; it ships none")
        return f"{SPEC_SCHEME}{self.id}"

    def setup(self, given: dict[str, str] | None) -> dict[str, str]:
        """The setup values a source is added with (graphql_remote.brands.check_setup)."""
        return check_setup(self.label, self.setup_fields, given)

    def base_url(self, setup: dict[str, str], spec: dict | None = None) -> str:
        """Where a source added with ``setup`` (as :meth:`setup` returns it) is called. A brand
        whose address comes from its instance's spec (:attr:`address_from_spec`) is given that
        spec."""
        if self.url:
            return self.url.format(**setup)
        if self.ships_spec:
            return self.spec()["servers"][0]["url"]
        if spec is None:
            raise ValueError(f"{self.label} is called where its instance's spec says; give it")
        from provisa.openapi.loader import spec_server_url

        return spec_server_url(spec, self.source_spec_path(setup))

    def verify_url(self, setup: dict[str, str], base_url: str) -> str:
        """The address of the call that succeeds only with a working credential."""
        if self.verify_at:
            return self.verify_at.format(**setup)
        return base_url.rstrip("/") + self.verify_path

    def source_spec_path(self, setup: dict[str, str]) -> str:
        """The spec location a source added with ``setup`` records (``sources.path``)."""
        if self.ships_spec:
            return self.spec_path
        return self.instance_spec_url.format(**setup)

    def auth(self, token: str) -> dict:
        return {"type": "bearer", "token": token}

    def credential_auth(
        self,
        setup: dict[str, str],
        *,
        token: str = "",
        client_id: str = "",
        client_secret: str = "",
    ) -> dict:
        """The generic source's ``auth_config`` for the credential as entered (a value, or a
        reference the operator typed). Raises :class:`CredentialMissing` for a part its auth
        type takes that was not given."""
        if self.auth_type == "bearer":
            if not token:
                raise CredentialMissing(self.label, "token")
            return self.auth(token)
        for name, value in (("client_id", client_id), ("client_secret", client_secret)):
            if not value:
                raise CredentialMissing(self.label, name)
        auth = {
            "type": "oauth2_client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "token_url": self.token_url.format(**setup),
        }
        if self.scope is not None:
            auth["scope"] = self.scope
        return auth

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
    # REQ-2265: a governance catalog, read live; nothing syncs. Each customer runs it at its own
    # host, which serves its REST API under /api and the spec of that API, which changes between
    # releases, at /swagger.json beside it. The credential is a bot's JWT token (Settings >
    # Bots), sent as a bearer token.
    "openmetadata": Brand(
        id="openmetadata",
        label="OpenMetadata",
        spec_url="",  # read from the instance (instance_spec_url), never baked
        url="{base_url}/api",
        setup_fields=(SetupField("base_url", BASE_URL_PATTERN),),
        instance_spec_url="{base_url}/swagger.json",
        verify_path="/v1/users/loggedInUser",
    ),
    # REQ-2265 (amended 2026-11-20): read through its OpenAPI v3 spec, which the running GMS
    # generates from its entity registry, so it differs between releases and between instances
    # with custom aspects. The GMS publishes it as the springdoc group 10-openapi-v3; the bare
    # /openapi/v3/api-docs holds its other groups, without the entity lists. Where its calls go
    # is the server that spec names: a release that writes its paths as /v3/entity/... names
    # /openapi/, one that writes /openapi/v3/entity/... names /, both on the GMS host. Each entity is a scroll-paged list (scrollId) whose rows carry
    # the aspects a read asks for. The credential is a personal access token or a service
    # account's token, sent as a bearer token; reading one user succeeds only with it.
    "datahub": Brand(
        id="datahub",
        label="DataHub",
        spec_url="",  # read from the instance (instance_spec_url), never baked
        setup_fields=(SetupField("base_url", BASE_URL_PATTERN),),
        instance_spec_url="{base_url}/openapi/v3/api-docs/10-openapi-v3",
        verify_path="",  # verify_at: one address whichever server its release's spec names
        verify_at="{base_url}/openapi/v3/entity/corpuser?count=1",
    ),
}


@lru_cache(maxsize=None)
def brand_spec(brand_id: str) -> dict:
    """The spec a brand ships with: the vendor's published one, pinned, with the brand's
    additions (:func:`_add_fields`) and without its exclusions (REQ-1957,
    ``provisa/openapi/exclusions.py``). Callers read it and never change it."""
    if brand_id not in BRANDS or not BRANDS[brand_id].ships_spec:
        raise FileNotFoundError(f"No branded OpenAPI source {brand_id!r} ships a spec")
    with gzip.open(_SPEC_DIR / f"{brand_id}.json.gz", "rb") as fh:
        spec = json.load(fh)
    additions = _SPEC_DIR / f"{brand_id}.additions.json"
    if additions.exists():
        _add_fields(brand_id, spec, json.loads(additions.read_text()))
    # REQ-1957: what the brand does not offer, applied the way an operator's own exclusions are.
    excluded = _SPEC_DIR / f"{brand_id}.exclusions.json"
    if excluded.exists():
        from provisa.openapi.exclusions import Exclusions, apply

        who = f"brand {brand_id}"
        spec = apply(spec, Exclusions.parse(json.loads(excluded.read_text()), who=who), who=who)
    return spec


def brand_excluded_operations(brand_id: str) -> dict[str, str]:
    """``operationId`` -> "METHOD path" for the operations ``brand_id`` excludes (REQ-1957), read
    against the vendor's specification as published: what a deployment compares its registered
    tables and commands with."""
    excluded = _SPEC_DIR / f"{brand_id}.exclusions.json"
    if not excluded.exists():
        return {}
    from provisa.openapi.exclusions import Exclusions, excluded_operation_ids

    with gzip.open(_SPEC_DIR / f"{brand_id}.json.gz", "rb") as fh:
        published = json.load(fh)
    listed = Exclusions.parse(json.loads(excluded.read_text()), who=f"brand {brand_id}")
    return excluded_operation_ids(published, listed)


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
