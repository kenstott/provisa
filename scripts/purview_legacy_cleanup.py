#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1d4c52-3a0e-4f6b-9c18-5d2e8a7f1b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One-off inventory and cleanup of what earlier Purview e2e runs left under the old fixed names
(REQ-254). Runs only as the purview-legacy-cleanup workflow: the account's credentials exist
only as repository secrets, read here from the environment and never printed.

``list`` is read-only. It finds the legacy objects and writes a markdown table to
$GITHUB_STEP_SUMMARY and a JSON file of their ids. ``delete --ids <json>`` deletes exactly the
ids in that JSON, in dependency order, attempts every delete, and exits non-zero listing every
failure.

Never listed, never deleted: the governance domain ``provisa-e2e-sales`` and the ``provisa_*``
type definitions (REQ-254: type definitions a target registers stay).
"""

# Requirements: REQ-254

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

from provisa.api.metadata_export.atlas import (
    CLUSTER,
    PROVISA_COLUMN_TYPE,
    PROVISA_DATA_PRODUCT_TYPE,
    PROVISA_SOURCE_TYPE,
    PROVISA_TABLE_TYPE,
)
from provisa.api.metadata_export.purview import API_VERSION

DOMAIN = "provisa-e2e-sales"
TERM_NAMES = frozenset({"customer", "order"})
GLOSSARY_QN = f"provisa_glossary.mdx_purview_e2e@{CLUSTER}"
SOURCE_QN = f"mdx-wh@{CLUSTER}"
TABLE_QN_PREFIX = "mdx-wh.mdx_e2e."
DATA_PRODUCT_QN_PREFIX = "data-product."
CREDENTIALS = (
    "PROVISA_E2E_PURVIEW_ENDPOINT",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
)
PURVIEW_SCOPE = "https://purview.azure.net/.default"
DRAFT = "DRAFT"
PRODUCT_CATEGORIES = ("DATAASSET",)
TERM_CATEGORIES = ("DATAASSET", "TERM")
# The keys of the ids JSON, in the order delete works through them.
ID_KINDS = ("products", "terms", "data_assets", "entities", "glossary_terms", "glossaries")


class Account(Protocol):
    """The Purview calls this tool makes; HttpAccount is the live one, tests give a fake."""

    def uc_list(self, kind: str) -> list[dict]: ...
    def uc_get(self, kind: str, item_id: str) -> dict: ...
    def uc_relationships(self, kind: str, item_id: str, category: str) -> list[dict]: ...
    def uc_delete_relationship(self, kind: str, item_id: str, category: str, rel: dict) -> None: ...
    def uc_put(self, kind: str, item_id: str, body: dict) -> None: ...
    def uc_delete(self, kind: str, item_id: str) -> None: ...
    def assets_over(self, guids: list[str]) -> list[dict]: ...
    def asset_delete(self, asset_id: str) -> None: ...
    def dm_search(self, type_name: str) -> list[dict]: ...
    def dm_entities(self, guids: list[str]) -> list[dict]: ...
    def dm_delete_entities(self, guids: list[str]) -> None: ...
    def dm_glossaries(self) -> list[dict]: ...
    def dm_glossary_terms(self, glossary_guid: str) -> list[dict]: ...
    def dm_delete_glossary_term(self, guid: str) -> None: ...
    def dm_delete_glossary(self, guid: str) -> None: ...


# ---------------------------------------------------------------- selection


@dataclass(frozen=True)
class Row:
    kind: str
    name: str
    id: str
    status: str
    created: str


def _cell(value: Any) -> str:
    # Display only: a column the service did not return shows as "-" in the table; the id lists
    # that delete acts on never take this value.
    return "-" if value is None else str(value)


def _uc_row(kind: str, item: dict) -> Row:
    return Row(
        kind,
        item["name"],
        item["id"],
        _cell(item.get("status")),
        _cell((item.get("systemData") or {}).get("createdAt")),
    )


def is_legacy_data_map_name(type_name: str, qn: str) -> bool:
    """A Data Map entity of the old fixed names: the source, its tables and columns
    (``mdx-wh.mdx_e2e.*``), or a data product entity. Never a type definition."""
    if not qn.endswith(f"@{CLUSTER}"):
        return False
    if type_name == PROVISA_SOURCE_TYPE:
        return qn == SOURCE_QN
    if type_name in (PROVISA_TABLE_TYPE, PROVISA_COLUMN_TYPE):
        return qn.startswith(TABLE_QN_PREFIX)
    if type_name == PROVISA_DATA_PRODUCT_TYPE:
        return qn.startswith(DATA_PRODUCT_QN_PREFIX)
    return False


def _guid_time(entity: dict) -> str:
    return _cell(entity.get("createTime"))


def find_legacy(account: Account) -> tuple[list[Row], dict[str, list[str]]]:
    """Every legacy object, as table rows and as the ids ``delete`` takes."""
    rows: list[Row] = []
    ids: dict[str, list[str]] = {k: [] for k in ID_KINDS}

    products = account.uc_list("dataProducts")
    terms = [t for t in account.uc_list("terms") if t["name"] in TERM_NAMES]
    for kind, key, items in (("data product", "products", products), ("term", "terms", terms)):
        for item in items:
            rows.append(_uc_row(kind, item))
            ids[key].append(item["id"])
    # Relationships are listed for the report: they go with their product or term on delete.
    for kind, plural, items, categories in (
        ("dataProducts", "data product", products, PRODUCT_CATEGORIES),
        ("terms", "term", terms, TERM_CATEGORIES),
    ):
        for item in items:
            for category in categories:
                for rel in account.uc_relationships(kind, item["id"], category):
                    rows.append(
                        Row(
                            f"{plural} {category} relationship",
                            f"{item['name']} -> {rel['entityId']}",
                            item["id"],
                            "-",
                            "-",
                        )
                    )

    guids: list[str] = []
    for type_name in (
        PROVISA_SOURCE_TYPE,
        PROVISA_TABLE_TYPE,
        PROVISA_COLUMN_TYPE,
        PROVISA_DATA_PRODUCT_TYPE,
    ):
        for hit in account.dm_search(type_name):
            if is_legacy_data_map_name(type_name, hit["qualifiedName"]):
                guids.append(hit["id"])
    guids = sorted(set(guids))
    for entity in account.dm_entities(guids) if guids else []:
        attrs = entity["attributes"]
        rows.append(
            Row(
                f"data map {entity['typeName']}",
                attrs["qualifiedName"],
                entity["guid"],
                _cell(entity.get("status")),
                _guid_time(entity),
            )
        )
        ids["entities"].append(entity["guid"])
    for asset in account.assets_over(guids) if guids else []:
        rows.append(
            Row(
                "data asset",
                f"over {asset['source']['assetId']}",
                asset["id"],
                _cell(asset.get("status")),
                _cell((asset.get("systemData") or {}).get("createdAt")),
            )
        )
        ids["data_assets"].append(asset["id"])

    for glossary in account.dm_glossaries():
        if glossary.get("qualifiedName") != GLOSSARY_QN:
            continue
        rows.append(
            Row("glossary", GLOSSARY_QN, glossary["guid"], "-", _cell(glossary.get("createTime")))
        )
        ids["glossaries"].append(glossary["guid"])
        for term in account.dm_glossary_terms(glossary["guid"]):
            rows.append(
                Row(
                    "glossary term",
                    term["name"],
                    term["guid"],
                    _cell(term.get("status")),
                    _cell(term.get("createTime")),
                )
            )
            ids["glossary_terms"].append(term["guid"])
    return rows, ids


def render_table(rows: list[Row]) -> str:
    lines = [
        "| kind | name / qualifiedName | id / guid | status | created |",
        "|---|---|---|---|---|",
    ]
    lines += [
        f"| {r.kind} | {r.name.replace('|', chr(92) + '|')} | {r.id} | {r.status} | {r.created} |"
        for r in rows
    ]
    return "\n".join(lines) + "\n"


# ----------------------------------------------------------------- deletion


def parse_ids(text: str) -> dict[str, list[str]]:
    """The ids JSON: exactly the ID_KINDS keys' lists of id strings; anything else is refused."""
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("ids must be a JSON object")
    unknown = sorted(set(data) - set(ID_KINDS))
    if unknown:
        raise ValueError(f"unknown id kinds {unknown}; expected {list(ID_KINDS)}")
    parsed: dict[str, list[str]] = {}
    for key in ID_KINDS:
        values = data.get(key)
        if not isinstance(values, list) or not all(isinstance(v, str) and v for v in values):
            raise ValueError(f"ids[{key!r}] must be a list of id strings")
        parsed[key] = values
    return parsed


@dataclass
class Deleter:
    """Runs deletes, each attempted whatever the others answered. A failure is kept and reported
    together at the end, never dropped."""

    failures: list[str] = field(default_factory=list)

    def step(self, what: str, call: Callable[[], Any]) -> Any:
        try:
            return call()
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            self.failures.append(f"{what}: {exc!r}")
            return None


def _remove_uc(
    d: Deleter,
    account: Account,
    kind: str,
    item_id: str,
    categories: tuple[str, ...],
    release: list[tuple[str, str]],
) -> None:
    """Phase 1 for one product or term: remove its relationships. The item is fetched for the
    unpublish and delete phases; ``release`` collects it for them."""
    what = f"{kind} {item_id}"
    item = d.step(f"{what}: read", lambda: account.uc_get(kind, item_id))
    if item is None:
        return
    for category in categories:
        rels = d.step(
            f"{what}: list {category} relationships",
            lambda category=category: account.uc_relationships(kind, item_id, category),
        )
        for rel in rels or []:
            d.step(
                f"{what}: remove {category} relationship to {rel['entityId']}",
                lambda category=category, rel=rel: account.uc_delete_relationship(
                    kind, item_id, category, rel
                ),
            )
    release.append((item_id, str(item.get("status", "")).upper()))
    # The item body is needed again for the PUT; read once more at that step.


def delete_ids(account: Account, ids: dict[str, list[str]]) -> list[str]:
    """Delete exactly ``ids``, in dependency order; the failures, each a message."""
    d = Deleter()
    pending: dict[str, list[tuple[str, str]]] = {"dataProducts": [], "terms": []}

    # 1. product/term DATAASSET (and TERM) relationships
    for kind, key, categories in (
        ("dataProducts", "products", PRODUCT_CATEGORIES),
        ("terms", "terms", TERM_CATEGORIES),
    ):
        for item_id in ids[key]:
            _remove_uc(d, account, kind, item_id, categories, pending[kind])
    # 2. product/term to draft
    for kind in ("dataProducts", "terms"):
        for item_id, status in pending[kind]:
            if status == DRAFT:
                continue
            item = d.step(
                f"{kind} {item_id}: read for unpublish", lambda: account.uc_get(kind, item_id)
            )
            if item is not None:
                d.step(
                    f"{kind} {item_id}: set to draft",
                    lambda: account.uc_put(kind, item_id, {**item, "status": DRAFT}),
                )
    # 3. products, 4. terms
    for kind, key in (("dataProducts", "products"), ("terms", "terms")):
        for item_id in ids[key]:
            d.step(f"{kind} {item_id}: delete", lambda: account.uc_delete(kind, item_id))
    # 5. data assets
    for asset_id in ids["data_assets"]:
        d.step(f"data asset {asset_id}: delete", lambda: account.asset_delete(asset_id))
    # 6. Data Map entities, one bulk call
    if ids["entities"]:
        d.step(
            "Data Map: bulk-delete entities", lambda: account.dm_delete_entities(ids["entities"])
        )
    # 7. glossary terms, 8. glossaries
    for guid in ids["glossary_terms"]:
        d.step(f"glossary term {guid}: delete", lambda: account.dm_delete_glossary_term(guid))
    for guid in ids["glossaries"]:
        d.step(f"glossary {guid}: delete", lambda: account.dm_delete_glossary(guid))
    return d.failures


# ------------------------------------------------------------------ the live account


class HttpAccount:
    def __init__(self, env: dict[str, str]) -> None:
        self._tenant = env["AZURE_TENANT_ID"]
        self._map = env["PROVISA_E2E_PURVIEW_ENDPOINT"].rstrip("/")
        token = httpx.post(
            f"https://login.microsoftonline.com/{self._tenant}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": env["AZURE_CLIENT_ID"],
                "client_secret": env["AZURE_CLIENT_SECRET"],
                "scope": PURVIEW_SCOPE,
            },
            timeout=60,
        )
        token.raise_for_status()
        self._http = httpx.Client(
            headers={"Authorization": f"Bearer {token.json()['access_token']}"}, timeout=60
        )
        self._domain: str | None = None

    @property
    def _catalog(self) -> str:
        return f"https://{self._tenant}-api.purview-service.microsoft.com/datagovernance/catalog"

    @property
    def _atlas(self) -> str:
        return f"{self._map}/api/atlas/v2"

    def _ok(self, response: httpx.Response) -> httpx.Response:
        response.raise_for_status()
        return response

    def _paged(self, url: str, **params: str) -> list[dict]:
        items: list[dict] = []
        response = self._http.get(url, params={"api-version": API_VERSION, **params})
        while True:
            self._ok(response)
            page = response.json()
            items.extend(page.get("value", []))
            if not page.get("nextLink"):
                return items
            response = self._http.get(page["nextLink"])

    def _domain_id(self) -> str:
        if self._domain is None:
            domains = {d["name"]: d["id"] for d in self._paged(f"{self._catalog}/businessdomains")}
            self._domain = domains[DOMAIN]
        return self._domain

    def uc_list(self, kind: str) -> list[dict]:
        return self._paged(f"{self._catalog}/{kind}", domainId=self._domain_id())

    def uc_get(self, kind: str, item_id: str) -> dict:
        return self._ok(
            self._http.get(f"{self._catalog}/{kind}/{item_id}", params={"api-version": API_VERSION})
        ).json()

    def uc_relationships(self, kind: str, item_id: str, category: str) -> list[dict]:
        return self._paged(f"{self._catalog}/{kind}/{item_id}/relationships", entityType=category)

    def uc_delete_relationship(self, kind: str, item_id: str, category: str, rel: dict) -> None:
        params = {"api-version": API_VERSION, "entityType": category, "entityId": rel["entityId"]}
        if rel.get("relationshipType"):
            params["relationshipType"] = rel["relationshipType"]
        self._ok(
            self._http.delete(f"{self._catalog}/{kind}/{item_id}/relationships", params=params)
        )

    def uc_put(self, kind: str, item_id: str, body: dict) -> None:
        self._ok(
            self._http.put(
                f"{self._catalog}/{kind}/{item_id}", params={"api-version": API_VERSION}, json=body
            )
        )

    def uc_delete(self, kind: str, item_id: str) -> None:
        self._ok(
            self._http.delete(
                f"{self._catalog}/{kind}/{item_id}", params={"api-version": API_VERSION}
            )
        )

    def assets_over(self, guids: list[str]) -> list[dict]:
        response = self._ok(
            self._http.post(
                f"{self._catalog}/dataAssets/query",
                params={"api-version": API_VERSION},
                json={"sourceAssetIds": sorted(guids)},
            )
        )
        return response.json().get("value", [])

    def asset_delete(self, asset_id: str) -> None:
        self._ok(
            self._http.delete(
                f"{self._catalog}/dataAssets/{asset_id}", params={"api-version": API_VERSION}
            )
        )

    def dm_search(self, type_name: str) -> list[dict]:
        """Every live Data Map entity of ``type_name`` as {"id", "qualifiedName"}, through Atlas
        basic search on the same Data Map base the adapter publishes to (``{endpoint}/api/atlas/v2``)."""
        hits: list[dict] = []
        offset = 0
        while True:
            body = self._ok(
                self._http.post(
                    f"{self._atlas}/search/basic",
                    json={
                        "typeName": type_name,
                        "excludeDeletedEntities": True,
                        "limit": 1000,
                        "offset": offset,
                    },
                )
            ).json()
            page = body.get("entities") or []
            hits.extend(
                {"id": e["guid"], "qualifiedName": e["attributes"]["qualifiedName"]} for e in page
            )
            offset += len(page)
            if len(page) < 1000:
                return hits

    def dm_entities(self, guids: list[str]) -> list[dict]:
        entities: list[dict] = []
        for start in range(0, len(guids), 50):
            response = self._ok(
                self._http.get(
                    f"{self._atlas}/entity/bulk",
                    params=[
                        *[("guid", g) for g in guids[start : start + 50]],
                        ("minExtInfo", "true"),
                        ("ignoreRelationships", "true"),
                    ],
                )
            )
            entities.extend(response.json()["entities"])
        return entities

    def dm_delete_entities(self, guids: list[str]) -> None:
        self._ok(
            self._http.delete(
                f"{self._atlas}/entity/bulk", params=[("guid", g) for g in sorted(guids)]
            )
        )

    def dm_glossaries(self) -> list[dict]:
        return self._ok(self._http.get(f"{self._atlas}/glossary")).json() or []

    def dm_glossary_terms(self, glossary_guid: str) -> list[dict]:
        return (
            self._ok(
                self._http.get(
                    f"{self._atlas}/glossary/{glossary_guid}/terms", params={"limit": 1000}
                )
            ).json()
            or []
        )

    def dm_delete_glossary_term(self, guid: str) -> None:
        self._ok(self._http.delete(f"{self._atlas}/glossary/term/{guid}"))

    def dm_delete_glossary(self, guid: str) -> None:
        self._ok(self._http.delete(f"{self._atlas}/glossary/{guid}"))


# ---------------------------------------------------------------------- CLI


def _env() -> dict[str, str]:
    missing = [n for n in CREDENTIALS if not os.environ.get(n)]
    if missing:
        raise SystemExit(f"missing environment: {', '.join(missing)}")
    return {n: os.environ[n] for n in CREDENTIALS}


def _summary(text: str) -> None:
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as fh:
        fh.write(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    lst = sub.add_parser("list", help="read-only inventory")
    lst.add_argument("--out", required=True, help="path of the ids JSON file to write")
    dele = sub.add_parser("delete", help="delete exactly the given ids")
    dele.add_argument("--ids", required=True, help="the ids JSON (text)")
    args = parser.parse_args(argv)

    if args.mode == "delete":
        ids = parse_ids(args.ids)
    account = HttpAccount(_env())
    if args.mode == "list":
        rows, found = find_legacy(account)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(found, fh, indent=2)
        _summary(f"## Purview legacy objects ({len(rows)} rows)\n\n{render_table(rows)}\n")
        print(f"{len(rows)} rows; ids written to {args.out}")
        return 0
    failures = delete_ids(account, ids)
    if failures:
        _summary("## Purview cleanup failures\n\n" + "\n".join(f"- {f}" for f in failures) + "\n")
        print("\n".join(failures), file=sys.stderr)
        return 1
    _summary("## Purview cleanup\n\nEvery listed id deleted.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
