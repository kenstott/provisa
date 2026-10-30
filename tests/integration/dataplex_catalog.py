# Copyright (c) 2026 Kenneth Stott
# Canary: 49191887-1979-4d0d-a745-693e553bbbd9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Removal of what a Dataplex e2e run published: read what exists, delete only that, read it back.

Dataplex answers a delete in one of two ways. A data product, a data asset and a glossary answer
with a long-running operation (its name has ``/operations/`` in it), polled until done. An entry
link answers with the deleted link itself (``entryLinks.delete`` returns the EntryLink): there is
no operation to poll, and polling the link's own name only finds it gone. Every delete is then
read back: a resource still there after its delete raises (REQ-254).
"""

# Requirements: REQ-254, REQ-2118

from __future__ import annotations

import time
import httpx

from provisa.api.metadata_export.dataplex import RESOURCE_MANAGER, DataplexExport

API = "https://dataplex.googleapis.com/v1"
OPERATION_TIMEOUT_S = 300.0
_POLL_S = 5.0


def exists(http: httpx.Client, name: str) -> bool:
    """Whether Dataplex holds ``name``: 200 yes, 404 no, anything else raises."""
    found = http.get(f"{API}/{name}")
    if found.status_code == 404:
        return False
    found.raise_for_status()
    return True


def listed(http: httpx.Client, url: str, key: str, **params: str) -> list[dict]:
    """Every entry of a Dataplex listing, page by page. A parent that does not exist (404) holds
    none."""
    entries: list[dict] = []
    query = dict(params)
    while True:
        resp = http.get(url, params=query)
        if resp.status_code == 404:
            return entries
        page = resp.raise_for_status().json()
        entries += page.get(key, [])
        if not page.get("nextPageToken"):
            return entries
        query["pageToken"] = page["nextPageToken"]


def delete(http: httpx.Client, name: str) -> None:
    """Delete ``name``, which was just found to exist; wait out the operation when Dataplex answers
    with one; then read it back gone. Any refusal, a failed operation, or a resource still there
    raises."""
    answer = http.delete(f"{API}/{name}").raise_for_status()
    operation = answer.json() if answer.content else {}
    if "/operations/" in operation.get("name", ""):
        deadline = time.monotonic() + OPERATION_TIMEOUT_S
        while not operation.get("done"):
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Dataplex delete of {name} not done in {OPERATION_TIMEOUT_S}s")
            time.sleep(_POLL_S)
            operation = http.get(f"{API}/{operation['name']}").raise_for_status().json()
        if "error" in operation:
            raise RuntimeError(f"Dataplex delete of {name} failed: {operation['error']}")
    if exists(http, name):
        raise RuntimeError(f"Dataplex {name} still exists after its delete")


def numbered(http: httpx.Client, parent: str) -> str:
    """``parent`` with its project named by number, as a term's catalog entry names it."""
    _, project, _, location = parent.split("/")
    found = http.get(f"{RESOURCE_MANAGER}/projects/{project}")
    found.raise_for_status()
    return f"{found.json()['name']}/locations/{location}"


def remove_product(http: httpx.Client, name: str) -> None:
    """The data product ``name`` and its assets, if it exists."""
    if not exists(http, name):
        return
    for asset in listed(http, f"{API}/{name}/dataAssets", "dataAssets"):
        delete(http, asset["name"])
    delete(http, name)


def remove_glossary(http: httpx.Client, parent: str, glossary: str) -> None:
    """The glossary ``glossary``, if it exists: the entry links on each of its terms, the terms,
    the categories, then the glossary."""
    if not exists(http, glossary):
        return
    location = numbered(http, parent)
    for term in listed(http, f"{API}/{glossary}/terms", "terms"):
        entry = DataplexExport._term_entry(location, term["name"])
        for link in listed(http, f"{API}/{parent}:lookupEntryLinks", "entryLinks", entry=entry):
            delete(http, link["name"])
        delete(http, f"{glossary}/terms/{term['name'].rsplit('/', 1)[-1]}")
    for category in listed(http, f"{API}/{glossary}/categories", "categories"):
        delete(http, category["name"])
    delete(http, glossary)
