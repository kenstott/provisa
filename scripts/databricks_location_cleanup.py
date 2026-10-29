# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e7d30-4c92-4f6a-9d08-2a7c63e1f4b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Inventory and remove Unity Catalog external locations that tests left behind (REQ-254).

Run only from .github/workflows/databricks-location-cleanup.yml. REST only: no SQL warehouse is
woken.

* ``list`` (read-only): every external location (name, url, credential, owner, created) and every
  table and volume whose storage location is the location's url or beneath it.
* ``delete --names '["x"]'``: deletes exactly the named external locations. A location that a table
  or volume still uses is refused, naming them; each deletion is read back and must be gone.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import httpx

_API = "/api/2.1/unity-catalog"
# Catalogs Databricks provides; they hold no customer storage.
_OWN_CATALOGS = {"system", "samples"}


def _client() -> httpx.Client:
    return httpx.Client(
        base_url="https://" + os.environ["DATABRICKS_SERVER_HOSTNAME"],
        headers={"Authorization": "Bearer " + os.environ["DATABRICKS_TOKEN"]},
        timeout=60,
    )


def _pages(client: httpx.Client, path: str, key: str, **params: str) -> list[dict]:
    """Every item of a paginated list endpoint: pages are read until no next_page_token."""
    items: list[dict] = []
    token: str | None = None
    while True:
        query = dict(params)
        if token:
            query["page_token"] = token
        resp = client.get(_API + path, params=query)
        resp.raise_for_status()
        body = resp.json()
        items.extend(body.get(key, []))
        token = body.get("next_page_token")
        if not token:
            return items


def _under(storage: str, url: str) -> bool:
    base = url.rstrip("/")
    return storage.rstrip("/") == base or storage.startswith(base + "/")


def _storage_objects(client: httpx.Client) -> list[dict]:
    """Every table and volume with a storage location, as {kind, name, storage_location}."""
    found: list[dict] = []
    for cat in _pages(client, "/catalogs", "catalogs"):
        if cat["name"] in _OWN_CATALOGS:
            continue
        for sch in _pages(client, "/schemas", "schemas", catalog_name=cat["name"]):
            scope = {"catalog_name": cat["name"], "schema_name": sch["name"]}
            for kind, path, key in (
                ("table", "/tables", "tables"),
                ("volume", "/volumes", "volumes"),
            ):
                for obj in _pages(client, path, key, **scope):
                    location = obj.get("storage_location")
                    if location:
                        found.append(
                            {
                                "kind": kind,
                                "name": f"{cat['name']}.{sch['name']}.{obj['name']}",
                                "storage_location": location,
                            }
                        )
    return found


def _users(objects: list[dict], url: str) -> list[str]:
    return [f"{o['kind']} {o['name']}" for o in objects if _under(o["storage_location"], url)]


def inventory(client: httpx.Client) -> list[dict]:
    objects = _storage_objects(client)
    return [
        {
            "name": loc["name"],
            "url": loc["url"],
            "credential": loc["credential_name"],
            "owner": loc["owner"],
            "created": loc["created_at"],
            "used_by": _users(objects, loc["url"]),
        }
        for loc in _pages(client, "/external-locations", "external_locations")
    ]


def report(locations: list[dict]) -> str:
    lines = [f"{len(locations)} external location(s)"]
    for loc in locations:
        lines.append(
            f"- {loc['name']}  url={loc['url']}  credential={loc['credential']}  "
            f"owner={loc['owner']}  created={loc['created']}"
        )
        lines.extend(f"    used by {u}" for u in loc["used_by"])
        if not loc["used_by"]:
            lines.append("    used by nothing")
    return "\n".join(lines)


def delete(client: httpx.Client, names: list[str]) -> str:
    existing = {loc["name"]: loc for loc in inventory(client)}
    missing = [n for n in names if n not in existing]
    if missing:
        raise SystemExit(f"no such external location: {missing}")
    in_use = {n: existing[n]["used_by"] for n in names if existing[n]["used_by"]}
    if in_use:
        raise SystemExit(f"refusing, still in use: {json.dumps(in_use)}")
    lines = []
    for name in names:
        client.delete(_API + f"/external-locations/{name}").raise_for_status()
        check = client.get(_API + f"/external-locations/{name}")
        if check.status_code != 404:
            raise SystemExit(f"{name} still exists after delete (HTTP {check.status_code})")
        lines.append(f"deleted {name}; read back: gone")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    dele = sub.add_parser("delete")
    dele.add_argument("--names", required=True, help="JSON list of external location names")
    args = parser.parse_args(argv)
    with _client() as client:
        if args.cmd == "list":
            print(report(inventory(client)))
        else:
            names = json.loads(args.names)
            if (
                not isinstance(names, list)
                or not names
                or not all(isinstance(n, str) for n in names)
            ):
                raise SystemExit("--names must be a non-empty JSON list of strings")
            print(delete(client, names))
    return 0


if __name__ == "__main__":
    sys.exit(main())
