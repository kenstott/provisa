# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2c9e14-3a6b-4d81-b5e0-2c8f6a1d9e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Microsoft Purview account and Unified Catalog governance domain made for one run, and deleted
after it (maintainer 2026-11-10, "Paid cloud resources": create, test, destroy).

A standing Purview account (provisa-e2e-purview) billed for three weeks. Now the Purview e2e
creates its own: the account, through Azure Resource Manager, in the test resource group
(``FABRIC_RESOURCE_GROUP``, where the CI principal provisions the Synapse lane too), tagged with
the run (tests/integration/cloud_run_tag.py); then a governance domain named for the run, through
the Unified Catalog API. Teardown deletes the domain, then the account, and reads each back: one
still there fails the run.

A tenant holds one Purview account (https://learn.microsoft.com/en-us/purview/create-microsoft-
purview-portal: "You can only create one Microsoft Purview account per tenant"), so two runs cannot
hold one each: the live lanes share one concurrency group (integration-suite-lanes.yml), and a run
that finds another's account fails, naming it, rather than using it. The account's creator holds
the root collection's roles (Collection Admin, Data Curator), which is what the Data Map writes
need; the roles take a while to reach the data plane, which ``_await_data_plane`` waits out.

Everything is called as the CI app (AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET) with
client-credential tokens; no secret is printed.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass

import httpx

from provisa.api.metadata_export.purview import API_VERSION
from tests.integration.cloud_run_tag import run_tags, run_token

ARM = "https://management.azure.com"
ARM_SCOPE = f"{ARM}/.default"
PURVIEW_SCOPE = "https://purview.azure.net/.default"
_ACCOUNT_API = "2021-12-01"
_GROUP_API = "2021-04-01"
_SUBSCRIPTIONS_API = "2020-01-01"
_PROVISIONED_TIMEOUT_S = 1800.0  # an account's create has taken several minutes
_DELETED_TIMEOUT_S = 1800.0
_DATA_PLANE_TIMEOUT_S = 900.0  # the creator's collection roles reaching the data plane
_POLL_S = 15.0


@dataclass(frozen=True)
class Account:
    """The run's account: its ARM url, its Data Map endpoint and its governance domain."""

    arm_url: str
    name: str
    endpoint: str
    domain_name: str
    domain_id: str


def token(creds: dict[str, str], scope: str) -> str:
    response = httpx.post(
        f"https://login.microsoftonline.com/{creds['AZURE_TENANT_ID']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": creds["AZURE_CLIENT_ID"],
            "client_secret": creds["AZURE_CLIENT_SECRET"],
            "scope": scope,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _client(creds: dict[str, str], scope: str) -> httpx.Client:
    return httpx.Client(headers={"Authorization": f"Bearer {token(creds, scope)}"}, timeout=120)


def catalog(creds: dict[str, str]) -> str:
    return (
        f"https://{creds['AZURE_TENANT_ID']}-api.purview-service.microsoft.com"
        "/datagovernance/catalog"
    )


def _group_url(arm: httpx.Client, group: str) -> tuple[str, str]:
    """``(ARM url, location)`` of resource group ``group`` in whichever subscription the CI app
    sees holds it."""
    subs = arm.get(f"{ARM}/subscriptions", params={"api-version": _SUBSCRIPTIONS_API})
    subs.raise_for_status()
    for sub in subs.json().get("value", []):
        url = f"{ARM}/subscriptions/{sub['subscriptionId']}/resourceGroups/{group}"
        got = arm.get(url, params={"api-version": _GROUP_API})
        if got.status_code == 200:
            return url, got.json()["location"]
        if got.status_code not in (403, 404):
            got.raise_for_status()
    raise RuntimeError(f"resource group {group!r} is in no subscription the CI app can read")


def _existing_accounts(arm: httpx.Client, group_url: str) -> list[str]:
    listed = arm.get(
        f"{group_url}/providers/Microsoft.Purview/accounts", params={"api-version": _ACCOUNT_API}
    )
    listed.raise_for_status()
    return [a["name"] for a in listed.json().get("value", [])]


def _wait(what: str, done: Callable[[], bool], bound_s: float) -> None:
    deadline = time.monotonic() + bound_s
    while not done():
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{what} not done after {bound_s:.0f}s")
        time.sleep(_POLL_S)


def _create_account(arm: httpx.Client, group_url: str, location: str, name: str) -> str:
    """Create the run's account and wait until it is provisioned; its ARM url."""
    url = f"{group_url}/providers/Microsoft.Purview/accounts/{name}"
    created = arm.put(
        url,
        params={"api-version": _ACCOUNT_API},
        json={
            "location": location,
            "identity": {"type": "SystemAssigned"},
            "tags": run_tags(),
            "properties": {"publicNetworkAccess": "Enabled"},
        },
    )
    if created.status_code not in (200, 201, 202):
        raise RuntimeError(f"Purview account {name} not created: HTTP {created.status_code}: "
                           f"{created.text[:1000]}")  # fmt: skip

    def provisioned() -> bool:
        got = arm.get(url, params={"api-version": _ACCOUNT_API})
        got.raise_for_status()
        state = got.json()["properties"]["provisioningState"]
        if state in ("Failed", "Canceled", "Deleting"):
            raise RuntimeError(f"Purview account {name} provisioning ended {state}")
        return state == "Succeeded"

    _wait(f"Purview account {name} provisioning", provisioned, _PROVISIONED_TIMEOUT_S)
    return url


def _await_data_plane(creds: dict[str, str], endpoint: str) -> None:
    """Wait until the Data Map admits the CI app: its collection roles as the account's creator
    reach the data plane some minutes after the account is provisioned."""
    last = ""

    def admitted() -> bool:
        nonlocal last
        with _client(creds, PURVIEW_SCOPE) as http:
            got = http.get(f"{endpoint}/api/atlas/v2/types/typedefs/headers")
        if got.status_code == 200:
            return True
        if got.status_code not in (401, 403, 404):
            got.raise_for_status()
        last = f"HTTP {got.status_code}: {got.text[:300]}"
        return False

    try:
        _wait(f"the Data Map of {endpoint} admitting the CI app", admitted, _DATA_PLANE_TIMEOUT_S)
    except RuntimeError as exc:
        raise RuntimeError(f"{exc}; last answer {last}") from exc


def _create_domain(creds: dict[str, str], name: str) -> str:
    """Create the run's governance domain, published (a data product is published only in a
    published domain); its id."""
    with _client(creds, PURVIEW_SCOPE) as http:
        made = http.post(
            f"{catalog(creds)}/businessdomains",
            params={"api-version": API_VERSION},
            json={
                "name": name,
                "description": "Provisa e2e: made for one run and deleted after it.",
                "type": "FunctionalUnit",
                "status": "Published",
            },
        )
    if made.status_code not in (200, 201):
        raise RuntimeError(
            f"governance domain {name} not created: HTTP {made.status_code}: {made.text[:1000]}"
        )
    return made.json()["id"]


def domains_named(creds: dict[str, str], name: str) -> list[dict]:
    """The governance domains named ``name``."""
    items: list[dict] = []
    with _client(creds, PURVIEW_SCOPE) as http:
        response = http.get(
            f"{catalog(creds)}/businessdomains", params={"api-version": API_VERSION}
        )
        while True:
            response.raise_for_status()
            page = response.json()
            items.extend(d for d in page.get("value", []) if d.get("name") == name)
            if not page.get("nextLink"):
                return items
            response = http.get(page["nextLink"])


def _delete_domain(creds: dict[str, str], domain_id: str, name: str) -> None:
    """Unpublish the run's domain, delete it, read it back gone. Its products and terms are
    deleted first (the test's own teardown)."""
    url = f"{catalog(creds)}/businessdomains/{domain_id}"
    params = {"api-version": API_VERSION}
    with _client(creds, PURVIEW_SCOPE) as http:
        got = http.get(url, params=params)
        if got.status_code == 200 and str(got.json().get("status", "")).upper() != "DRAFT":
            http.put(url, params=params, json={**got.json(), "status": "Draft"}).raise_for_status()
        if got.status_code != 404:
            deleted = http.delete(url, params=params)
            if deleted.status_code not in (200, 204, 404):
                raise RuntimeError(
                    f"governance domain {name} not deleted: HTTP {deleted.status_code}: "
                    f"{deleted.text[:500]}"
                )
    left = domains_named(creds, name)
    if left:
        raise AssertionError(f"governance domain {name} is still there after its delete: {left}")


def _delete_account(creds: dict[str, str], url: str, name: str) -> None:
    """Delete the run's account and read it back gone."""
    with _client(creds, ARM_SCOPE) as arm:
        deleted = arm.delete(url, params={"api-version": _ACCOUNT_API})
        if deleted.status_code not in (200, 202, 204, 404):
            raise RuntimeError(
                f"Purview account {name} not deleted: HTTP {deleted.status_code}: "
                f"{deleted.text[:500]}"
            )

        def gone() -> bool:
            got = arm.get(url, params={"api-version": _ACCOUNT_API})
            if got.status_code == 404:
                return True
            got.raise_for_status()
            return False

        try:
            _wait(f"Purview account {name} deletion", gone, _DELETED_TIMEOUT_S)
        except RuntimeError as exc:
            raise AssertionError(f"Purview account {name} is still there: {exc}") from exc


@contextmanager
def purview_account(creds: dict[str, str], group: str, domain_stem: str) -> Generator[Account]:
    """The run's Purview account and governance domain (``<domain_stem>-<run>``), deleted and read
    back gone on exit however the block ends. Every teardown step is attempted; their failures
    are raised together (tests/integration/live_teardown.run_every)."""
    from tests.integration.live_teardown import run_every

    name = f"provisa-pv-{run_token()}"[:63]
    domain_name = f"{domain_stem}-{run_token()}"
    with _client(creds, ARM_SCOPE) as arm:
        group_url, location = _group_url(arm, group)
        standing = _existing_accounts(arm, group_url)
        if standing:
            raise RuntimeError(
                f"the test resource group already holds Purview account(s) {standing}: a tenant "
                "holds one, and a run never uses another's (the cloud audit deletes one whose run "
                "is over)"
            )
        url = f"{group_url}/providers/Microsoft.Purview/accounts/{name}"
        domain_id: str | None = None
        try:
            _create_account(arm, group_url, location, name)
            endpoint = f"https://{name}.purview.azure.com"
            _await_data_plane(creds, endpoint)
            domain_id = _create_domain(creds, domain_name)
            yield Account(url, name, endpoint, domain_name, domain_id)
        finally:
            steps: list[tuple[str, Callable[[], object]]] = []
            if domain_id is not None:
                made = domain_id
                steps.append(
                    (
                        f"delete governance domain {domain_name}",
                        lambda: _delete_domain(creds, made, domain_name),
                    )
                )
            else:
                # A create that failed after the service made the domain still leaves it.
                steps.append(
                    (
                        f"find and delete governance domain {domain_name}",
                        lambda: [
                            _delete_domain(creds, d["id"], domain_name)
                            for d in domains_named(creds, domain_name)
                        ],
                    )
                )
            steps.append(
                (f"delete Purview account {name}", lambda: _delete_account(creds, url, name))
            )
            run_every(steps)
