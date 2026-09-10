# Copyright (c) 2026 Kenneth Stott
# Canary: 4c6f2e91-3b7a-4d18-9e52-8a1c0f6d7b34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Splunk demo source (REQ-1694): an index, seven shelter-alert events, and the
Data Model that makes them a queryable table.

Why a Data Model. Both paths into Splunk (the Trino ``splunk`` connector and, on every other
engine, the bundled Calcite pgwire server of REQ-1690) wrap the SAME Apache Calcite Splunk adapter,
whose dynamic discovery lists one table per Splunk **Data Model** (``GET /services/data/models``).
There is no way to expose a bare index as a table through it, so the demo seeds a model.

Two Splunk-side gotchas, root-caused against a live container by the Trino-lane e2e
(tests/integration/test_splunk_source_e2e.py — see its module docstring for the full account) and
reproduced here because they are properties of Splunk, not of that test:

  * a root event object's ``parentName`` MUST be ``"BaseEvent"``. null/omitted/"" is accepted and
    echoed back by the REST API but makes the ``| datamodel`` SPL command fail with a misleading
    error, so the model exists and lists but never returns rows.
  * a freshly created model is app-shared and readable only by its creator; the account the
    connector authenticates as gets "not accessible" until the ACL is widened to global/``*``.

Credentials. Nothing is minted here. The demo container's admin username and password are fixed by
its own compose.yml, so both the config fragment (``${env:PROVISA_DEMO_SPLUNK_PASSWORD}``, exported
by start-ui-install.sh) and the source-to-query e2e (typed into the Sources form's username/password
fields) address Splunk with a credential that is known before the container starts. An API token is
Splunk-generated and therefore cannot be, which is why this script used to mint one and hand it over
in a file; a source registered through the UI now persists its password like any other (REQ-1695),
so the constant credential is the simpler handoff and the only one.
"""

from __future__ import annotations

import json
import os
import time

import httpx

MGMT_PORT = int(os.environ.get("PROVISA_DEMO_SPLUNK_PORT", "8089"))
HEC_PORT = int(os.environ.get("PROVISA_DEMO_SPLUNK_HEC_PORT", "8088"))
BASE = f"https://localhost:{MGMT_PORT}"
USER = "admin"
PASSWORD = "Provisa_2026!"  # noqa: S105 - the demo container's own fixed password (compose.yml)
AUTH = (USER, PASSWORD)

INDEX = "provisa_demo"
SOURCETYPE = "_json"
HEC_TOKEN_NAME = "provisa_demo_hec"
MODEL = "shelter_alerts"

# The rows the demo (and the source-to-query e2e) read back out of Splunk.
ALERTS = [
    (1, "medical", "Buddy"),
    (2, "intake", "Mittens"),
    (3, "medical", "Rex"),
    (4, "transfer", "Luna"),
    (5, "adoption_hold", "Coco"),
    (6, "intake", "Pepper"),
    (7, "transfer", "Shadow"),
]


def _wait_for_mgmt_api(client: httpx.Client) -> None:
    """Splunk's own healthcheck reports splunkd process health, not that the REST management API
    answers yet — poll the API itself."""
    deadline = time.monotonic() + 300
    last: object = None
    while time.monotonic() < deadline:
        try:
            r = client.get(
                f"{BASE}/services/server/info", params={"output_mode": "json"}, auth=AUTH
            )
            if r.status_code == 200:
                return
            last = r.status_code
        except httpx.HTTPError as exc:
            last = exc
        time.sleep(3)
    raise RuntimeError(f"splunk management API at {BASE} never became ready (last: {last!r})")


def _seed_events(client: httpx.Client) -> str:
    """HEC over plain HTTP, an index, and the seven alert events.

    Returns the ``source`` value this run stamped every event with. Priming is re-run whenever the
    unit is provisioned again and a HEC POST always APPENDS — an earlier version left two copies of
    every event (14 rows for 7) on the second run. Splunk offers no way to empty an index here: the
    ``clean`` action is not on the indexes handler in this build (404 "Invalid custom action"), and
    DELETE only marks an index disabled, after which it can be neither removed nor re-enabled
    (409 "Unable to remove disabled indexes"). So each run stamps its events with a unique
    ``source`` and the Data Model's constraint selects that one — the model exposes exactly this
    run's events, whatever the index accumulated before.
    """
    r = client.post(
        f"{BASE}/servicesNS/nobody/splunk_httpinput/data/inputs/http/http",
        params={"output_mode": "json"},
        data={"enableSSL": "0", "disabled": "0"},
        auth=AUTH,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"HEC global config failed: {r.status_code} {r.text}")

    # The management API can 503 briefly after it first answers; retry the index creation.
    deadline = time.monotonic() + 120
    while True:
        r = client.post(
            f"{BASE}/servicesNS/admin/search/data/indexes",
            params={"output_mode": "json"},
            data={"name": INDEX, "datatype": "event"},
            auth=AUTH,
        )
        if r.status_code in (200, 201) or "already exists" in r.text:
            break
        if time.monotonic() > deadline:
            raise RuntimeError(f"index {INDEX} creation never succeeded: {r.status_code} {r.text}")
        time.sleep(3)

    r = client.post(
        f"{BASE}/servicesNS/nobody/splunk_httpinput/data/inputs/http",
        params={"output_mode": "json"},
        data={
            "name": HEC_TOKEN_NAME,
            "index": INDEX,
            "indexes": INDEX,
            "sourcetype": SOURCETYPE,
        },
        auth=AUTH,
    )
    if r.status_code not in (200, 201) and "already exists" not in r.text:
        raise RuntimeError(f"HEC token creation failed: {r.status_code} {r.text}")
    r = client.get(
        f"{BASE}/servicesNS/nobody/splunk_httpinput/data/inputs/http/{HEC_TOKEN_NAME}",
        params={"output_mode": "json"},
        auth=AUTH,
    )
    r.raise_for_status()
    hec_token = r.json()["entry"][0]["content"]["token"]

    run_source = f"{INDEX}_{int(time.time())}"
    with httpx.Client(timeout=30) as hec:
        for alert_id, alert_type, animal_name in ALERTS:
            r = hec.post(
                f"http://localhost:{HEC_PORT}/services/collector",
                headers={"Authorization": f"Splunk {hec_token}"},
                json={
                    "index": INDEX,
                    "sourcetype": SOURCETYPE,
                    "source": run_source,
                    "event": {
                        "alert_id": alert_id,
                        "alert_type": alert_type,
                        "animal_name": animal_name,
                    },
                },
            )
            if r.status_code != 200 or r.json().get("code") != 0:
                raise RuntimeError(f"HEC event POST failed: {r.status_code} {r.text}")
    return run_source


def _create_data_model(client: httpx.Client, run_source: str) -> None:
    """The Data Model the Calcite adapter discovers as a table, with its ACL widened. Its
    constraint selects only ``run_source``'s events — see :func:`_seed_events`."""
    model_json = json.dumps(
        {
            "objects": [
                {
                    "objectName": MODEL,
                    "displayName": MODEL,
                    # MUST be BaseEvent — see the module docstring.
                    "parentName": "BaseEvent",
                    "lineage": MODEL,
                    "fields": [
                        {"fieldName": "alert_id", "type": "number", "displayName": "alert_id"},
                        {"fieldName": "alert_type", "type": "string", "displayName": "alert_type"},
                        {
                            "fieldName": "animal_name",
                            "type": "string",
                            "displayName": "animal_name",
                        },
                    ],
                    "calculations": [],
                    "constraints": [{"search": f'index={INDEX} source="{run_source}"'}],
                }
            ]
        }
    )
    # Remove a model left by an earlier run against this same volume, so the definition below wins.
    client.delete(
        f"{BASE}/servicesNS/nobody/search/data/models/{MODEL}",
        params={"output_mode": "json"},
        auth=AUTH,
    )
    r = client.post(
        f"{BASE}/servicesNS/nobody/search/data/models",
        params={"output_mode": "json"},
        data={"name": MODEL, "eai:data": model_json},
        auth=AUTH,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"data model creation failed: {r.status_code} {r.text}")
    r = client.post(
        f"{BASE}/servicesNS/nobody/search/data/models/{MODEL}/acl",
        params={"output_mode": "json"},
        data={
            "sharing": "global",
            "owner": "nobody",
            "perms.read": "*",
            "perms.write": "admin",
        },
        auth=AUTH,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"data model ACL update failed: {r.status_code} {r.text}")


def main() -> int:
    # The container serves the management port with its own self-signed certificate; the demo
    # source sets mapping.disable_ssl_validation for the same reason (REQ-724).
    with httpx.Client(verify=False, timeout=60) as client:  # noqa: S501 - self-signed demo cert
        _wait_for_mgmt_api(client)
        run_source = _seed_events(client)
        _create_data_model(client, run_source)
    print(
        f"splunk demo source primed: {len(ALERTS)} events in index '{INDEX}', data model "
        f"'{MODEL}' at {BASE}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
