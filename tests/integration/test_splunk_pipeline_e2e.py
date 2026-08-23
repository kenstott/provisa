# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b1c0a-8d47-4e2b-9a51-7c0e8b45d219
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Splunk as a connector-source, over a CIM data model.

``test_splunk_source_e2e.py`` proves Trino can reach Splunk, using a hand-built single-purpose
data model over three synthetic ``widget`` events. This proves Provisa can serve a *governed
semantic query* over Splunk, and it does so over the Common Information Model — which is how
Splunk is actually queried in production. Nobody hand-authors a data model per question; the CIM
add-on ships 27 normalized models (Authentication, Web, Network_Traffic, …) and every technology
add-on maps its own events onto them, so one query works across every vendor's logs.

Why the CIM model is the realistic target here
----------------------------------------------
The Calcite Splunk adapter surfaces one Trino table per Splunk **Data Model**, and it has explicit
first-class CIM knowledge: ``DataModelDiscovery.addCimCalculatedFields`` switches on the
normalized model name and, for ``authentication``, adds ``action``/``app``/``user``/``src``/
``src_user``/``dest`` as VARCHAR plus ``is_failure``/``is_success`` as BOOLEAN. Those columns come
from the model's *calculations* (fillnull evals), not its declared ``fields`` list, so they exist
only because the adapter special-cases CIM. A test over a bespoke model never exercises that path.

The three Splunk-side pieces, and why they live where they do
-------------------------------------------------------------
``Splunk_SA_CIM`` (mounted from ``.splunk-cim/``, vendored by ``scripts/fetch-splunk-cim.sh``)
ships the *models only*. Getting events into one is always two separate search-time steps, owned
by a technology add-on rather than by CIM — here ``tests/fixtures/splunk_ta_provisa``, mounted at
``/opt/splunk/etc/apps/provisa_cim_ta``:

  1. **Selection** — an eventtype over this index/sourcetype, carrying ``tag = authentication``.
     The Authentication root constraint is ``` `cim_Authentication_indexes` tag=authentication NOT
     (action=success user=*$) ```, so the tag is what admits an event to the model. (The ``NOT``
     clause drops successful machine-account logins; none of the seeded users end in ``$``.)
  2. **Scope** — ``cim_Authentication_indexes``, which ships as ``definition = ()`` and is
     overridden in the add-on's ``local/`` to ``index=provisa_cim``. ``local`` outranks every app's
     ``default`` regardless of app-directory ordering, which is why it cannot sit in ``default/``
     next to the other stanzas.

Field mapping is the third step and is a no-op here only because the seeded events already use CIM
names as their JSON keys (``props.conf`` sets ``KV_MODE = json``); a real source would add
``FIELDALIAS-*`` in the same file.

CIM models ship unaccelerated, which is correct for this path: the adapter issues
``| datamodel Authentication Authentication search``, which reads raw events rather than an
acceleration summary. Splunk still indexes asynchronously, hence ``settle_seconds``.
"""

from __future__ import annotations

import time

import httpx
import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.integration.test_splunk_source_e2e import (
    _SPLUNK_HEC_PORT,
    _SPLUNK_PASSWORD,
    _SPLUNK_USER,
    _mgmt_base,
    _splunk_container_id,
    _wait_for_splunk_mgmt_api,
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Must match the add-on: eventtypes.conf selects `index=provisa_cim sourcetype=provisa:auth`, and
# local/macros.conf scopes cim_Authentication_indexes to `index=provisa_cim`.
_INDEX = "provisa_cim"
_SOURCETYPE = "provisa:auth"
_HEC_TOKEN_NAME = "provisa_cim_token"

# The CIM model, and the Trino table name the adapter normalizes it to (lowercase/underscores).
_MODEL_TABLE = "authentication"

# Events already keyed by CIM field names — see the module docstring on why that is the only
# mapping step this fixture needs. `user` is deliberately free of a trailing `$` so the model's
# root constraint does not exclude the successful logins.
_EVENTS = [
    {"action": "success", "user": "alice", "src": "10.0.0.11", "dest": "sso.example.com",
     "app": "okta"},
    {"action": "failure", "user": "bob", "src": "10.0.0.12", "dest": "sso.example.com",
     "app": "okta"},
    {"action": "success", "user": "carol", "src": "10.0.0.13", "dest": "vpn.example.com",
     "app": "anyconnect"},
]  # fmt: skip

_COLUMNS = ["user", "src", "dest", "action", "app"]

# Hyphen-free source id with an empty `database`, so createSource's recorded catalog name and
# create_catalog's physical one cannot diverge — see the harness docstring.
_SOURCE_ID = "splunk_cim"
_DOMAIN = "splunk_e2e"


def _seed_splunk_cim() -> None:
    """Enable HEC, create the index, and post the CIM-shaped auth events.

    No data model is created here: ``Splunk_SA_CIM`` already ships ``Authentication``, and the
    add-on already tags events into it. That absence is the point — this test exercises the models
    a real deployment has, not one written for the test.
    """
    _wait_for_splunk_mgmt_api()

    with httpx.Client(verify=False, timeout=30) as client:  # noqa: S501 - self-signed test cert
        auth = (_SPLUNK_USER, _SPLUNK_PASSWORD)

        r = client.post(
            f"{_mgmt_base()}/servicesNS/nobody/splunk_httpinput/data/inputs/http/http",
            params={"output_mode": "json"},
            data={"enableSSL": "0", "disabled": "0"},
            auth=auth,
        )
        assert r.status_code in (200, 201), f"HEC global config failed: {r.status_code} {r.text}"

        # The management API can 503 briefly after it first answers /services/server/info.
        deadline = time.monotonic() + 60
        last_resp: httpx.Response | None = None
        while time.monotonic() < deadline:
            r = client.post(
                f"{_mgmt_base()}/servicesNS/admin/search/data/indexes",
                params={"output_mode": "json"},
                data={"name": _INDEX, "datatype": "event"},
                auth=auth,
            )
            last_resp = r
            if r.status_code in (200, 201) or "already exists" in r.text:
                break
            time.sleep(3)
        else:
            raise RuntimeError(f"index creation never succeeded: {last_resp}")

        r = client.post(
            f"{_mgmt_base()}/servicesNS/nobody/splunk_httpinput/data/inputs/http",
            params={"output_mode": "json"},
            data={
                "name": _HEC_TOKEN_NAME,
                "index": _INDEX,
                "indexes": _INDEX,
                "sourcetype": _SOURCETYPE,
            },
            auth=auth,
        )
        assert r.status_code in (200, 201) or "already exists" in r.text, (
            f"HEC token creation failed: {r.status_code} {r.text}"
        )
        r = client.get(
            f"{_mgmt_base()}/servicesNS/nobody/splunk_httpinput/data/inputs/http/{_HEC_TOKEN_NAME}",
            params={"output_mode": "json"},
            auth=auth,
        )
        r.raise_for_status()
        token = r.json()["entry"][0]["content"]["token"]

    hec_url = f"http://localhost:{_SPLUNK_HEC_PORT}/services/collector"
    with httpx.Client(timeout=15) as hec_client:
        for event in _EVENTS:
            r = hec_client.post(
                hec_url,
                headers={"Authorization": f"Splunk {token}"},
                json={"index": _INDEX, "sourcetype": _SOURCETYPE, "event": event},
            )
            assert r.status_code == 200 and r.json().get("code") == 0, (
                f"HEC event POST failed: {r.status_code} {r.text}"
            )


@pytest.mark.requires_splunk
async def test_splunk_cim_model_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic SELECT.

    ``host="splunk"``/``port=8089`` are what *Trino* dials on the isolated stack's private network;
    the host-published ephemeral ports are used only by this process to seed. The registered
    columns are all CIM calculated fields, so a passing run also proves the adapter's CIM handling
    reaches Provisa's introspection path — ``information_schema.columns`` must report them, or
    ``_ensure_source_column_types`` refuses the registration outright.
    """
    _splunk_container_id()  # fail fast with a clear message if the service isn't up
    _seed_splunk_cim()

    expected = sorted(({c: e[c] for c in _COLUMNS} for e in _EVENTS), key=lambda row: row["user"])

    await assert_registration_and_query(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="splunk",
        host="splunk",
        port=8089,
        username=_SPLUNK_USER,
        password=_SPLUNK_PASSWORD,
        mapping={"use_token": False, "disable_ssl_validation": True},
        domain_id=_DOMAIN,
        schema_name="splunk",
        table_name=_MODEL_TABLE,
        columns=_COLUMNS,
        order_by="user",
        expected_rows=expected,
        # Splunk indexes asynchronously and the model search reads raw events, so the rows appear
        # some seconds after the HEC POSTs return 200.
        settle_seconds=180,
    )
