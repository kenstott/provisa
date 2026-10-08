# Copyright (c) 2026 Kenneth Stott
# Canary: 5f8d2b60-9c47-4e13-a72f-6b0e3d9c1a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JDBC driver reads its catalog from the role-narrowed Flight catalog only (REQ-128).

The driver used to answer ``getTables``/``getColumns`` and key metadata from two queries to
``/admin/graphql``: the admin catalog, which lists every registered table and column to any
signed-in caller, and whose query text (kept in Java) had also drifted from the schema. It now
reads the server's Arrow Flight catalog, which lists what the signed-in role is served. This
holds the driver to that: no admin query is left in its source.
"""

# Requirements: REQ-128, REQ-126

from __future__ import annotations

from pathlib import Path

_DRIVER = Path(__file__).resolve().parents[2] / "jdbc-driver/src/main/java/io/provisa/jdbc"


def test_the_driver_sends_nothing_to_the_admin_api():
    for source in sorted(_DRIVER.glob("*.java")):
        text = source.read_text(encoding="utf-8")
        assert "/admin/" not in text, f"{source.name} reaches the admin API"


def test_the_catalog_is_read_from_the_flight_catalog_with_the_credential():
    transport = (_DRIVER / "FlightTransport.java").read_text(encoding="utf-8")
    assert "client.listFlights(Criteria.ALL, new HeaderCallOption(headers))" in transport
    assert 'headers.insert("authorization", "Bearer " + token)' in transport
    assert 'headers.insert("x-provisa-role", role)' in transport


def test_the_rest_document_has_the_shape_the_drivers_http_catalog_reads():
    """With the Flight port unreachable the driver reads tables and columns from the role's REST
    OpenAPI document. Its reader lives in Java; this pins what it relies on in the document the
    server generates: a ``/{domain}/{table}`` path whose GET ``fields`` parameter names
    ``<Row>Field``, and a ``<Row>`` schema whose properties are the columns."""
    from provisa.api.rest.openapi_spec import generate_rest_openapi_spec
    from tests.unit.test_openapi_spec import _make_state

    spec = generate_rest_openapi_spec(_make_state("admin"), "admin")
    table_paths = {p: item for p, item in spec["paths"].items() if len(p.split("/")) == 3}
    assert table_paths, spec["paths"].keys()
    for path, item in table_paths.items():
        (fields,) = [p for p in item["get"]["parameters"] if p["name"] == "fields"]
        ref = fields["schema"]["items"]["$ref"]
        assert ref.startswith("#/components/schemas/") and ref.endswith("Field"), (path, ref)
        row = spec["components"]["schemas"][ref.rsplit("/", 1)[1][: -len("Field")]]
        assert row["properties"], path
        assert all("type" in column for column in row["properties"].values()), row


def test_the_drivers_http_catalog_reads_that_document_and_nothing_else():
    connection = (_DRIVER / "ProvisaConnection.java").read_text(encoding="utf-8")
    assert 'baseUrl + "/data/rest/openapi.json"' in connection
