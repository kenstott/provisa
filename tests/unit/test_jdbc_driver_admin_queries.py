# Copyright (c) 2026 Kenneth Stott
# Canary: 5f8d2b60-9c47-4e13-a72f-6b0e3d9c1a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JDBC driver reads its catalog from the server's role-narrowed catalog only (REQ-128).

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


def test_with_the_flight_port_unreachable_the_driver_reads_the_same_catalog_over_http():
    """The driver's second source is ``/data/catalog``: the Flight listing's own builder over
    HTTP (tests/unit/test_flight_catalog_credential.py holds the two listings equal field for
    field). It used to read the role's REST OpenAPI document, whose names and types differed
    from the Flight listing's; no reader of that document is left."""
    connection = (_DRIVER / "ProvisaConnection.java").read_text(encoding="utf-8")
    assert 'baseUrl + "/data/catalog"' in connection
    for source in sorted(_DRIVER.glob("*.java")):
        text = source.read_text(encoding="utf-8")
        assert "openapi" not in text.lower(), f"{source.name} reads an OpenAPI document"
        assert "/data/rest" not in text, f"{source.name} reads the REST surface for its catalog"


def test_both_transports_listings_are_read_by_one_function():
    transport = (_DRIVER / "FlightTransport.java").read_text(encoding="utf-8")
    connection = (_DRIVER / "ProvisaConnection.java").read_text(encoding="utf-8")
    assert (
        "static CatalogTable catalogTable(String domain, String table, Schema schema)" in transport
    )
    assert "tables.add(catalogTable(path.get(0), path.get(1), schema));" in transport
    assert "FlightTransport.catalogTable(" in connection
