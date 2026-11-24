# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2c4a17-b593-4f06-9d7e-a1c5f3b80e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Spark source's script validated by its host on a one-node Spark, in three steps (REQ-2225).

The source serves a script whose @query takes the governed table ``fx.clips`` (a Python source;
the analyst's row rule allows clip 1). Validating it as the analyst runs the static check, a dry
run of the @query on the cluster with ``fx.clips`` read over Arrow Flight as the analyst, and the
key check: all pass, and the dry run saw the one row the analyst may read. A bad script in the
form -- a column whose rows do not match its declared type, and a function that raises -- is
validated by the same host without being served: the dry run names both, and the source still
serves its own script. A script with an unknown tag fails the static check and runs nothing.

The cluster, the connector's host and the server are the fixtures of
tests/integration/test_spark_connector_e2e.py; nothing here touches local-dev."""

# Requirements: REQ-2225

from __future__ import annotations

import json
import textwrap

import httpx
import pytest

from tests.integration import test_spark_connector_e2e as _spark
from tests.integration.test_spark_connector_e2e import _ADMIN, _ANALYST, _TABLE_SCRIPT

pytestmark = [pytest.mark.integration]

# The one-node cluster, the connector's host and the server: test_spark_connector_e2e's fixtures.
client_python = _spark.client_python
cluster = _spark.cluster
flight_tls = _spark.flight_tls
spark_host = _spark.spark_host
provisa_server = _spark.provisa_server

_SOURCE = "spark_jobs"
_INPUTS = {"clips_on_the_cluster": {"clips": "fx.clips"}}

_BAD = textwrap.dedent(
    '''
    from dataclasses import dataclass

    from provisa_services import TableRef
    from provisa_spark import dataframe


    @dataclass
    class Clip:
        id: int
        region: str


    @dataclass
    class Tally:
        region: str
        clips: int


    def clips_on_the_cluster(clips: TableRef[Clip]) -> list[Tally]:
        """The clips per region -- each count returned as text, not the declared integer.

        @query
        """
        frame = dataframe(clips).groupBy("region").count().orderBy("region")
        return [Tally(region=row["region"], clips=f"{row['count']} clips") for row in frame.collect()]


    def broken(clips: TableRef[Clip]) -> list[Clip]:
        """A job the cluster cannot run.

        @query
        """
        return [Clip(id=row.id, region=row.region) for row in dataframe(clips).sql_nonexistent()]
    '''
)


@pytest.fixture(scope="module")
def spark_source(provisa_server, spark_host, cluster):
    """The governed ``fx.clips`` and the Spark source whose script reads it, as
    test_spark_connector_e2e's ``clips_command`` makes them; no command is registered."""
    from tests.integration.test_code_sources_e2e import _admin, _archive

    srv = provisa_server
    upload = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-uploads/archive",
        params={"filename": "ledger.zip"},
        content=_archive(_spark._LEDGER),
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=srv.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    ledger = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "ledger", type: "python", host: "", port: 0, database: "", '
        f"cacheTtl: 300, mappingJson: {json.dumps(json.dumps(ledger))}}})",
    )
    columns = ", ".join([_spark._column("id", "bigint"), _spark._column("region", "text")])
    _admin(
        srv,
        'registerTable(input: {sourceId: "ledger", domainId: "fx", schemaName: "default", '
        f'tableName: "clips", cacheTtl: 300, columns: [{columns}]}})',
    )
    _admin(
        srv,
        f'upsertRlsRule(input: {{tableId: "clips", roleId: "{_ANALYST}", filterExpr: "id = 1", '
        'description: "The analyst reads clip 1 only."})',
    )
    joining = {
        "connector_join": {
            "connector": "spark",
            "host": "127.0.0.1",
            "port": spark_host.port,
            "pairing_code": spark_host.pairing,
        },
        "settings": {"script": _TABLE_SCRIPT, "cluster": cluster},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "{_SOURCE}", type: "python", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(joining))}}})',
    )
    return srv


def _validated(srv, role: str, script: str | None, inputs: dict | None = None) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-sources/{_SOURCE}/validate",
        json={"script": script, "inputs": _INPUTS if inputs is None else inputs, "keys": {}},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _by_function(step: dict) -> dict[str, dict]:
    return {f["function"]: f for f in step["functions"]}


def test_the_served_script_passes_all_three_steps_as_the_caller(spark_source, cluster):
    report = _validated(spark_source, _ANALYST, None)
    assert report["static"]["status"] == "ok", report["static"]
    assert [f["name"] for f in report["static"]["functions"]] == ["clips_on_the_cluster"]
    assert report["cluster"] == {"address": cluster, "origin": "source"}
    assert report["dry_run"]["status"] == "ok", report["dry_run"]
    ran = _by_function(report["dry_run"])["clips_on_the_cluster"]
    # The analyst's row rule allows clip 1: the cluster was given that one row and no other.
    assert ran["rows"] == 1
    assert ran["declared"] == [
        {"name": "id", "type": "int64"},
        {"name": "region", "type": "string"},
    ]
    assert report["keys"] == {"status": "ok", "functions": []}
    # As the administrator, every clip reaches the cluster.
    assert (
        _by_function(_validated(spark_source, _ADMIN, None)["dry_run"])["clips_on_the_cluster"][
            "rows"
        ]
        == 3
    )


def test_a_bad_script_in_the_form_is_named_and_not_served(spark_source):
    inputs = {**_INPUTS, "broken": {"clips": "fx.clips"}}
    report = _validated(spark_source, _ADMIN, _BAD, inputs)
    assert report["static"]["status"] == "ok", report["static"]
    assert report["dry_run"]["status"] == "failed"
    ran = _by_function(report["dry_run"])
    tallied = ran["clips_on_the_cluster"]
    assert tallied["status"] == "mismatch"
    assert tallied["rows"] == 3
    assert any("do not convert to the declared types" in m for m in tallied["mismatches"])
    assert {"name": "clips", "type": "str"} in tallied["actual"]
    assert ran["broken"]["status"] == "error"
    assert "sql_nonexistent" in ran["broken"]["error"]
    # Validating a script does not serve it: the source still serves its own.
    assert _validated(spark_source, _ADMIN, None)["dry_run"]["status"] == "ok"


def test_an_unknown_tag_fails_the_static_check_and_nothing_runs(spark_source):
    script = _TABLE_SCRIPT.replace("@query", "@qurey")
    report = _validated(spark_source, _ADMIN, script)
    assert report["static"]["status"] == "failed"
    assert "clips_on_the_cluster: unknown tag @qurey" in report["static"]["problems"]
    assert report["dry_run"]["status"] == "not_run"
    assert report["keys"]["status"] == "not_run"
