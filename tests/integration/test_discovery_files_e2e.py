# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2a8c41-7b3d-4f96-a1c0-9d4e6f2b8a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: relationship discovery over a folder of files, on the default demo (REQ-413).

The ``test`` instance: the smoke check's isolated instance of the default demo, with one more
source registered through the admin as a person registers it: a files source on a copy of the
files demo's drop folder (CSV, Parquet, Excel, JSON), its five tables registered with the column
types the source reports. inspections.permit_id refers to building_permits.permit_id, and
neighborhood is shared by the budget, the permits and both complaints files.

The model is an OpenAI-compatible endpoint served by this test on the loopback: it records every
request and answers with the relationships a reader of those files would name, each only when its
two columns are compared in the request, one of them at a confidence under the threshold.

A run over the five tables measures every pair it proposes on the files' own values, read as the
person who started it: inspections.permit_id is contained in building_permits.permit_id, a key,
and is suggested whatever the model rated it; neighborhood is a key of no table, so its pairs are
many-to-many and scored under the threshold; complaints' addresses barely meet the permits'. It
lists every proposal it read; a reviewer accepts two, marks one not to be recommended again and
leaves the rest undecided. A run over the demo's own domains compares none of the five and
says which domains it compared; a run over every domain reads the key relationship among the other
tables.
"""

# Requirements: REQ-413

from __future__ import annotations

import json
import shutil
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import pytest
import yaml

from provisa.discovery.context import KEY_SCORE, SHARED_CEILING, measured_score
from tests.helpers import registered_id
from tests.smoke import test_main_is_startable as smoke
from tests.smoke.test_main_is_startable import REPO, Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_HEADERS = {"X-Provisa-Role": "org_admin"}
_VENDOR = "files-test-endpoint"
_MODEL = "fake-relationship-model"
#: The limits of the model the maintainer ran (claude-fable-5-2), so the windows are his.
_LIMITS = {"input_tokens": 1_000_000, "output_tokens": 128_000}
_DOMAIN = "city-files"
_SOURCE = "drop-test"

#: What a reader of the files would propose: (source table, column, target table, column,
#: confidence). Each is rated between the measure cutoff and the threshold, so each is measured
#: (REQ-413, amended 2026-11-16); the last is a guess the model itself rates lowest.
_ANSWERS = [
    ("inspections", "permit_id", "building_permits", "permit_id", 0.55),
    ("complaints_2026_08", "neighborhood", "budget_2026__budget", "neighborhood", 0.5),
    ("complaints_2026_09", "neighborhood", "budget_2026__budget", "neighborhood", 0.5),
    ("building_permits", "neighborhood", "budget_2026__budget", "neighborhood", 0.5),
    ("complaints_2026_08", "address", "building_permits", "address", 0.4),
]


def _windows(prompt: str) -> list[list[dict]]:
    """The windows of a request, as it sent them."""
    lines = prompt.split("\n")
    return [json.loads(lines[i + 1]) for i, line in enumerate(lines) if line.startswith("## ")]


def _compared(prompt: str) -> dict[tuple[str, str], tuple[int, dict]]:
    """Every compared column of a request, by (table name, column): its table's id and context."""
    out: dict[tuple[str, str], tuple[int, dict]] = {}
    for window in _windows(prompt):
        for block in window:
            table = block["table"]
            for column in block["compared_columns"]:
                out[(table["name"], column["name"])] = (int(table["id"]), column)
    return out


class _Endpoint:
    """An OpenAI-compatible endpoint: it records each request and answers with those of
    :data:`_ANSWERS` whose two columns the request compares."""

    def __init__(self) -> None:
        self.prompts: list[str] = []
        endpoint = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 -- http.server's name
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                prompt = body["messages"][-1]["content"]
                endpoint.prompts.append(prompt)
                compared = _compared(prompt)
                answer = [
                    {
                        "source_table_id": compared[(st, sc)][0],
                        "source_column": sc,
                        "target_table_id": compared[(tt, tc)][0],
                        "target_column": tc,
                        "cardinality": "many-to-one",
                        "confidence": confidence,
                        "reasoning": f"{st}.{sc} names {tt}.{tc}",
                        "suggested_name": named[0]
                        if named
                        else f"{st}-{sc}-{tt}".replace("_", "-"),
                    }
                    for st, sc, tt, tc, confidence, *named in _ANSWERS
                    if (st, sc) in compared and (tt, tc) in compared
                ]
                payload = json.dumps(
                    {
                        "id": "chatcmpl-1",
                        "object": "chat.completion",
                        "created": 0,
                        "model": body["model"],
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": json.dumps(answer)},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args) -> None:  # noqa: A002 -- the base method's name
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/v1"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self._server.shutdown()


@pytest.fixture(scope="module")
def endpoint() -> Iterator[_Endpoint]:
    served = _Endpoint()
    yield served
    served.stop()


def _gql(inst: Instance, query: str) -> dict[str, Any]:
    resp = httpx.post(
        f"{inst.url}/admin/graphql", json={"query": query}, headers=_HEADERS, timeout=300
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def _listed(inst: Instance, query: str) -> dict[str, Any]:
    """A listing of a source's schemas, tables or columns. A source whose connector is still
    loading answers STARTING and is asked again, as the admin's pages ask it again."""
    deadline = time.monotonic() + 300
    while True:
        resp = httpx.post(
            f"{inst.url}/admin/graphql", json={"query": query}, headers=_HEADERS, timeout=300
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        errors = body.get("errors") or []
        if not errors:
            return body["data"]
        assert all(e["message"].startswith("STARTING:") for e in errors), body
        assert time.monotonic() < deadline, body
        time.sleep(2)


def _ok(resp: httpx.Response) -> dict:
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.fixture(scope="module")
def instance(tmp_path_factory, endpoint) -> Iterator[Instance]:
    """The smoke check's instance of the default demo, its configuration naming this test's
    endpoint as a vendor whose model has the limits of the model the maintainer ran."""
    from _pytest.monkeypatch import MonkeyPatch

    home = tmp_path_factory.mktemp("discovery_files")
    config = yaml.safe_load((REPO / smoke.CONFIG).read_text())
    config["includes"] = [str(REPO / "config" / "provisa-install.yaml")]
    config["ai_endpoints"] = [
        {
            "id": _VENDOR,
            "style": "openai",
            "base_url": endpoint.url,
            # The OpenAI client the endpoint is called through needs a key, any key.
            "api_key_env": "DISCOVERY_FILES_ENDPOINT_KEY",
        }
    ]
    config["ai_models"] = {"model_limits": {_VENDOR: {_MODEL: _LIMITS}}}
    path = home / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    mp = MonkeyPatch()
    mp.setenv("DISCOVERY_FILES_ENDPOINT_KEY", "test-key")
    mp.setattr(smoke, "CONFIG", str(path))
    try:
        yield from smoke.instance.__wrapped__()
    finally:
        mp.undo()


@pytest.fixture(scope="module")
def files(instance: Instance, tmp_path_factory) -> dict[str, int]:
    """The drop folder's five tables, registered as the admin registers them: the source's own
    list of tables, each with the columns and types the source reports. By table name, its id."""
    drop = tmp_path_factory.mktemp("drop")
    shutil.copytree(REPO / "demo" / "named" / "files" / "drop", drop, dirs_exist_ok=True)
    _gql(
        instance,
        f'mutation {{ createDomain(input: {{id: "{_DOMAIN}", description: "A drop of files"}}) '
        "{ success message } }",
    )
    created = _gql(
        instance,
        f'mutation {{ createSource(input: {{id: "{_SOURCE}", type: "files", '
        f"path: {json.dumps(str(drop))}}}) {{ success message }} }}",
    )["createSource"]
    assert created["success"], created
    schemas = _listed(instance, f'{{ availableSchemas(sourceId: "{_SOURCE}") }}')[
        "availableSchemas"
    ]
    ids: dict[str, int] = {}
    reported: dict[str, dict[str, str]] = {}
    for schema in schemas:
        tables = _listed(
            instance,
            f'{{ availableTables(sourceId: "{_SOURCE}", schemaName: "{schema}") {{ name }} }}',
        )["availableTables"]
        for table in tables:
            name = table["name"]
            columns = _listed(
                instance,
                f'{{ availableColumnsMetadata(sourceId: "{_SOURCE}", schemaName: "{schema}", '
                f'tableName: "{name}") {{ name dataType }} }}',
            )["availableColumnsMetadata"]
            reported[name] = {c["name"]: c["dataType"] for c in columns}
            cols = ", ".join(f'{{name: "{c["name"]}", visibleTo: ["org_admin"]}}' for c in columns)
            registered = _gql(
                instance,
                f'mutation {{ registerTable(input: {{sourceId: "{_SOURCE}", '
                f'domainId: "{_DOMAIN}", schemaName: "{schema}", tableName: "{name}", '
                f"columns: [{cols}]}}) {{ success message }} }}",
            )["registerTable"]
            assert registered["success"], registered
            ids[name] = registered_id(registered["message"])
    _gql(instance, "mutation { rebuildSchemas { success message } }")
    print("the files source reports:", json.dumps(reported, sort_keys=True))
    # The key the files share is text in both: the JSON file's and the Parquet file's.
    assert reported["inspections"]["permit_id"] == reported["building_permits"]["permit_id"]
    assert reported["inspections"]["permit_id"] == "varchar"
    for table in ("budget_2026__budget", "building_permits", "complaints_2026_08"):
        assert reported[table]["neighborhood"] == "varchar"
    return ids


def _run(inst: Instance, scope: dict) -> dict:
    run = _ok(
        httpx.post(f"{inst.url}/admin/discover/runs", json=scope, headers=_HEADERS, timeout=120)
    )["run"]
    deadline = time.monotonic() + 600
    while run["status"] == "running":
        assert time.monotonic() < deadline, run
        time.sleep(1)
        run = _ok(
            httpx.get(f"{inst.url}/admin/discover/runs/{run['id']}", headers=_HEADERS, timeout=60)
        )["run"]
    assert run["status"] == "done", f"{run}\n{(inst.log_dir / 'backend.log').read_text()[-6000:]}"
    return run


def _candidates(inst: Instance) -> list[dict]:
    return httpx.get(f"{inst.url}/admin/discover/candidates", headers=_HEADERS, timeout=60).json()


def _proposals(inst: Instance, run_id: int) -> list[dict]:
    """Every proposal of the run no reviewer has decided, the highest score first."""
    listed = _ok(
        httpx.get(
            f"{inst.url}/admin/discover/runs/{run_id}/proposals", headers=_HEADERS, timeout=60
        )
    )
    assert listed["threshold"] == 0.7
    return listed["proposals"]


def _scope(**sets: Any) -> dict:
    """A run as the dialog sends it by default: Key value overlap chosen, with its settings."""
    return {
        "kinds": ["foreign_keys", "semantics", "key_values"],
        "measure_cutoff": 0.2,
        "sample_size": 10_000,
        "vendor": _VENDOR,
        "model": _MODEL,
        **sets,
    }


def _edge(ids: dict[str, int], row: dict) -> tuple:
    names = {v: k for k, v in ids.items()}
    return (
        names.get(row["source_table_id"]),
        row["source_column"],
        names.get(row["target_table_id"]),
        row["target_column"],
    )


_PERMIT = ("inspections", "permit_id", "building_permits", "permit_id")
_ADDRESS = ("complaints_2026_08", "address", "building_permits", "address")


def test_the_files_register_with_the_types_their_source_reports(files):
    assert sorted(files) == [
        "budget_2026__budget",
        "building_permits",
        "complaints_2026_08",
        "complaints_2026_09",
        "inspections",
    ]


def test_a_run_over_the_files_suggests_their_key_and_lists_all_it_read(
    instance: Instance, endpoint: _Endpoint, files
):
    """The five tables alone: the JSON file's permit_id and the Parquet file's are text columns
    of one request, the key relationship is suggested, and what the model rated low is kept."""
    before = len(endpoint.prompts)
    run = _run(instance, _scope(source_tables=sorted(files.values())))
    prompts = endpoint.prompts[before:]
    report = run["report"]
    print("report:", json.dumps(report, sort_keys=True))
    for prompt in prompts:
        print(prompt)

    # What the run compared is said: five tables of one domain.
    assert report["source_domains"] == report["target_domains"] == [_DOMAIN]
    assert (report["source_tables"], report["target_tables"]) == (5, 5)
    assert report["target_same_as_source"] is True and report["tables_not_read"] == 0

    # One request per type group; the text one holds both permit columns and every neighborhood.
    # The files' date and timestamp columns are in no group (REQ-413): no request asks about them.
    groups = [p.split("type group '", 1)[1].split("'", 1)[0] for p in prompts]
    assert sorted(groups) == ["text", "whole_number"]
    (text,) = [p for p in prompts if "type group 'text'" in p]
    compared = _compared(text)
    for table, column in (
        ("inspections", "permit_id"),
        ("building_permits", "permit_id"),
        ("budget_2026__budget", "neighborhood"),
        ("building_permits", "neighborhood"),
        ("complaints_2026_08", "neighborhood"),
        ("complaints_2026_09", "neighborhood"),
    ):
        assert compared[(table, column)][1]["classes"] == ["unknown"], (table, column)
    assert compared[("inspections", "permit_id")][1]["sample_values"][0].startswith("BP-")
    assert compared[("building_permits", "permit_id")][1]["sample_values"][0].startswith("BP-")
    # The request says what a confidence states and where a suggestion is kept.
    assert "A relationship is suggested at 0.7 or more" in text

    # Each pair is in the band, so scored by its measure: the key relationship is the one
    # suggestion, admitted on matching key values.
    semantics = report["semantics"]
    assert (semantics["proposed"], semantics["suggested"]) == (5, 1)
    assert semantics["below_threshold"] == 4 and semantics["invalid"] == 0
    assert semantics["scored"] == 0 and semantics["scored_below_threshold"] == 0
    mine = [c for c in _candidates(instance) if c["scope"] == f"run:{run['id']}"]
    suggested = {_edge(files, c): c["confidence"] for c in mine}
    assert suggested == {_PERMIT: KEY_SCORE}

    # Nothing read is dropped: one table of the run's proposals, the highest score (the
    # measure's) first, each with the model's confidence beside it and why it is not suggested.
    listed = _proposals(instance, run["id"])
    address_score = measured_score(75 / 215, 75 / 128, True)
    assert [
        (_edge(files, r), r["confidence"], r["score"], r["cardinality"], r["reason"])
        for r in listed
    ] == [
        # 60 inspections, each of a different permit, all 60 among the 128 permits.
        (_PERMIT, 0.55, KEY_SCORE, "one-to-one", None),
        # Six neighborhoods on every side, repeated in each file: a key of none.
        *(
            (
                (table, "neighborhood", "budget_2026__budget", "neighborhood"),
                0.5,
                SHARED_CEILING,
                "many-to-many",
                "below_threshold",
            )
            for table in ("complaints_2026_08", "complaints_2026_09", "building_permits")
        ),
        # 75 of the complaints' 215 addresses are among the permits' 128, each permit's own.
        (_ADDRESS, 0.4, address_score, "many-to-one", "below_threshold"),
    ]
    by_edge = {_edge(files, r): r for r in listed}
    assert [_edge(files, r) for r in listed if r["admitted_on_key_values"]] == [_PERMIT]
    assert (semantics["measured"], semantics["admitted"]) == (5, 1)
    assert report["key_values"] == {"cutoff": 0.2, "sample_size": 10_000}
    permit = by_edge[_PERMIT]["measure"]
    assert (permit["source_in_target"], permit["source"]["values"]) == (60, 60)
    assert permit["target"]["values"] == 128 and permit["target"]["distinct"] is True
    address = by_edge[_ADDRESS]["measure"]
    assert (address["source_in_target"], address["source"]["values"]) == (75, 215)
    # A many-to-many pair is accepted as a shared dimension (REQ-413): every row can be.
    assert [r["acceptable"] for r in listed] == [True, True, True, True, True]
    assert by_edge[_ADDRESS]["reasoning"] == (
        "complaints_2026_08.address names building_permits.address"
    )

    # A reviewer accepts the key and the one under the threshold, marks one neighborhood pair
    # not to be recommended again, and leaves the other two alone.
    not_wanted = ("building_permits", "neighborhood", "budget_2026__budget", "neighborhood")
    applied = _ok(
        httpx.post(
            f"{instance.url}/admin/discover/runs/{run['id']}/apply",
            json={
                "accept": [
                    {"id": by_edge[_PERMIT]["id"], "name": "inspection-of-a-permit"},
                    {"id": by_edge[_ADDRESS]["id"], "name": "complaint-at-a-permit-address"},
                ],
                "not_again": [by_edge[not_wanted]["id"]],
            },
            headers=_HEADERS,
            timeout=120,
        )
    )["results"]
    assert [(r["action"], r["ok"]) for r in applied] == [
        ("accept", True),
        ("accept", True),
        ("not_again", True),
    ]
    registered = _gql(instance, "{ relationships { id sourceColumn targetColumn } }")
    for name, column in (
        ("inspection-of-a-permit", "permit_id"),
        ("complaint-at-a-permit-address", "address"),
    ):
        assert {"id": name, "sourceColumn": column, "targetColumn": column} in (
            registered["relationships"]
        )
    # What was decided leaves the run's table; the two left alone are undecided. Scored under
    # the threshold, they were never suggestions, so none is waiting.
    remaining = _proposals(instance, run["id"])
    assert sorted(_edge(files, r)[0] for r in remaining) == [
        "complaints_2026_08",
        "complaints_2026_09",
    ]
    waiting = {_edge(files, c) for c in _candidates(instance) if c["scope"] == f"run:{run['id']}"}
    assert waiting == set()


def test_a_run_over_other_domains_says_which_and_compares_none_of_the_files(
    instance: Instance, endpoint: _Endpoint, files
):
    """The demo's own two domains: none of the five tables is in any request, and the result
    names the domains it compared, so a table outside them is plain."""
    before = len(endpoint.prompts)
    run = _run(instance, _scope(source_domains=["pet-store", "shelter"]))
    prompts = endpoint.prompts[before:]
    assert prompts
    for prompt in prompts:
        named = {table for table, _ in _compared(prompt)}
        assert not named & set(files), named & set(files)
    report = run["report"]
    assert report["source_domains"] == ["pet-store", "shelter"]
    assert report["source_tables"] > 5
    assert report["semantics"]["proposed"] == 0
    assert report["semantics"]["empty_reason"] == "none_proposed"
    assert [r for r in _proposals(instance, run["id"]) if r["kind"] == "semantics"] == []


def test_a_run_over_every_domain_reads_the_files_key_among_the_other_tables(
    instance: Instance, endpoint: _Endpoint, files
):
    """All three domains, with the limits of a model that holds a whole type group in one request:
    the permit columns share the text request with every other text column, and every proposal is
    read again. What the first run's reviewer decided shows: the accepted ones are registered and
    cannot be decided, the one marked keeps its decision and cannot be decided, and the other
    neighborhood pairs, a key of no table, are scored under the threshold again."""
    before = len(endpoint.prompts)
    run = _run(instance, _scope(source_domains=["pet-store", "shelter", _DOMAIN]))
    prompts = endpoint.prompts[before:]
    (text,) = [p for p in prompts if "type group 'text'" in p]
    compared = _compared(text)
    assert ("inspections", "permit_id") in compared and (
        "building_permits",
        "permit_id",
    ) in compared
    assert len({table for table, _ in compared}) > 5
    report = run["report"]
    assert report["source_domains"] == [_DOMAIN, "pet-store", "shelter"]
    read = {
        _edge(files, r): (r["reason"], r["decidable"])
        for r in _proposals(instance, run["id"])
        if r["kind"] == "semantics"
    }
    assert read == {
        _PERMIT: ("existing", False),
        # Marked not to be recommended again: it keeps that decision and is not measured anew.
        ("building_permits", "neighborhood", "budget_2026__budget", "neighborhood"): (
            "rejected",
            False,
        ),
        ("complaints_2026_08", "neighborhood", "budget_2026__budget", "neighborhood"): (
            "below_threshold",
            True,
        ),
        ("complaints_2026_09", "neighborhood", "budget_2026__budget", "neighborhood"): (
            "below_threshold",
            True,
        ),
        # Rated 0.4 again, and registered since: said as registered, not to be accepted twice.
        _ADDRESS: ("existing", False),
    }
