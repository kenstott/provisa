# Copyright (c) 2026 Kenneth Stott
# Canary: 8c14f0d9-2b7e-4a35-91c6-3f8d02e5b74a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Contract checks in the published snapshot, and each vendor's mapping of them (REQ-1443).

An assertion publishes on the asset the checker OBSERVES, never on the table the outcomes land
in — that table publishes as an ordinary table carrying the derived ``data_quality`` tag.
"""

# Requirements: REQ-1443

from __future__ import annotations

import json

from datetime import UTC, datetime

import pytest

from provisa.api.metadata_export.atlas import GOVERNANCE_ATTRIBUTE, to_entities as atlas_entities
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.collibra import DATA_QUALITY_ATTRIBUTE, to_rows
from provisa.api.metadata_export.datahub import to_proposals
from provisa.api.metadata_export.model import DataQualityOutcome
from provisa.api.metadata_export.openlineage import to_events
from provisa.api.metadata_export.openmetadata import to_entities as om_entities
from provisa.core.models import (
    Column,
    Domain,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)
from provisa.dq.contract import ContractError

# One column check carrying an explicit warn threshold and one dataset-level check with none, so
# the severity mapping is exercised in both directions from a single contract.
CONTRACT = """
dataset: provisa/sales/orders
checks:
  - row_count:
      threshold:
        must_be_greater_than: 0
columns:
  - name: customer
    checks:
      - missing:
          threshold:
            level: warn
"""

GX_CONTRACT = json.dumps(
    {
        "meta": {"dataset": "provisa/sales/orders"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer"}}
        ],
    }
)

ORDERS_FQN = "wh.sales.orders"
SCANS_FQN = "dq.quality.orders_scans"
SCAN_TIME = datetime(2026, 8, 12, 3, 0, tzinfo=UTC)


def _observed() -> Table:
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="sales",
        table_name="orders",
        data_product=True,
        columns=[
            Column(name="id", data_type="integer", visible_to=["admin"]),
            Column(name="customer", data_type="varchar", visible_to=["admin"]),
        ],
    )


def _results(contract: str = CONTRACT) -> Table:
    return Table(
        source_id="dq",
        domain_id="sales",
        schema_name="quality",
        table_name="orders_scans",
        data_product=True,
        dq_contract=contract,
        columns=[Column(name="check_name", data_type="varchar", visible_to=["admin"])],
    )


def _config(
    *,
    checker: SourceType = SourceType.soda,
    tables: list[Table] | None = None,
) -> ProvisaConfig:
    return ProvisaConfig(
        sources=[
            Source(id="wh", type=SourceType.postgresql, description="warehouse"),
            Source(id="dq", type=checker, description="quality"),
        ],
        domains=[Domain(id="sales", description="Sales", steward="data-steward")],
        tables=tables if tables is not None else [_observed(), _results()],
        roles=[],
    )


@pytest.fixture
def snapshot():
    """A registered contract no scan has reached yet."""
    return build_snapshot(_config(), org_id="acme", dialect="postgres")


@pytest.fixture
def scanned():
    """The same contract after a scan — the outcomes the export reads back out of the results
    table, keyed the way the loader addressed the scan (schema.table, column, check type)."""
    return build_snapshot(
        _config(),
        org_id="acme",
        dialect="postgres",
        dq_outcomes={
            ("sales.orders", "", "row_count"): DataQualityOutcome(
                status="pass",
                scan_id="scan-9",
                scan_time=SCAN_TIME,
                metric_value=1200.0,
                failed_rows=0,
            ),
            ("sales.orders", "customer", "missing"): DataQualityOutcome(
                status="fail",
                scan_id="scan-9",
                scan_time=SCAN_TIME,
                metric_value=0.07,
                failed_rows=7,
            ),
        },
    )


def _orders_dataset(events):
    return next(
        event
        for event in events
        if event.kind == "dataset" and event.payload["dataset"]["name"] == ORDERS_FQN
    )


def test_assertions_publish_on_the_observed_asset_not_the_results_table(snapshot):
    assert {a.asset.fqn() for a in snapshot.assertions} == {
        ORDERS_FQN,
        f"{ORDERS_FQN}.customer",
    }
    assert {a.results_table.fqn() for a in snapshot.assertions} == {SCANS_FQN}


def test_soda_threshold_level_is_the_published_severity(snapshot):
    by_asset = {a.asset.fqn(): a for a in snapshot.assertions}
    assert by_asset[f"{ORDERS_FQN}.customer"].severity == "warn"
    # No level stated: Soda's own default, not a value this exporter chose.
    assert by_asset[ORDERS_FQN].severity == "fail"


def test_a_gx_suite_publishes_every_check_as_fail():
    """GX has no warn level at all, so reporting one would invent a severity."""
    config = _config(
        checker=SourceType.great_expectations,
        tables=[_observed(), _results(GX_CONTRACT)],
    )
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert [a.severity for a in snapshot.assertions] == ["fail"]
    assert [a.checker for a in snapshot.assertions] == ["great_expectations"]


def test_the_definition_is_the_authored_check_text(snapshot):
    by_asset = {a.asset.fqn(): a for a in snapshot.assertions}
    assert "level: warn" in by_asset[f"{ORDERS_FQN}.customer"].definition
    assert by_asset[f"{ORDERS_FQN}.customer"].check_type == "missing"


def test_the_results_table_carries_the_derived_data_quality_tag(snapshot):
    derived = {(tag.tag_id, tag.asset.fqn()) for tag in snapshot.model_tags if tag.asset}
    assert ("data_quality", SCANS_FQN) in derived
    assert ("data_quality", ORDERS_FQN) not in derived


def test_an_unpublished_observed_table_publishes_no_assertion():
    """Both ends must publish or the reference dangles — the filter's whole purpose."""
    observed = _observed()
    observed.data_product = False
    config = _config(tables=[observed, _results()])
    snapshot = build_snapshot(config, org_id="acme", dialect="postgres")
    assert snapshot.assertions == []


def test_a_contract_that_no_longer_parses_raises_rather_than_being_skipped():
    config = _config(tables=[_observed(), _results("dataset: provisa/sales/orders\nchecks: 3\n")])
    with pytest.raises(ContractError):
        build_snapshot(config, org_id="acme", dialect="postgres")


def test_atlas_rides_the_governance_document(snapshot):
    entities = atlas_entities(snapshot)
    orders = next(e for e in entities if e.asset is not None and e.asset.fqn() == ORDERS_FQN)
    document = json.loads(orders.attributes[GOVERNANCE_ATTRIBUTE])
    checks = document["dataQuality"]
    assert {c["asset"] for c in checks} == {ORDERS_FQN, f"{ORDERS_FQN}.customer"}
    assert {c["resultsTable"] for c in checks} == {SCANS_FQN}
    assert {c["checker"] for c in checks} == {"soda"}


def test_collibra_publishes_the_checks_as_an_attribute_on_the_observed_asset(snapshot):
    rows = to_rows(snapshot, "Provisa", "Provisa Governed Assets")
    by_name = {row["name"]: row for row in rows}
    table_checks = json.loads(by_name[ORDERS_FQN]["attributes"][DATA_QUALITY_ATTRIBUTE][0]["value"])
    assert [c["checkType"] for c in table_checks] == ["row_count"]
    column_checks = json.loads(
        by_name[f"{ORDERS_FQN}.customer"]["attributes"][DATA_QUALITY_ATTRIBUTE][0]["value"]
    )
    assert [(c["checkType"], c["severity"]) for c in column_checks] == [("missing", "warn")]
    assert DATA_QUALITY_ATTRIBUTE not in by_name[SCANS_FQN]["attributes"]


def test_datahub_publishes_native_assertion_entities(snapshot):
    proposals = [p for p in to_proposals(snapshot) if p.entity_type == "assertion"]
    assert len(proposals) == 2
    scopes = {p.aspect["datasetAssertion"]["scope"] for p in proposals}
    assert scopes == {"DATASET_ROWS", "DATASET_COLUMN"}
    for proposal in proposals:
        assertion = proposal.aspect["datasetAssertion"]
        # Every assertion addresses the observed DATASET, whatever its scope.
        assert assertion["dataset"].endswith(f"{ORDERS_FQN},PROD)")
        assert proposal.aspect["customProperties"]["provisaResultsTable"] == SCANS_FQN


def test_datahub_assertion_urns_are_stable_across_publishes(snapshot):
    first = [p.urn for p in to_proposals(snapshot) if p.entity_type == "assertion"]
    second = [
        p.urn
        for p in to_proposals(build_snapshot(_config(), org_id="acme", dialect="postgres"))
        if p.entity_type == "assertion"
    ]
    assert first == second


def test_openlineage_facet_reports_definitions_and_the_last_outcome(scanned):
    events = to_events(scanned, event_time=datetime(2026, 8, 13, tzinfo=UTC))
    orders = _orders_dataset(events)
    entries = orders.payload["dataset"]["facets"]["provisa_data_quality"]["assertions"]
    assert {e["column"] for e in entries} == {None, "customer"}
    by_column = {e["column"]: e["outcome"] for e in entries}
    assert by_column[None] == {
        "status": "pass",
        "scanId": "scan-9",
        "scanTime": "2026-08-12T03:00:00+00:00",
        "metricValue": 1200.0,
        "failedRows": 0,
    }
    assert by_column["customer"]["status"] == "fail"
    assert by_column["customer"]["failedRows"] == 7


def test_openlineage_publishes_the_spec_facet_with_real_success_flags(scanned):
    """The spec's own facet, filled from the scan that ran — not a verdict this exporter chose."""
    orders = _orders_dataset(to_events(scanned, event_time=datetime(2026, 8, 13, tzinfo=UTC)))
    facet = orders.payload["dataset"]["facets"]["dataQualityAssertions"]
    assert {(e["assertion"], e["column"], e["success"]) for e in facet["assertions"]} == {
        ("row_count", None, True),
        ("missing", "customer", False),
    }


def test_a_never_run_contract_publishes_that_state_rather_than_vanishing(snapshot):
    """An unrun check must not read as a clean one, so the state is published, not omitted."""
    orders = _orders_dataset(to_events(snapshot, event_time=datetime(2026, 8, 13, tzinfo=UTC)))
    facets = orders.payload["dataset"]["facets"]
    entries = facets["provisa_data_quality"]["assertions"]
    assert {e["outcome"]["status"] for e in entries} == {"never_run"}
    assert {e["outcome"]["scanTime"] for e in entries} == {None}
    # never_run reached no verdict, so the spec facet — which can only say pass or fail — is absent.
    assert "dataQualityAssertions" not in facets


def test_a_warn_is_published_as_a_failure_not_a_pass():
    """A warn threshold WAS breached; only its severity is milder."""
    snapshot = build_snapshot(
        _config(),
        org_id="acme",
        dialect="postgres",
        dq_outcomes={
            ("sales.orders", "customer", "missing"): DataQualityOutcome(
                status="warn", scan_id="scan-9", scan_time=SCAN_TIME, failed_rows=2
            )
        },
    )
    orders = _orders_dataset(to_events(snapshot, event_time=datetime(2026, 8, 13, tzinfo=UTC)))
    facet = orders.payload["dataset"]["facets"]["dataQualityAssertions"]
    assert [(e["column"], e["success"]) for e in facet["assertions"]] == [("customer", False)]

    result = next(
        e for e in om_entities(snapshot) if e.kind == "test_case_result"
    )
    assert result.body["testCaseStatus"] == "Failed"

    run = next(p for p in to_proposals(snapshot) if p.aspect_name == "assertionRunEvent")
    assert run.aspect["result"]["type"] == "FAILURE"


def test_openmetadata_publishes_the_last_run_as_a_test_case_result(scanned):
    results = [e for e in om_entities(scanned) if e.kind == "test_case_result"]
    assert len(results) == 2
    by_status = {r.body["testCaseStatus"]: r for r in results}
    assert set(by_status) == {"Success", "Failed"}
    passed = by_status["Success"]
    assert passed.body["timestamp"] == int(SCAN_TIME.timestamp() * 1000)
    # The result posts against the case's own FQN, so it lands on the case just published.
    case_names = {e.body["name"] for e in om_entities(scanned) if e.kind == "test_case"}
    for entity in results:
        assert entity.path.endswith("/testCaseResult")
        assert entity.path.split("/")[-2].split(".")[-1] in case_names


def test_openmetadata_publishes_no_result_for_a_check_that_never_ran(snapshot):
    """A testCaseResult can only carry a verdict, and there is none to carry."""
    assert [e for e in om_entities(snapshot) if e.kind == "test_case_result"] == []
    assert [e for e in om_entities(snapshot) if e.kind == "test_case"] != []


def test_datahub_publishes_the_last_run_as_an_assertion_run_event(scanned):
    runs = [p for p in to_proposals(scanned) if p.aspect_name == "assertionRunEvent"]
    assert len(runs) == 2
    assert {r.aspect["result"]["type"] for r in runs} == {"SUCCESS", "FAILURE"}
    for run in runs:
        assert run.aspect["runId"] == "scan-9"
        assert run.aspect["status"] == "COMPLETE"
        assert run.aspect["timestampMillis"] == int(SCAN_TIME.timestamp() * 1000)
        assert run.aspect["asserteeUrn"].endswith(f"{ORDERS_FQN},PROD)")
        # The run event points at the assertion entity published alongside it.
        assert run.aspect["assertionUrn"] == run.urn
    failed = next(r for r in runs if r.aspect["result"]["type"] == "FAILURE")
    assert failed.aspect["result"]["rowCount"] == 7


def test_datahub_carries_the_status_of_a_check_that_never_ran(snapshot):
    assertions = [p for p in to_proposals(snapshot) if p.aspect_name == "assertionInfo"]
    assert {p.aspect["customProperties"]["provisaOutcome"] for p in assertions} == {"never_run"}
    assert [p for p in to_proposals(snapshot) if p.aspect_name == "assertionRunEvent"] == []


def test_atlas_and_collibra_carry_the_outcome_in_their_governance_documents(scanned):
    orders = next(
        e for e in atlas_entities(scanned) if e.asset is not None and e.asset.fqn() == ORDERS_FQN
    )
    checks = json.loads(orders.attributes[GOVERNANCE_ATTRIBUTE])["dataQuality"]
    assert {c["outcome"]["status"] for c in checks} == {"pass", "fail"}

    by_name = {row["name"]: row for row in to_rows(scanned, "Provisa", "Provisa Governed Assets")}
    column_checks = json.loads(
        by_name[f"{ORDERS_FQN}.customer"]["attributes"][DATA_QUALITY_ATTRIBUTE][0]["value"]
    )
    assert column_checks[0]["outcome"]["status"] == "fail"
    assert column_checks[0]["outcome"]["scanId"] == "scan-9"


def test_openmetadata_publishes_test_definitions_suites_and_cases(snapshot):
    entities = om_entities(snapshot)
    kinds = [e.kind for e in entities if e.kind.startswith("test_")]
    assert kinds.count("test_definition") == 2
    assert kinds.count("test_suite") == 1
    assert kinds.count("test_case") == 2
    # Definitions precede the suite, and the suite precedes the cases it holds.
    order = [k for k in kinds]
    assert order == ["test_definition", "test_definition", "test_suite", "test_case", "test_case"]

    suite = next(e for e in entities if e.kind == "test_suite")
    assert suite.body["basicEntityReference"] == "wh.default.sales.orders"

    cases = [e for e in entities if e.kind == "test_case"]
    links = {case.body["entityLink"] for case in cases}
    assert links == {
        "<#E::table::wh.default.sales.orders>",
        "<#E::table::wh.default.sales.orders::columns::customer>",
    }
    assert all(SCANS_FQN in case.body["description"] for case in cases)


def test_openmetadata_test_definitions_name_the_checker_as_the_platform(snapshot):
    definitions = [e for e in om_entities(snapshot) if e.kind == "test_definition"]
    assert {d.body["testPlatforms"][0] for d in definitions} == {"Soda"}
    assert {d.body["entityType"] for d in definitions} == {"TABLE", "COLUMN"}


def test_openmetadata_test_case_names_survive_a_check_being_removed(snapshot):
    """A positional name would re-address every case below a removed check."""
    full = {e.body["name"] for e in om_entities(snapshot) if e.kind == "test_case"}
    trimmed_contract = "\n".join(CONTRACT.strip().splitlines()[:1] + CONTRACT.strip().splitlines()[5:])
    trimmed = build_snapshot(
        _config(tables=[_observed(), _results(trimmed_contract)]),
        org_id="acme",
        dialect="postgres",
    )
    remaining = {e.body["name"] for e in om_entities(trimmed) if e.kind == "test_case"}
    assert remaining and remaining < full
