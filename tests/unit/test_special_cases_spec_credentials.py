# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1742: the live cases of the special-cases spec fail without their credentials, and CI
gives each the credentials it needs.

source-to-query-special-cases.spec.ts has two live cases. The google_sheets one is the Playwright
project "warehouse", run by the warehouse UI job (integration-suite-lanes.yml warehouse-ui); the
govdata one is "core-provisioning", run by the core UI lane's provisioning job (ui-e2e-core.yml).
Each job hands the lane workflow its case's secrets, and the lane passes them to its prepare and
run steps; the prepare script writes the Google key to the file GOOGLE_APPLICATION_CREDENTIALS
names. A case that skipped without them would report a pass it did not earn."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
SPEC = REPO / "provisa-ui" / "e2e" / "source-to-query-special-cases.spec.ts"
PREPARE = REPO / "scripts" / "ci" / "lanes" / "ui-core-prepare.sh"

_GSHEETS_SECRETS = {"GOOGLE_APPLICATION_CREDENTIALS_JSON", "GSHEETS_TEST_SHEET_ID"}
_GOVDATA_SECRETS = {"FREE_ASKAMERICA_KEY"}


def _job(workflow: str, job: str) -> dict:
    return yaml.safe_load((WORKFLOWS / workflow).read_text())["jobs"][job]


def test_the_spec_skips_no_live_case_and_names_what_is_missing():
    text = SPEC.read_text()
    assert not re.search(r"\btest\.(skip|fixme)\s*\(", text)
    assert '"GOOGLE_APPLICATION_CREDENTIALS (secret GOOGLE_APPLICATION_CREDENTIALS_JSON)"' in text
    assert '"GSHEETS_TEST_SHEET_ID"' in text
    assert '["FREE_ASKAMERICA_KEY"]' in text
    assert "throw new Error(`${caseName} cannot run; not set: " in text


def test_the_warehouse_ui_job_runs_the_google_sheets_case_with_its_credentials():
    job = _job("integration-suite-lanes.yml", "warehouse-ui")
    assert "--project=warehouse" in job["with"]["run"]
    assert job["with"]["prepare"] == "scripts/ci/lanes/ui-core-prepare.sh"
    passed = job["secrets"]
    for name in _GSHEETS_SECRETS:
        assert passed.get(name) == f"${{{{ secrets.{name} }}}}", name


def test_the_provisioning_job_runs_the_govdata_case_with_its_key():
    job = _job("ui-e2e-core.yml", "provisioning")
    assert "--project=core-provisioning" in job["with"]["run"]
    passed = job["secrets"]
    for name in _GOVDATA_SECRETS:
        assert passed.get(name) == f"${{{{ secrets.{name} }}}}", name


def test_the_lane_gives_its_prepare_and_run_steps_both_cases_credentials():
    lane = yaml.safe_load((WORKFLOWS / "lane.yml").read_text())
    declared = set(lane[True]["workflow_call"]["secrets"])
    assert _GSHEETS_SECRETS | _GOVDATA_SECRETS <= declared
    steps = {s.get("name"): s for s in lane["jobs"]["lane"]["steps"]}
    for step in ("Prepare", "Run lane"):
        env = steps[step]["env"]
        for name in _GSHEETS_SECRETS | _GOVDATA_SECRETS:
            assert env.get(name) == f"${{{{ secrets.{name} }}}}", f"{step}: {name}"


def test_the_prepare_script_writes_the_google_key_where_the_spec_reads_it():
    text = PREPARE.read_text()
    assert (
        'printf \'%s\' "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > "$dir/gcp-service-account.json"'
        in text
    )
    assert (
        'echo "GOOGLE_APPLICATION_CREDENTIALS=$dir/gcp-service-account.json" >> "$GITHUB_ENV"'
        in text
    )
    assert "process.env.GOOGLE_APPLICATION_CREDENTIALS" in SPEC.read_text()
