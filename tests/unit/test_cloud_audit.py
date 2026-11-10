# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a1c63-2f97-4b5d-8c0e-6d2b9a7f3e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""scripts/ci/cloud_audit.py against recorded provider answers (maintainer 2026-11-10, "Paid
cloud resources"): a resource tagged with a run in progress is kept; one of a finished run, and an
untagged one in a test scope, are deleted and read back; nothing in cloud-dev or outside the
scopes is touched."""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "ci" / "cloud_audit.py"
_spec = importlib.util.spec_from_file_location("cloud_audit", _PATH)
assert _spec is not None and _spec.loader is not None
ca = importlib.util.module_from_spec(_spec)
sys.modules["cloud_audit"] = ca
_spec.loader.exec_module(ca)

NOW = 1_800_000_000.0
SUB = "e37b61f0-2a28-4546-b9f9-160bf25b1ab8"


def _tags(run: str, created: float = NOW - 600) -> dict[str, str]:
    return {"provisa-run": run, "provisa-created": str(int(created))}


class _Runs:
    """GitHub's answers for runs, recorded: 101 in progress, 102 completed, others unknown."""

    def __init__(self) -> None:
        self.asked: list[str] = []

    def get(self, path: str) -> tuple[int, dict]:
        self.asked.append(path)
        run = path.rsplit("/", 1)[1]
        if run == "101":
            return 200, {"id": 101, "status": "in_progress"}
        if run == "102":
            return 200, {"id": 102, "status": "completed", "conclusion": "success"}
        if run == "500":
            return 502, {"message": "Bad gateway"}
        return 404, {"message": "Not Found"}


def _github(runs: _Runs | None = None):
    runs = runs or _Runs()
    return ca.GitHubRuns("kenstott/provisa", runs.get), runs


@dataclass
class _FakeProvider:
    """A provider holding ``resources``; deletes remove them unless the id is in ``sticky``."""

    resources: list
    sticky: set = field(default_factory=set)
    refuse: set = field(default_factory=set)
    name: str = "fake"
    deleted: list = field(default_factory=list)

    def scope(self) -> str:
        return "fake-scope"

    def list(self):
        return list(self.resources)

    def delete(self, resource) -> None:
        if resource.id in self.refuse:
            raise RuntimeError("AuthorizationFailed")
        self.deleted.append(resource.id)
        if resource.id not in self.sticky:
            self.resources = [r for r in self.resources if r.id != resource.id]

    def exists(self, resource) -> bool:
        return any(r.id == resource.id for r in self.resources)

    def absent(self) -> list[str]:
        return []


def _res(id_: str, tags: dict[str, str] | None = None):
    return ca.Resource("fake", "fake-scope", "kind", id_, id_, tags or {})


# ------------------------------------------------------------------------------- classification


def test_a_resource_of_a_run_in_progress_is_kept():
    runs, _ = _github()
    provider = _FakeProvider([_res("active", _tags("gh-101"))])
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert [(r.resource.id, r.action) for r in report.rows] == [("active", "kept")]
    assert provider.deleted == [] and not report.failed


def test_a_resource_of_a_finished_run_is_deleted_and_read_back_gone():
    runs, _ = _github()
    provider = _FakeProvider([_res("finished", _tags("gh-102"))])
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert provider.deleted == ["finished"]
    assert [(r.action, r.reason) for r in report.rows] == [
        ("deleted", "run gh-102 not in progress")
    ]
    assert not report.failed


def test_an_untagged_resource_in_a_test_scope_is_deleted():
    runs, _ = _github()
    provider = _FakeProvider([_res("provisatest2")])
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert provider.deleted == ["provisatest2"]
    assert report.rows[0].reason == "untagged"


def test_a_run_github_does_not_know_is_not_in_progress():
    runs, _ = _github()
    provider = _FakeProvider([_res("gone-run", _tags("gh-999")), _res("odd", _tags("someone"))])
    ca.audit([provider], runs, NOW, dry_run=False)
    assert provider.deleted == ["gone-run", "odd"]


def test_a_local_run_is_in_progress_only_within_its_lease():
    runs, asked = _github()
    young = _res("young", _tags("local-abc", NOW - ca.LOCAL_LEASE_S + 60))
    old = _res("old", _tags("local-abc", NOW - ca.LOCAL_LEASE_S - 60))
    undated = _res("undated", {"provisa-run": "local-abc"})
    provider = _FakeProvider([young, old, undated])
    ca.audit([provider], runs, NOW, dry_run=False)
    assert provider.deleted == ["old", "undated"]
    assert asked.asked == []  # a local run is never asked of GitHub


def test_each_github_run_is_asked_once():
    runs, asked = _github()
    provider = _FakeProvider([_res("a", _tags("gh-101")), _res("b", _tags("gh-101"))])
    ca.audit([provider], runs, NOW, dry_run=False)
    assert asked.asked == ["/repos/kenstott/provisa/actions/runs/101"]


def test_a_run_whose_status_cannot_be_read_is_kept_and_fails_the_audit():
    runs, _ = _github()
    provider = _FakeProvider([_res("unknown", _tags("gh-500"))])
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert provider.deleted == []
    assert report.rows[0].action == "error" and report.failed


def test_a_delete_that_did_not_take_fails_the_audit():
    runs, _ = _github()
    provider = _FakeProvider([_res("sticky")], sticky={"sticky"})
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert report.rows[0].action == "left" and report.failed


def test_a_refused_delete_is_reported_and_the_rest_still_run():
    runs, _ = _github()
    provider = _FakeProvider([_res("refused"), _res("other")], refuse={"refused"})
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert [(r.resource.id, r.action) for r in report.rows] == [
        ("refused", "delete failed"),
        ("other", "deleted"),
    ]
    assert report.failed


def test_a_dry_run_deletes_nothing():
    runs, _ = _github()
    provider = _FakeProvider([_res("x"), _res("y", _tags("gh-101"))])
    report = ca.audit([provider], runs, NOW, dry_run=True)
    assert provider.deleted == []
    assert [r.action for r in report.rows] == ["would delete", "kept"]


def test_the_summary_names_what_was_found_and_deleted():
    runs, _ = _github()
    provider = _FakeProvider([_res("kept-one", _tags("gh-101")), _res("gone-one")])
    text = ca.summary(ca.audit([provider], runs, NOW, dry_run=False), dry_run=False)
    assert "2 resource(s) found; 1 deleted; 1 kept" in text
    assert "| kept-one |" in text and "| gone-one |" in text and "untagged" in text


# ---------------------------------------------------------------------------------------- scopes


def test_the_scopes_never_name_cloud_dev():
    ca.check_scopes(ca.SCOPES, {"GOOGLE_CLOUD_PROJECT": "some-test-project"})


@pytest.mark.parametrize(
    "name", ["provisa-test-473", "PROVISA-TEST-473", "rg-cloud.provisa.dev", "provisa.dev"]
)
def test_a_scope_naming_cloud_dev_is_refused_by_name(name):
    with pytest.raises(ca.ScopeRefused, match="cloud-dev"):
        ca.refuse_cloud_dev([name])


def test_a_gcp_project_that_is_cloud_dev_is_refused():
    with pytest.raises(ca.ScopeRefused, match="provisa-test-473"):
        ca.check_scopes(ca.SCOPES, {"GOOGLE_CLOUD_PROJECT": "provisa-test-473"})
    with pytest.raises(ca.ScopeRefused):
        ca.BigQueryProvider(ca.SCOPES.gcp, object(), "provisa-test-473")


def test_an_azure_scope_naming_cloud_dev_is_refused():
    scopes = ca.Scopes(
        azure=ca.AzureScope(SUB, ("my-rg", "provisa-test-473-rg")),
        aws=ca.SCOPES.aws,
        gcp=ca.SCOPES.gcp,
    )
    with pytest.raises(ca.ScopeRefused):
        ca.check_scopes(scopes, {})


# ----------------------------------------------------------------------------------------- Azure


class _Az:
    """Recorded `az` answers for the test subscription (the resources standing on 2026-11-10)."""

    def __init__(
        self, groups: dict[str, list[dict] | None], denied: frozenset[str] = frozenset()
    ) -> None:
        self.groups = groups
        self.denied = denied
        self.calls: list[tuple[str, ...]] = []
        self.gone: set[str] = set()

    def __call__(self, *args: str, timeout: int = 0) -> str:
        self.calls.append(args)
        if args[:2] == ("resource", "list"):
            group = args[args.index("-g") + 1]
            if group in self.denied:
                raise RuntimeError(
                    "(AuthorizationFailed) The client does not have authorization to perform "
                    "action 'Microsoft.Resources/subscriptions/resourcegroups/read' over scope "
                    f"'/subscriptions/{SUB}/resourcegroups/{group}' or the scope is invalid."
                )
            items = self.groups.get(group)
            if items is None:
                raise RuntimeError(f"(ResourceGroupNotFound) Resource group '{group}' not found")
            return json.dumps([i for i in items if i["id"] not in self.gone])
        if args[:2] == ("resource", "delete"):
            self.gone.add(args[args.index("--ids") + 1])
            return ""
        if args[:2] == ("resource", "show"):
            if args[args.index("--ids") + 1] in self.gone:
                raise RuntimeError("(ResourceNotFound) The Resource was not found")
            return ""
        raise AssertionError(f"unexpected az call {args}")


def _arm(group: str, type_: str, name: str, tags: dict | None = None) -> dict:
    return {
        "id": f"/subscriptions/{SUB}/resourceGroups/{group}/providers/{type_}/{name}",
        "type": type_,
        "name": name,
        "tags": tags,
    }


def test_azure_deletes_the_untagged_standing_resources_and_keeps_an_active_one():
    az = _Az(
        {
            "my-rg": [
                _arm("my-rg", "Microsoft.Fabric/capacities", "provisatest2", {}),
                _arm("my-rg", "Microsoft.Synapse/workspaces", "provisa-syn-e2e-261016102645"),
                _arm("my-rg", "Microsoft.Storage/storageAccounts", "provisasyn261016102645", {}),
                _arm("my-rg", "Microsoft.Purview/accounts", "provisa-pv-101", _tags("gh-101")),
                # A child resource goes with its parent.
                _arm("my-rg", "Microsoft.Synapse/workspaces/sqlPools", "x/pool"),
            ],
        }
    )
    runs, _ = _github()
    provider = ca.AzureProvider(ca.SCOPES.azure, az)
    report = ca.audit([provider], runs, NOW, dry_run=False)
    actions = {r.resource.name: r.action for r in report.rows}
    assert actions == {
        "provisatest2": "deleted",
        "provisa-syn-e2e-261016102645": "deleted",
        "provisasyn261016102645": "deleted",
        "provisa-pv-101": "kept",
    }
    assert not report.failed
    listed = {c[c.index("-g") + 1] for c in az.calls if c[:2] == ("resource", "list")}
    assert listed == {"my-rg"}  # only the scope's groups are listed
    assert all(SUB in c[c.index("--subscription") + 1] for c in az.calls if "--subscription" in c)


def test_azure_lists_each_scope_group_by_itself_never_the_subscription():
    """The CI principal holds rights on the test groups, not read on the subscription."""
    scope = ca.AzureScope(SUB, ("my-rg", "other-rg"))
    az = _Az(
        {
            "my-rg": [_arm("my-rg", "Microsoft.Purview/accounts", "provisa-pv-1", {})],
            "other-rg": [_arm("other-rg", "Microsoft.Storage/storageAccounts", "st1", {})],
        }
    )
    found = ca.AzureProvider(scope, az).list()
    assert {(r.scope, r.name) for r in found} == {
        (f"{SUB}/my-rg", "provisa-pv-1"),
        (f"{SUB}/other-rg", "st1"),
    }
    lists = [c for c in az.calls if c[:2] == ("resource", "list")]
    assert [c[c.index("-g") + 1] for c in lists] == ["my-rg", "other-rg"]
    assert all(c[c.index("--subscription") + 1] == SUB for c in lists)


def test_a_scope_group_that_does_not_exist_is_reported_absent_and_holds_nothing():
    scope = ca.AzureScope(SUB, ("my-rg", "gone-rg"))
    az = _Az({"my-rg": [_arm("my-rg", "Microsoft.Storage/storageAccounts", "st1", {})]})
    runs, _ = _github()
    report = ca.audit([ca.AzureProvider(scope, az)], runs, NOW, dry_run=True)
    assert not report.failed
    assert [r.resource.name for r in report.rows] == ["st1"]
    assert report.absent == [f"azure: resource group gone-rg ({SUB}) does not exist"]
    text = ca.summary(report, dry_run=True)
    assert "### Absent" in text and "gone-rg" in text and "### Errors" not in text


def test_a_scope_group_the_principal_cannot_read_fails_the_audit_by_name():
    scope = ca.AzureScope(SUB, ("my-rg", "locked-rg"))
    az = _Az(
        {"my-rg": [_arm("my-rg", "Microsoft.Storage/storageAccounts", "st1", {})], "locked-rg": []},
        denied=frozenset({"locked-rg"}),
    )
    runs, _ = _github()
    report = ca.audit([ca.AzureProvider(scope, az)], runs, NOW, dry_run=True)
    assert report.failed
    assert report.absent == []
    assert len(report.errors) == 1
    assert "resource group locked-rg:" in report.errors[0]
    assert "AuthorizationFailed" in report.errors[0]


def test_the_azure_scope_is_the_groups_the_live_tests_create_in():
    """FABRIC_RESOURCE_GROUP (my-rg) holds the capacity and each run's Purview account; the
    principal has no rights on any other group, so naming one fails every audit."""
    assert ca.SCOPES.azure.resource_groups == ("my-rg",)


def test_azure_never_deletes_outside_its_resource_groups():
    az = _Az({"my-rg": []})
    provider = ca.AzureProvider(ca.SCOPES.azure, az)
    outside = ca.Resource(
        "azure",
        "x",
        "Microsoft.Compute/virtualMachines",
        f"/subscriptions/{SUB}/resourceGroups/cloud-dev-rg/providers/Microsoft.Compute/virtualMachines/vm",
        "vm",
    )
    other_sub = ca.Resource(
        "azure",
        "x",
        "Microsoft.Storage/storageAccounts",
        "/subscriptions/031bdc1b-0ba1-4e45-9f07-a8174eba33c7/resourceGroups/my-rg/providers/x/y",
        "y",
    )
    for resource in (outside, other_sub):
        with pytest.raises(ca.ScopeRefused):
            provider.delete(resource)
    assert not any(c[:2] == ("resource", "delete") for c in az.calls)


# ------------------------------------------------------------------------------------------- AWS


class _NotFound(Exception):
    pass


_NotFound.__name__ = "ResourceNotFoundException"


class _Rss:
    """Recorded Redshift Serverless answers: one active run's lane, one leaked, one not ours."""

    def __init__(self) -> None:
        self.workgroups = {
            "provisa-e2e-wg-261101": _tags("gh-102"),
            "provisa-e2e-wg-261110": _tags("gh-101"),
            "analytics-prod": {},
        }
        self.namespaces = {"provisa-e2e-261101": {}, "provisa-e2e-261110": _tags("gh-101")}
        self.deleted: list[str] = []

    def list_workgroups(self):
        return {
            "workgroups": [
                {"workgroupName": n, "workgroupArn": f"arn:wg/{n}"} for n in self.workgroups
            ]
        }

    def list_namespaces(self):
        return {
            "namespaces": [
                {"namespaceName": n, "namespaceArn": f"arn:ns/{n}"} for n in self.namespaces
            ]
        }

    def list_tags_for_resource(self, resourceArn):
        kind, name = resourceArn.split(":", 1)[1].split("/")
        tags = (self.workgroups if kind == "wg" else self.namespaces)[name]
        return {"tags": [{"key": k, "value": v} for k, v in tags.items()]}

    def delete_workgroup(self, workgroupName):
        self.deleted.append(workgroupName)
        del self.workgroups[workgroupName]

    def delete_namespace(self, namespaceName):
        self.deleted.append(namespaceName)
        del self.namespaces[namespaceName]

    def get_workgroup(self, workgroupName):
        if workgroupName not in self.workgroups:
            raise _NotFound(workgroupName)
        return {}

    def get_namespace(self, namespaceName):
        if namespaceName not in self.namespaces:
            raise _NotFound(namespaceName)
        return {}


def test_redshift_deletes_a_finished_runs_lane_workgroup_first_and_never_another_name():
    rss = _Rss()
    runs, _ = _github()
    provider = ca.RedshiftServerlessProvider(ca.SCOPES.aws, rss, "us-east-1", sleep=lambda s: None)
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert rss.deleted == ["provisa-e2e-wg-261101", "provisa-e2e-261101"]
    assert "analytics-prod" in rss.workgroups
    assert not report.failed


# ------------------------------------------------------------------------------------------- GCP


@dataclass
class _Dataset:
    dataset_id: str
    labels: dict


class _Bq:
    def __init__(self) -> None:
        self.datasets = {
            "provisa_e2e_it_0a1b2c3d": {},
            "provisa_parity_it_deadbeef": _tags("gh-101"),
            "org_swaptest_1234abcd_replicas": {},
            "analytics": {},  # a standing dataset: not a test's name
            "provisa_e2e_it": {},  # no run suffix: not a test's name
        }
        self.deleted: list[str] = []

    def list_datasets(self, project):
        return [_Dataset(n, {}) for n in self.datasets]

    def get_dataset(self, ref):
        from google.api_core.exceptions import NotFound

        name = ref.split(".", 1)[1]
        if name not in self.datasets:
            raise NotFound(ref)
        return _Dataset(name, self.datasets[name])

    def delete_dataset(self, ref, delete_contents, not_found_ok):
        assert delete_contents and not_found_ok
        name = ref.split(".", 1)[1]
        self.deleted.append(name)
        self.datasets.pop(name, None)


def test_bigquery_deletes_only_the_tests_own_datasets_not_in_progress():
    bq = _Bq()
    runs, _ = _github()
    provider = ca.BigQueryProvider(ca.SCOPES.gcp, bq, "provisa-e2e-project")
    report = ca.audit([provider], runs, NOW, dry_run=False)
    assert sorted(bq.deleted) == ["org_swaptest_1234abcd_replicas", "provisa_e2e_it_0a1b2c3d"]
    assert {"analytics", "provisa_e2e_it", "provisa_parity_it_deadbeef"} <= set(bq.datasets)
    assert not report.failed


def test_a_provider_that_cannot_list_fails_the_audit_and_the_others_still_run():
    class _Broken(_FakeProvider):
        def list(self):
            raise RuntimeError("AuthorizationFailed")

    runs, _ = _github()
    healthy = _FakeProvider([_res("x")])
    report = ca.audit([_Broken([]), healthy], runs, NOW, dry_run=False)
    assert healthy.deleted == ["x"]
    assert report.failed and "listing failed" in report.errors[0]


# ------------------------------------------------------------------------------- the workflow


def test_the_audit_runs_every_four_hours_and_on_demand_and_never_beside_a_live_lane():
    import yaml

    workflow = yaml.safe_load((_PATH.parents[2] / ".github/workflows/cloud-audit.yml").read_text())
    triggers = workflow[True]
    assert triggers["schedule"] == [{"cron": "0 */4 * * *"}]
    assert "workflow_dispatch" in triggers
    job = workflow["jobs"]["audit"]
    # The live lanes' group: a resource a lane is making is never taken from under it.
    assert job["concurrency"] == {"group": "live-cloud-warehouses", "queue": "max"}
    assert workflow["permissions"] == {"contents": "read", "actions": "read"}
    audit_step = next(s for s in job["steps"] if s.get("name") == "Audit")
    assert "scripts/ci/cloud_audit.py" in audit_step["run"]
    # Credentials by the names the live tests already use: nothing new.
    lanes = (_PATH.parents[2] / ".github/workflows/integration-suite-lanes.yml").read_text()
    for name in job["env"]:
        if name.startswith(("AZURE_", "REDSHIFT_", "GOOGLE_")):
            assert f"secrets.{name}" in lanes, name


def test_every_tag_value_fits_every_providers_label_rules():
    """GCP labels are the strictest: lowercase letters, digits, - and _, at most 63."""
    import re

    from tests.integration import cloud_run_tag

    for key, value in cloud_run_tag.run_tags(NOW).items():
        for text in (key, value):
            assert re.fullmatch(r"[a-z0-9_-]{1,63}", text), text
    assert cloud_run_tag.run_tags(NOW)[ca.CREATED_KEY] == str(int(NOW))


# ------------------------------------------------------------------ standing pieces: their costs
#
# Maintainer 2026-11-10: a piece that bills nothing while idle may stand between runs; each audit
# reports what each one billed since the last audit ended, and fails, by name, if one billed while
# no run of the lanes that use it was in progress.

from datetime import datetime, timezone  # noqa: E402

START = NOW - 4 * 3600  # the last audit ended four hours ago


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, timezone.utc)


class _Actions:
    """The Actions API, recorded: workflow runs per workflow file, and each run's jobs."""

    def __init__(self, runs: dict[str, list[dict]], jobs: dict[int, list[dict]], audits=None):
        self.runs = runs
        self.jobs = jobs
        self.audits = audits if audits is not None else [{"updated_at": _iso(START)}]
        self.asked: list[str] = []

    def get(self, path: str) -> tuple[int, dict]:
        self.asked.append(path)
        if "/workflows/cloud-audit.yml/runs?status=completed" in path:
            return 200, {"workflow_runs": self.audits}
        if "/actions/workflows/" in path:
            workflow = path.split("/actions/workflows/")[1].split("/")[0]
            page = int(path.rsplit("page=", 1)[1])
            runs = self.runs.get(workflow, [])
            return 200, {"workflow_runs": runs[(page - 1) * 100 : page * 100]}
        run_id = int(path.split("/actions/runs/")[1].split("/")[0])
        page = int(path.rsplit("page=", 1)[1])
        return 200, {"jobs": self.jobs.get(run_id, [])[(page - 1) * 100 : page * 100]}


def _job(name: str, began: float, ended: float | None) -> dict:
    return {
        "name": name,
        "started_at": _iso(began),
        "completed_at": _iso(ended) if ended is not None else None,
    }


_LANE = "lanes / warehouse (live cloud warehouses)"


def test_the_window_starts_when_the_last_completed_audit_ended():
    actions = _Actions({}, {})
    assert ca.RunJobs("o/r", actions.get).last_audit_end() == START


def test_with_no_completed_audit_the_window_has_no_start_and_says_so():
    actions = _Actions({}, {}, audits=[])
    with pytest.raises(RuntimeError, match="no completed cloud audit run"):
        ca.RunJobs("o/r", actions.get).last_audit_end()


def test_a_jobs_runs_in_the_window_are_found_by_its_name_paged_and_a_running_one_ends_now():
    runs = {"integration-suite.yml": [{"id": i} for i in range(1, 102)]}
    jobs = {
        1: [_job("lanes / suite 1/6", START + 60, START + 120)]
        + [_job(f"lanes / other {n}", START, START + 1) for n in range(99)]
        + [_job(_LANE, START + 600, START + 3600)],
        101: [_job(_LANE, NOW - 300, None)],
        2: [_job(_LANE, START - 3 * 3600, START - 2 * 3600)],  # before the window
        3: [{"name": _LANE, "started_at": None, "completed_at": None}],  # still queued
    }
    actions = _Actions(runs, jobs)
    got = ca.RunJobs("o/r", actions.get).intervals({ca.WAREHOUSE_LANE}, START, NOW)
    assert got[ca.WAREHOUSE_LANE] == [(START + 600, START + 3600), (NOW - 300, NOW)]
    # Runs created before the window are asked too (a run may wait in the queue), and the
    # second page of runs and of the first run's jobs is read.
    assert any("created=%3E%3D" in p and "page=2" in p for p in actions.asked)
    assert any("/runs/1/jobs" in p and "page=2" in p for p in actions.asked)


@dataclass
class _Piece:
    read_as: object
    name: str = "piece"
    users: tuple = (ca.WAREHOUSE_LANE,)

    def read(self, start, end):
        if isinstance(self.read_as, Exception):
            raise self.read_as
        return self.read_as


def _intervals(*spans):
    return {ca.WAREHOUSE_LANE: list(spans)}


def test_a_use_inside_a_run_of_its_lanes_is_reported_and_passes():
    read = ca.CostRead(cost="0.0833 credits", uses=[ca.Use(START + 700, START + 800, "q")])
    line = ca.judge(_Piece(read), read, _intervals((START + 600, START + 900)))
    assert not line.failed and line.cost == "0.0833 credits"


def test_a_use_just_after_its_run_is_that_runs_but_one_long_after_fails_by_name():
    near = ca.Use(START + 900 + 300, START + 900 + 300, "warehouse resumed")
    far = ca.Use(START + 9000, START + 9000, "warehouse resumed")
    read = ca.CostRead(cost="c", uses=[near, far])
    line = ca.judge(
        _Piece(read, name="Snowflake warehouse"), read, _intervals((START, START + 900))
    )
    assert line.failed
    assert "billed with no run of its lanes" in line.verdict
    assert "warehouse (live cloud warehouses)" in line.verdict
    assert _iso(START + 9000) in line.verdict and _iso(START + 1200) not in line.verdict


def test_a_use_of_another_pieces_lane_does_not_cover_this_piece():
    read = ca.CostRead(cost="c", uses=[ca.Use(START + 10, START + 10, "q")])
    piece = _Piece(read, users=(ca.ATHENA_LANE,))
    assert ca.judge(piece, read, _intervals((START, START + 100))).failed


def test_an_unreadable_cost_is_reported_and_fails_only_on_a_use_outside_a_run():
    quiet = ca.CostRead(cost_unreadable="no billing API", uses=[])
    line = ca.judge(_Piece(quiet), quiet, _intervals())
    assert not line.failed and line.cost == "cost unreadable: no billing API"
    busy = ca.CostRead(cost_unreadable="no billing API", uses=[ca.Use(START, START, "q")])
    assert ca.judge(_Piece(busy), busy, _intervals()).failed


def test_a_piece_whose_use_cannot_be_read_fails_since_the_audit_cannot_tell():
    read = ca.CostRead(cost_unreadable="no key", usage_unreadable="no key either")
    line = ca.judge(_Piece(read), read, _intervals((START, NOW)))
    assert line.failed and "cannot tell whether it billed" in line.verdict


def test_a_piece_whose_read_raises_fails_and_the_others_are_still_read():
    actions = _Actions({}, {})
    ok = _Piece(ca.CostRead(cost="0 credits", uses=[]), name="ok")
    start, lines = ca.standing_costs(
        [_Piece(RuntimeError("boom"), name="broken"), ok], ca.RunJobs("o/r", actions.get), NOW
    )
    assert start == START
    assert [(line.piece, line.failed) for line in lines] == [("broken", True), ("ok", False)]
    assert "boom" in lines[0].verdict


def test_the_summary_carries_a_cost_line_per_piece_and_a_failed_one_fails_the_audit():
    report = ca.Report(
        costs=[
            ca.CostLine("Snowflake warehouse", "0.0000 credits", "no use in the window", False),
            ca.CostLine("MotherDuck", "cost unreadable: x", "billed with no run", True),
        ],
        cost_start=START,
    )
    text = ca.summary(report, dry_run=False)
    assert f"since the last audit ended ({_iso(START)})" in text
    assert "| Snowflake warehouse | 0.0000 credits | no use in the window |" in text
    assert "| MotherDuck | cost unreadable: x | FAIL: billed with no run |" in text
    assert report.failed
    assert not ca.Report(costs=[report.costs[0]], cost_start=START).failed


# Snowflake


class _Snowflake:
    def __init__(self, state: str, resumed: float | None, meter=None):
        self.state, self.resumed, self.meter = state, resumed, meter
        self.statements: list[tuple[str, bool]] = []

    def sql(self, statement: str, params: list, warehouse: bool) -> list[dict]:
        self.statements.append((statement, warehouse))
        if statement.startswith("SHOW WAREHOUSES"):
            return [
                {"name": "OTHER_WH", "state": "STARTED", "resumed_on": _dt(NOW)},
                {
                    "name": "CI_WH",
                    "state": self.state,
                    "resumed_on": _dt(self.resumed) if self.resumed else None,
                },
            ]
        if isinstance(self.meter, Exception):
            raise self.meter
        return self.meter


def test_snowflake_suspended_since_before_the_window_costs_nothing_and_resumes_nothing():
    sf = _Snowflake("SUSPENDED", START - 3600)
    read = ca.SnowflakeWarehouse("ci_wh", sf.sql).read(START, NOW)
    assert read.uses == [] and read.cost is not None and read.cost.startswith("0 credits")
    # Only SHOW WAREHOUSES, through a session with no warehouse.
    assert sf.statements == [("SHOW WAREHOUSES LIKE 'ci_wh'", False)]


def test_snowflake_resumed_in_the_window_reads_its_hourly_metering_through_the_warehouse():
    meter = [
        {"start_time": _dt(START), "end_time": _dt(START + 3600), "credits_used": 0.05},
        {"start_time": _dt(START + 3600), "end_time": _dt(START + 7200), "credits_used": 0},
    ]
    sf = _Snowflake("SUSPENDED", START + 120, meter)
    read = ca.SnowflakeWarehouse("CI_WH", sf.sql).read(START, NOW)
    assert read.cost == "0.0500 credits (hourly metering)"
    assert sf.statements[1][1] is True and "WAREHOUSE_METERING_HISTORY" in sf.statements[1][0]
    assert [u.what for u in read.uses or []] == ["warehouse resumed", "0.0500 credits"]


def test_snowflake_metering_the_user_may_not_read_is_cost_unreadable_with_its_uses_kept():
    sf = _Snowflake("STARTED", START + 120, RuntimeError("Insufficient privileges"))
    read = ca.SnowflakeWarehouse("CI_WH", sf.sql).read(START, NOW)
    assert read.cost is None and "Insufficient privileges" in read.cost_unreadable
    assert [u.what for u in read.uses or []] == ["warehouse STARTED now", "warehouse resumed"]


# Databricks


def test_databricks_reads_its_state_and_paged_query_history_and_never_starts_it():
    asked: list[tuple[str, dict]] = []
    pages = [
        {
            "res": [{"query_id": "a", "query_start_time_ms": (START + 60) * 1000,
                     "query_end_time_ms": (START + 90) * 1000}],
            "has_next_page": True,
            "next_page_token": "t2",
        },
        {"res": [{"query_id": "b", "query_start_time_ms": (START + 120) * 1000}]},
    ]  # fmt: skip

    def get(path: str, params: dict) -> dict:
        asked.append((path, params))
        if path.endswith("/warehouses/abc123"):
            return {"state": "STOPPED"}
        return pages.pop(0)

    read = ca.DatabricksWarehouse("abc123", get).read(START, NOW)
    assert [(u.start, u.end, u.what) for u in read.uses or []] == [
        (START + 60, START + 90, "query a"),
        (START + 120, START + 120, "query b"),
    ]
    assert read.cost is None and "system.billing.usage" in read.cost_unreadable
    assert asked[1][1]["filter_by.warehouse_ids"] == "abc123"
    assert asked[2][1]["page_token"] == "t2"
    assert all(not p.endswith("/start") for p, _ in asked)


def test_databricks_running_now_is_a_use_now():
    def get(path: str, params: dict) -> dict:
        return {"state": "RUNNING"} if "/warehouses/" in path else {"res": []}

    read = ca.DatabricksWarehouse("abc123", get).read(START, NOW)
    assert [(u.start, u.what) for u in read.uses or []] == [(NOW, "warehouse RUNNING now")]


# The buckets


@dataclass
class _Blob:
    name: str
    size: int
    updated: datetime


def test_the_gcs_bucket_reports_what_it_stores_and_each_write_in_the_window():
    blobs = [_Blob("old.parquet", 1000, _dt(START - 60)), _Blob("new.parquet", 500, _dt(START + 5))]
    read = ca.GcsBucket(lambda: blobs).read(START, NOW)
    assert read.cost is not None and read.cost.startswith("2 object(s), 1500 bytes stored")
    assert [u.what for u in read.uses or []] == ["object new.parquet written"]


class _S3:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self.objects

        class _P:
            def paginate(self, Bucket):
                return [{"Contents": objects}]

        return _P()


class _Athena:
    def __init__(self, pages, executions):
        self.pages, self.executions, self.listed = pages, executions, 0

    def list_query_executions(self, **kw):
        page = self.pages[self.listed]
        self.listed += 1
        return page

    def batch_get_query_execution(self, QueryExecutionIds):
        return {"QueryExecutions": [self.executions[i] for i in QueryExecutionIds]}


def _qe(qid: str, at: float, scanned: int) -> dict:
    return {
        "QueryExecutionId": qid,
        "Status": {"SubmissionDateTime": _dt(at), "CompletionDateTime": _dt(at + 5)},
        "Statistics": {"DataScannedInBytes": scanned},
    }


def test_the_athena_bucket_reports_storage_writes_and_the_windows_queries_by_bytes_scanned():
    s3 = _S3([{"Key": "itest/x.csv", "Size": 100, "LastModified": _dt(START + 30)}])
    executions = {
        "q1": _qe("q1", START + 40, 2 * 10**12),
        "q2": _qe("q2", START + 50, 0),
        "q0": _qe("q0", START - 50, 10**12),
    }
    athena = _Athena(
        [{"QueryExecutionIds": ["q2", "q1"], "NextToken": "n"}, {"QueryExecutionIds": ["q0"]}],
        executions,
    )
    read = ca.AthenaBucket("b", s3, athena).read(START, NOW)
    assert read.cost is not None
    assert "2 Athena quer(ies) scanned 2000000000000 bytes" in read.cost
    # $5/TB for 2 TB, plus the 10 MB minimum of the query that scanned nothing.
    assert "($10.0001 at $5/TB" in read.cost
    assert [u.what for u in read.uses or []] == [
        "object itest/x.csv written", "Athena query q2", "Athena query q1",
    ]  # fmt: skip
    # The walk stopped at the page that reached before the window.
    assert athena.listed == 2


# DataZone


class _Denied(Exception):
    pass


class _Ce:
    def __init__(self, days):
        self.days = days

    def get_cost_and_usage(self, **kw):
        assert kw["Filter"]["Dimensions"]["Values"] == ["Amazon DataZone"]
        if isinstance(self.days, Exception):
            raise self.days
        return {"ResultsByTime": self.days}


class _Trail:
    def __init__(self, events):
        self.events = events

    def get_paginator(self, name):
        events = self.events

        class _P:
            def paginate(self, **kw):
                if isinstance(events, Exception):
                    raise events
                return [{"Events": events}]

        return _P()


def _day(day: str, end: str, amount: str) -> dict:
    return {
        "TimePeriod": {"Start": day, "End": end},
        "Total": {"UnblendedCost": {"Amount": amount}},
    }


def test_datazone_cost_by_day_is_a_use_and_cloudtrail_denied_still_leaves_it_judged():
    d0 = _dt(START).date()
    days = [_day(str(d0), "2099-01-01", "0.0"), _day("2099-01-01", "2099-01-02", "0.25")]
    read = ca.DataZoneDomain(_Ce(days), _Trail(_Denied("AccessDenied"))).read(START, NOW)
    assert read.cost is not None and read.cost.startswith("$0.2500 (Amazon DataZone")
    assert [u.what for u in read.uses or []] == ["$0.2500 billed on 2099-01-01"]
    assert "AccessDenied" in read.usage_unreadable


def test_datazone_cost_denied_is_unreadable_and_its_api_calls_are_its_uses():
    events = [{"EventTime": _dt(START + 10), "EventName": "GetDomain"}]
    read = ca.DataZoneDomain(_Ce(_Denied("AccessDenied ce")), _Trail(events)).read(START, NOW)
    assert read.cost is None and "AccessDenied ce" in read.cost_unreadable
    assert [u.what for u in read.uses or []] == ["DataZone GetDomain"]


def test_datazone_with_neither_read_cannot_be_told_and_fails():
    read = ca.DataZoneDomain(_Ce(_Denied("ce")), _Trail(_Denied("trail"))).read(START, NOW)
    assert read.uses is None
    assert ca.judge(ca.DataZoneDomain(None, None), read, _intervals((START, NOW))).failed


# SingleStore, MotherDuck, R2


def test_singlestore_has_no_cost_or_use_the_ci_can_read_and_fails_by_name_saying_why():
    piece = ca.SingleStoreWorkspace()
    line = ca.judge(piece, piece.read(START, NOW), _intervals((START, NOW)))
    assert line.failed and line.piece == "SingleStore workspace"
    assert "Management API" in line.cost and "cannot tell" in line.verdict


def test_motherduck_queries_are_read_from_its_query_history():
    rows = [{"query_id": "m1", "start_time": _dt(START + 5), "end_time": None}]
    read = ca.MotherDuckAccount(lambda s, p: rows).read(START, NOW)
    assert read.cost is None and "no billing API" in read.cost_unreadable
    assert [(u.start, u.what) for u in read.uses or []] == [(START + 5, "query m1")]


def test_motherduck_without_query_history_cannot_be_told():
    def refuse(statement, params):
        raise RuntimeError("Catalog Error: md_information_schema does not exist")

    read = ca.MotherDuckAccount(refuse).read(START, NOW)
    assert read.uses is None and "md_information_schema" in read.usage_unreadable


def test_r2_reports_its_storage_and_hourly_requests_from_the_analytics_api():
    asked: list[dict] = []

    def graphql(query: str, variables: dict) -> dict:
        asked.append(variables)
        hour = _iso(START + 3600)
        return {
            "data": {
                "viewer": {
                    "accounts": [
                        {
                            "ops": [
                                {"sum": {"requests": 3},
                                 "dimensions": {"datetimeHour": hour, "actionType": "PutObject"}},
                                {"sum": {"requests": 0},
                                 "dimensions": {"datetimeHour": hour, "actionType": "GetObject"}},
                            ],
                            "storage": [{"max": {"payloadSize": 2048, "objectCount": 4}}],
                        }
                    ]
                }
            }
        }  # fmt: skip

    read = ca.R2Bucket("pubs", graphql).read(START, NOW)
    assert asked == [{"bucket": "pubs", "start": _iso(START), "end": _iso(NOW)}]
    assert read.cost is not None and read.cost.startswith("4 object(s), 2048 bytes stored")
    assert "GetObject 0, PutObject 3" in read.cost
    assert [(u.start, u.end) for u in read.uses or []] == [(START + 3600, START + 7200)]


def test_r2_analytics_the_token_may_not_read_leaves_both_unreadable():
    def graphql(query: str, variables: dict) -> dict:
        return {"data": None, "errors": [{"message": "not authorized for that account"}]}

    read = ca.R2Bucket("pubs", graphql).read(START, NOW)
    assert read.uses is None and "not authorized" in read.cost_unreadable


# The wiring


def test_every_standing_pieces_lanes_are_jobs_the_workflows_have():
    import yaml

    root = _PATH.parents[2] / ".github/workflows"
    lanes = yaml.safe_load((root / "integration-suite-lanes.yml").read_text())["jobs"]
    names = {job.get("name", key) for key, job in lanes.items()}
    for job in (ca.WAREHOUSE_LANE, ca.WAREHOUSE_UI, ca.ATHENA_LANE):
        assert job.workflow == "integration-suite.yml"
        assert any(job.name in n for n in names), job
    suite = yaml.safe_load((root / "integration-suite.yml").read_text())["jobs"]
    assert suite["lanes"]["uses"] == "./.github/workflows/integration-suite-lanes.yml"
    for job in (ca.SNOWFLAKE_CLEANUP, ca.AUDIT_JOB):
        jobs = yaml.safe_load((root / job.workflow).read_text())["jobs"]
        assert job.name in jobs, job


def test_the_audit_reads_the_standing_pieces_with_secrets_the_workflows_already_pass():
    import re

    import yaml

    root = _PATH.parents[2] / ".github/workflows"
    text = (root / "cloud-audit.yml").read_text()
    others = "".join(p.read_text() for p in root.glob("*.yml") if p.name != "cloud-audit.yml")
    for name in set(re.findall(r"secrets\.([A-Z0-9_]+)", text)):
        assert f"secrets.{name}" in others, name
    job = yaml.safe_load(text)["jobs"]["audit"]
    assert "standing" in job["env"]["PROVIDERS"]
    # Every credential a piece's reader requires is in the audit's environment.
    source = _PATH.read_text()
    body = source[source.index("def live_pieces") : source.index("def main")]
    required = set()
    for call in re.findall(r"_require\(env,([^)]*)\)", body):
        required |= set(re.findall(r'"([A-Z][A-Z0-9_]+)"', call))
    assert required
    provided = set(job["env"]) | {"GOOGLE_APPLICATION_CREDENTIALS"}  # written by its step
    assert required <= provided, required - provided


def test_every_standing_piece_is_read_each_audit():
    pieces = ca.live_pieces({})
    assert [p.name for p in pieces] == [
        "Snowflake warehouse",
        "Databricks SQL warehouse",
        "GCS bucket (BIGQUERY_TEST_GCS_BUCKET)",
        "Athena S3 bucket (ATHENA_S3_BUCKET)",
        "AWS DataZone domain provisa-e2e-datazone",
        "SingleStore workspace",
        "MotherDuck",
        "Cloudflare R2 bucket pubs",
    ]
    # With no credentials each is reported by name, unreadable, and fails.
    for piece in pieces:
        line = ca.judge(piece, piece.read(START, NOW), {})
        assert line.failed, piece.name
