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
