#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3b7e21-4c58-4f0a-b6e2-1a8c5f3d7e94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The paid-cloud audit: no billable resource of Provisa's testing outlives its run.

Maintainer 2026-11-10 (.claude/refs/swarm-mode.md, "Paid cloud resources"). Every 4 hours
(.github/workflows/cloud-audit.yml) this lists the billable resources in each test scope and
deletes every one that is not active. Active means tagged ``provisa-run`` with a run still in
progress (tests/integration/cloud_run_tag.py): a GitHub Actions run whose status is not yet
completed, or a local run younger than ``LOCAL_LEASE_S`` (a local run has no status to ask). An
untagged resource in a test scope, or one of a finished run, is deleted, then read back; one still
there fails the audit. What it found and what it deleted go to the run summary.

The test scopes are data (``SCOPES``), and nothing outside them is listed or touched:

- Azure: the resource groups of the test subscription the live tests create in;
- AWS: the Redshift Serverless workgroups and namespaces of the account and region the Redshift
  tests use (REDSHIFT_AWS_*), named ``provisa-e2e*``;
- GCP: the BigQuery datasets of the project the BigQuery tests use (GOOGLE_CLOUD_PROJECT) whose
  names are the tests' own run-suffixed names.

Cloud-dev (the GCP project provisa-test-473, cloud.provisa.dev and its GKE engine shards) and
local-dev are never a scope: a scope naming one is refused by name before anything is listed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tests.integration.cloud_run_tag import (  # noqa: E402
    CREATED_KEY,
    GITHUB_PREFIX,
    LOCAL_PREFIX,
    RUN_KEY,
)

# Cloud-dev and local-dev, by every name a scope could carry: never audited, never touched.
CLOUD_DEV = frozenset({"provisa-test-473", "cloud.provisa.dev", "provisa.dev"})

# A local run has no status to ask: its resources count as in progress for this long after they
# were made -- longer than any live lane's bound (the warehouse lane's is 180 minutes).
LOCAL_LEASE_S = 6 * 3600

# GitHub's run statuses before "completed".
_RUN_IN_PROGRESS = frozenset(
    {"queued", "in_progress", "waiting", "requested", "pending", "action_required"}
)


@dataclass(frozen=True)
class AzureScope:
    subscription: str
    resource_groups: tuple[str, ...]


@dataclass(frozen=True)
class AwsScope:
    """Redshift Serverless in the account and region the credentials in these variables reach."""

    access_key_env: str
    secret_key_env: str
    region_env: str
    name_prefix: str


@dataclass(frozen=True)
class GcpScope:
    """BigQuery datasets of the project named by ``project_env`` whose names match one of
    ``dataset_patterns`` (the tests' run-suffixed names)."""

    project_env: str
    credentials_env: str
    dataset_patterns: tuple[str, ...]


@dataclass(frozen=True)
class Scopes:
    azure: AzureScope
    aws: AwsScope
    gcp: GcpScope


# The test scopes. Azure: the subscription the live Azure tests run in and the resource groups
# they create in (FABRIC_RESOURCE_GROUP is my-rg; provisa-e2e held the Purview account).
# The BigQuery patterns are run_name()'s ``<stem>_<8 hex>`` for every stem a live test creates a
# dataset under (directly, or as an org's replica/export schema).
_RUN_SUFFIX = r"_[0-9a-f]{8}"
SCOPES = Scopes(
    azure=AzureScope(
        subscription="e37b61f0-2a28-4546-b9f9-160bf25b1ab8",
        resource_groups=("my-rg", "provisa-e2e"),
    ),
    aws=AwsScope(
        access_key_env="REDSHIFT_AWS_ACCESS_KEY_ID",
        secret_key_env="REDSHIFT_AWS_SECRET_ACCESS_KEY",
        region_env="REDSHIFT_AWS_REGION",
        name_prefix="provisa-e2e",
    ),
    gcp=GcpScope(
        project_env="GOOGLE_CLOUD_PROJECT",
        credentials_env="GOOGLE_APPLICATION_CREDENTIALS",
        dataset_patterns=(
            rf"^provisa_e2e_it{_RUN_SUFFIX}$",
            rf"^provisa_ext_it_ds{_RUN_SUFFIX}$",
            rf"^provisa_parity_it{_RUN_SUFFIX}$",
            rf"swaptest{_RUN_SUFFIX}",
            rf"mdx_dataplex_e2e{_RUN_SUFFIX}",
        ),
    ),
)


class ScopeRefused(Exception):
    """A scope names cloud-dev or local-dev."""


def refuse_cloud_dev(names: Iterable[str]) -> None:
    """Raise, naming it, if any scope identifier is cloud-dev's or local-dev's."""
    for name in names:
        lowered = name.strip().lower()
        hit = next((dev for dev in CLOUD_DEV if dev in lowered), None)
        if hit is not None:
            raise ScopeRefused(
                f"scope {name!r} is cloud-dev ({hit}): the audit never lists or deletes there"
            )


def check_scopes(scopes: Scopes, env: dict[str, str]) -> None:
    """Refuse the scopes if any names cloud-dev, including the GCP project the environment names."""
    refuse_cloud_dev([scopes.azure.subscription, *scopes.azure.resource_groups])
    project = env.get(scopes.gcp.project_env, "")
    if project:
        refuse_cloud_dev([project])


@dataclass(frozen=True)
class Resource:
    provider: str
    scope: str
    kind: str
    id: str
    name: str
    tags: dict[str, str] = field(default_factory=dict)


class Provider(Protocol):
    name: str

    def scope(self) -> str: ...
    def list(self) -> list[Resource]: ...
    def delete(self, resource: Resource) -> None: ...
    def exists(self, resource: Resource) -> bool: ...


class Runs(Protocol):
    def in_progress(self, run: str, created: int | None, now: float) -> bool: ...


# ------------------------------------------------------------------------------- classification


@dataclass(frozen=True)
class Verdict:
    keep: bool
    reason: str


def _created(tags: dict[str, str]) -> int | None:
    value = tags.get(CREATED_KEY, "")
    return int(value) if value.isdigit() else None


def classify(resource: Resource, runs: Runs, now: float) -> Verdict:
    """Keep a resource only while the run its tag names is in progress."""
    run = resource.tags.get(RUN_KEY)
    if not run:
        return Verdict(False, "untagged")
    if runs.in_progress(run, _created(resource.tags), now):
        return Verdict(True, f"run {run} in progress")
    return Verdict(False, f"run {run} not in progress")


class GitHubRuns:
    """Whether a run is in progress: a GitHub Actions run's status, read from the API; a local
    run's age against ``LOCAL_LEASE_S``. A run id of neither form is no run in progress."""

    def __init__(self, repo: str, get: Callable[[str], tuple[int, dict]]) -> None:
        self._repo = repo
        self._get = get
        self._seen: dict[str, bool] = {}

    def in_progress(self, run: str, created: int | None, now: float) -> bool:
        if run.startswith(LOCAL_PREFIX):
            return created is not None and now - created < LOCAL_LEASE_S
        if not run.startswith(GITHUB_PREFIX) or not run[len(GITHUB_PREFIX) :].isdigit():
            return False
        if run not in self._seen:
            run_id = run[len(GITHUB_PREFIX) :]
            status, body = self._get(f"/repos/{self._repo}/actions/runs/{run_id}")
            if status == 404:
                self._seen[run] = False
            elif status == 200:
                self._seen[run] = body.get("status") in _RUN_IN_PROGRESS
            else:
                raise RuntimeError(f"GitHub answered {status} for run {run_id}: {body}")
        return self._seen[run]


# ------------------------------------------------------------------------------------- the audit


@dataclass
class Row:
    resource: Resource
    action: str  # kept | deleted | would delete | delete failed | left | error
    reason: str


@dataclass
class Report:
    rows: list[Row] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return bool(self.errors) or any(
            r.action in ("delete failed", "left", "error") for r in self.rows
        )


def audit(providers: Iterable[Provider], runs: Runs, now: float, *, dry_run: bool) -> Report:
    """List each provider's resources, delete each that is not active, then read each deleted one
    back. Every step is attempted; every failure is reported."""
    report = Report()
    for provider in providers:
        try:
            resources = provider.list()
        except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
            report.errors.append(f"{provider.name} ({provider.scope()}): listing failed: {exc}")
            continue
        deleted: list[Row] = []
        for resource in resources:
            try:
                verdict = classify(resource, runs, now)
            except Exception as exc:  # noqa: BLE001 -- kept, reported, fails the audit
                report.rows.append(Row(resource, "error", f"cannot tell its run: {exc}"))
                continue
            if verdict.keep:
                report.rows.append(Row(resource, "kept", verdict.reason))
                continue
            if dry_run:
                report.rows.append(Row(resource, "would delete", verdict.reason))
                continue
            try:
                provider.delete(resource)
            except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
                report.rows.append(Row(resource, "delete failed", f"{verdict.reason}: {exc}"))
                continue
            row = Row(resource, "deleted", verdict.reason)
            report.rows.append(row)
            deleted.append(row)
        for row in deleted:
            try:
                still = provider.exists(row.resource)
            except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
                row.action, row.reason = "left", f"{row.reason}; read-back failed: {exc}"
                continue
            if still:
                row.action, row.reason = "left", f"{row.reason}; still there after its delete"
    return report


def summary(report: Report, *, dry_run: bool) -> str:
    """The run summary: every resource found, what was done to it and why."""
    lines = [
        f"## Paid cloud audit{' (dry run)' if dry_run else ''}",
        "",
        f"{len(report.rows)} resource(s) found; "
        f"{sum(r.action == 'deleted' for r in report.rows)} deleted; "
        f"{sum(r.action == 'kept' for r in report.rows)} kept (run in progress).",
        "",
    ]
    if report.rows:
        lines += [
            "| provider | scope | kind | name | run | created | action | why |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for row in report.rows:
            r = row.resource
            lines.append(
                f"| {r.provider} | {r.scope} | {r.kind} | {r.name} | "
                f"{r.tags.get(RUN_KEY, '-')} | {r.tags.get(CREATED_KEY, '-')} | "
                f"{row.action} | {row.reason} |"
            )
    if report.errors:
        lines += ["", "### Errors", ""] + [f"- {e}" for e in report.errors]
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------------------------- providers


def _not_found(text: str) -> bool:
    return any(
        m in text for m in ("ResourceNotFound", "NotFound", "Not Found", "could not be found")
    )


class AzureProvider:
    """The top-level resources of the scope's resource groups, through the Azure CLI (logged in
    as the CI principal). A child resource (a type of more than one segment) goes with its
    parent; a resource group the subscription does not have holds nothing."""

    name = "azure"

    def __init__(self, scope: AzureScope, az: Callable[..., str]) -> None:
        self._scope = scope
        self._az = az

    def scope(self) -> str:
        return f"{self._scope.subscription}/{','.join(self._scope.resource_groups)}"

    def list(self) -> list[Resource]:
        found: list[Resource] = []
        for group in self._scope.resource_groups:
            try:
                out = self._az(
                    "resource", "list", "--subscription", self._scope.subscription,
                    "-g", group, "-o", "json",
                )  # fmt: skip
            except RuntimeError as exc:
                if "ResourceGroupNotFound" in str(exc):
                    continue
                raise
            for item in json.loads(out or "[]"):
                if item["type"].count("/") != 1:
                    continue
                found.append(
                    Resource(
                        "azure",
                        f"{self._scope.subscription}/{group}",
                        item["type"],
                        item["id"],
                        item["name"],
                        dict(item.get("tags") or {}),
                    )
                )
        return found

    def _in_scope(self, resource: Resource) -> None:
        prefixes = [
            f"/subscriptions/{self._scope.subscription}/resourcegroups/{g}/".lower()
            for g in self._scope.resource_groups
        ]
        if not any(resource.id.lower().startswith(p) for p in prefixes):
            raise ScopeRefused(f"{resource.id} is outside the Azure test scope")

    def delete(self, resource: Resource) -> None:
        self._in_scope(resource)
        self._az("resource", "delete", "--ids", resource.id)

    def exists(self, resource: Resource) -> bool:
        self._in_scope(resource)
        try:
            self._az("resource", "show", "--ids", resource.id, "-o", "none")
        except RuntimeError as exc:
            if _not_found(str(exc)):
                return False
            raise
        return True


class RedshiftServerlessProvider:
    """The scope's Redshift Serverless workgroups, then namespaces (a namespace is deleted only
    once no workgroup refers to it, so workgroups are listed, and deleted, first)."""

    name = "aws"

    def __init__(
        self,
        scope: AwsScope,
        client: Any,
        region: str,
        *,
        sleep: Callable[[float], None] = time.sleep,
        bound_s: float = 1800,
    ) -> None:
        self._scope = scope
        self._rss = client
        self._region = region
        self._sleep = sleep
        self._bound_s = bound_s

    def scope(self) -> str:
        return f"redshift-serverless/{self._region}/{self._scope.name_prefix}*"

    def _tags(self, arn: str) -> dict[str, str]:
        listed = self._rss.list_tags_for_resource(resourceArn=arn).get("tags", [])
        return {t["key"]: t["value"] for t in listed}

    def list(self) -> list[Resource]:
        found: list[Resource] = []
        for wg in self._rss.list_workgroups().get("workgroups", []):
            name = wg["workgroupName"]
            if name.startswith(self._scope.name_prefix):
                arn = wg["workgroupArn"]
                found.append(Resource("aws", self.scope(), "workgroup", arn, name, self._tags(arn)))
        for ns in self._rss.list_namespaces().get("namespaces", []):
            name = ns["namespaceName"]
            if name.startswith(self._scope.name_prefix):
                arn = ns["namespaceArn"]
                found.append(Resource("aws", self.scope(), "namespace", arn, name, self._tags(arn)))
        return found

    def delete(self, resource: Resource) -> None:
        if not resource.name.startswith(self._scope.name_prefix):
            raise ScopeRefused(f"{resource.name} is outside the Redshift test scope")
        if resource.kind == "workgroup":
            self._rss.delete_workgroup(workgroupName=resource.name)
        else:
            self._rss.delete_namespace(namespaceName=resource.name)
        # A namespace's delete is refused while its workgroup still stands: wait each out.
        deadline = time.monotonic() + self._bound_s
        while self.exists(resource):
            if time.monotonic() >= deadline:
                raise RuntimeError(f"{resource.kind} {resource.name} still deleting")
            self._sleep(15)

    def exists(self, resource: Resource) -> bool:
        try:
            if resource.kind == "workgroup":
                self._rss.get_workgroup(workgroupName=resource.name)
            else:
                self._rss.get_namespace(namespaceName=resource.name)
        except Exception as exc:  # noqa: BLE001 -- only "not found" means gone
            if "ResourceNotFound" in type(exc).__name__ or "ResourceNotFound" in str(exc):
                return False
            raise
        return True


class BigQueryProvider:
    """The scope's BigQuery datasets: those of the project whose names are a test's own."""

    name = "gcp"

    def __init__(self, scope: GcpScope, client: Any, project: str) -> None:
        refuse_cloud_dev([project])
        self._scope = scope
        self._bq = client
        self._project = project
        self._patterns = [re.compile(p) for p in scope.dataset_patterns]

    def scope(self) -> str:
        return f"bigquery/{self._project}"

    def _ours(self, name: str) -> bool:
        return any(p.search(name) for p in self._patterns)

    def list(self) -> list[Resource]:
        found: list[Resource] = []
        for item in self._bq.list_datasets(self._project):
            name = item.dataset_id
            if not self._ours(name):
                continue
            dataset = self._bq.get_dataset(f"{self._project}.{name}")
            found.append(
                Resource(
                    "gcp",
                    self.scope(),
                    "dataset",
                    f"{self._project}.{name}",
                    name,
                    dict(dataset.labels or {}),
                )
            )
        return found

    def delete(self, resource: Resource) -> None:
        if not resource.id.startswith(f"{self._project}.") or not self._ours(resource.name):
            raise ScopeRefused(f"{resource.id} is outside the BigQuery test scope")
        self._bq.delete_dataset(resource.id, delete_contents=True, not_found_ok=True)

    def exists(self, resource: Resource) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            self._bq.get_dataset(resource.id)
        except NotFound:
            return False
        return True


# ------------------------------------------------------------------------------------ live wiring


def _az(*args: str, timeout: int = 1800) -> str:
    proc = subprocess.run(
        ["az", *args], capture_output=True, text=True, timeout=timeout, check=False
    )
    if proc.returncode != 0:
        raise RuntimeError(f"az {' '.join(args[:3])} failed ({proc.returncode}): {proc.stderr}")
    return proc.stdout.strip()


def _github_get(path: str) -> tuple[int, dict]:
    import httpx

    response = httpx.get(
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github+json",
        },
        timeout=60,
    )
    return response.status_code, response.json()


def _require(env: dict[str, str], *names: str) -> None:
    missing = [n for n in names if not env.get(n)]
    if missing:
        raise RuntimeError(f"the audit needs {', '.join(missing)}")


def live_providers(scopes: Scopes, env: dict[str, str], which: list[str]) -> list[Provider]:
    providers: list[Provider] = []
    if "azure" in which:
        providers.append(AzureProvider(scopes.azure, _az))
    if "aws" in which:
        import boto3

        a = scopes.aws
        _require(env, a.access_key_env, a.secret_key_env, a.region_env)
        session = boto3.Session(
            aws_access_key_id=env[a.access_key_env],
            aws_secret_access_key=env[a.secret_key_env],
            region_name=env[a.region_env],
        )
        providers.append(
            RedshiftServerlessProvider(a, session.client("redshift-serverless"), env[a.region_env])
        )
    if "gcp" in which:
        from google.cloud import bigquery

        g = scopes.gcp
        _require(env, g.project_env, g.credentials_env)
        project = env[g.project_env]
        providers.append(BigQueryProvider(g, bigquery.Client(project=project), project))
    return providers


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="The paid-cloud audit of the test scopes")
    parser.add_argument("--dry-run", action="store_true", help="list and classify, delete nothing")
    parser.add_argument("--providers", default="azure,aws,gcp")
    args = parser.parse_args(argv)
    env = dict(os.environ)
    check_scopes(SCOPES, env)
    _require(env, "GITHUB_REPOSITORY", "GITHUB_TOKEN")
    runs = GitHubRuns(env["GITHUB_REPOSITORY"], _github_get)
    report = Report()
    providers: list[Provider] = []
    for which in [p.strip() for p in args.providers.split(",") if p.strip()]:
        try:
            providers += live_providers(SCOPES, env, [which])
        except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
            report.errors.append(f"{which}: cannot start: {exc}")
    found = audit(providers, runs, time.time(), dry_run=args.dry_run)
    report.rows += found.rows
    report.errors += found.errors
    text = summary(report, dry_run=args.dry_run)
    print(text)
    target = env.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as fh:
            fh.write(text)
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
