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

- Azure: the resource groups of the test subscription the live tests create in, each listed by
  itself (the CI principal holds rights on those groups, not read on the subscription); a group
  that does not exist is reported absent by name and holds nothing;
- AWS: the Redshift Serverless workgroups and namespaces of the account and region the Redshift
  tests use (REDSHIFT_AWS_*), named ``provisa-e2e*``;
- GCP: the BigQuery datasets of the project the BigQuery tests use (GOOGLE_CLOUD_PROJECT) whose
  names are the tests' own run-suffixed names.

The standing pieces (maintainer 2026-11-10: a piece that bills nothing while idle may stand
between runs) are not deleted: each audit reports what each billed since the last audit ended, and
fails, naming it, if one billed while no run of the lanes that use it was in progress (the run's
jobs read from the Actions API). See "standing pieces" below for how each one's cost is read.

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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

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
# they create in (FABRIC_RESOURCE_GROUP is my-rg, where the per-run Purview account is made too;
# provisa-e2e, which held the standing one, was deleted 2026-11-10). A group is listed only with
# rights on it: the CI principal's are on my-rg alone, and Azure answers a group it holds none on
# with AuthorizationFailed whether or not that group exists.
# The BigQuery patterns are run_name()'s ``<stem>_<8 hex>`` for every stem a live test creates a
# dataset under (directly, or as an org's replica/export schema).
_RUN_SUFFIX = r"_[0-9a-f]{8}"
SCOPES = Scopes(
    azure=AzureScope(
        subscription="e37b61f0-2a28-4546-b9f9-160bf25b1ab8",
        resource_groups=("my-rg",),
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
    def absent(self) -> list[str]: ...


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
    # Parts of a scope the last listing found not to exist (they hold nothing): reported, not failed.
    absent: list[str] = field(default_factory=list)
    # The standing pieces' cost lines, and when their window began.
    costs: list[CostLine] = field(default_factory=list)
    cost_start: float | None = None

    @property
    def failed(self) -> bool:
        return (
            bool(self.errors)
            or any(r.action in ("delete failed", "left", "error") for r in self.rows)
            or any(c.failed for c in self.costs)
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
        report.absent += [f"{provider.name}: {part}" for part in provider.absent()]
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
    if report.cost_start is not None:
        lines += cost_summary(report.cost_start, report.costs)
    if report.absent:
        lines += ["", "### Absent (hold nothing)", ""] + [f"- {a}" for a in report.absent]
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
    as the CI principal), each group listed by itself. A child resource (a type of more than one
    segment) goes with its parent. A resource group the subscription does not have holds nothing
    and is reported absent by name; any other failure to list a group fails the listing, naming
    the group."""

    name = "azure"

    def __init__(self, scope: AzureScope, az: Callable[..., str]) -> None:
        self._scope = scope
        self._az = az
        self._absent: list[str] = []

    def scope(self) -> str:
        return f"{self._scope.subscription}/{','.join(self._scope.resource_groups)}"

    def absent(self) -> list[str]:
        return list(self._absent)

    def list(self) -> list[Resource]:
        found: list[Resource] = []
        self._absent = []
        for group in self._scope.resource_groups:
            try:
                out = self._az(
                    "resource", "list", "--subscription", self._scope.subscription,
                    "-g", group, "-o", "json",
                )  # fmt: skip
            except RuntimeError as exc:
                if "ResourceGroupNotFound" in str(exc):
                    self._absent.append(
                        f"resource group {group} ({self._scope.subscription}) does not exist"
                    )
                    continue
                raise RuntimeError(f"resource group {group}: {exc}") from exc
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

    def absent(self) -> list[str]:
        return []

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

    def absent(self) -> list[str]:
        return []

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


# ------------------------------------------------------------- standing pieces: what each costs
#
# Maintainer 2026-11-10: a piece that bills nothing while idle (an auto-suspending warehouse, a
# storage bucket, a free tier) may stand between runs. Each audit reports what each one billed
# since the last audit ended, and fails, naming the piece, if one billed while no run of the lanes
# that use it was in progress. A piece whose cost the CI credentials cannot read is reported
# "cost unreadable: <why>" and fails only on a use outside its lanes' runs; one whose use cannot be
# read either fails, since the audit cannot tell that it did not bill.

# A run may wait in the live-cloud-warehouses queue for hours before its job starts: runs created
# this long before the window are asked for their jobs too.
RUN_LOOKBACK_S = 24 * 3600
# A use this close to a using job's start or end is that job's: a warehouse suspends 60 s after
# its last statement, and provider clocks and billing records round to the minute.
USE_SLACK_S = 600
# Athena bills $5 per TB scanned, with a 10 MB minimum per query.
ATHENA_USD_PER_BYTE = 5.0 / 1e12
ATHENA_MIN_BYTES = 10 * 1024 * 1024


@dataclass(frozen=True)
class Job:
    """A job that uses a standing piece: its workflow file, and text of its name as the Actions
    API spells it (a called workflow's jobs are named ``<caller job> / <job>``)."""

    workflow: str
    name: str


WAREHOUSE_LANE = Job("integration-suite.yml", "warehouse (live cloud warehouses)")
WAREHOUSE_UI = Job("integration-suite.yml", "warehouse UI (live cloud warehouses")
ATHENA_LANE = Job("integration-suite.yml", "athena (live account)")
SNOWFLAKE_CLEANUP = Job("snowflake-cleanup.yml", "snowflake-cleanup")
# The audit itself, where its own read of a piece runs on that piece's compute.
AUDIT_JOB = Job("cloud-audit.yml", "audit")


@dataclass(frozen=True)
class Use:
    """Something a piece did that bills, at ``start``..``end`` (epoch seconds)."""

    start: float
    end: float
    what: str


@dataclass
class CostRead:
    """A piece's cost line (None when unreadable, and why) and its uses in the window (None when
    unreadable, and why)."""

    cost: str | None = None
    cost_unreadable: str = ""
    uses: list[Use] | None = None
    usage_unreadable: str = ""


class Piece(Protocol):
    name: str
    users: tuple[Job, ...]

    def read(self, start: float, end: float) -> CostRead: ...


@dataclass
class CostLine:
    piece: str
    cost: str
    verdict: str
    failed: bool


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts(value: Any) -> float:
    """Epoch seconds of an ISO-8601 string or a timezone-aware datetime."""
    if isinstance(value, datetime):
        return value.timestamp()
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


class RunJobs:
    """When the last audit ended, and when each using job ran, from the Actions API."""

    def __init__(self, repo: str, get: Callable[[str], tuple[int, dict]]) -> None:
        self._repo = repo
        self._get = get

    def _ok(self, path: str) -> dict:
        status, body = self._get(path)
        if status != 200:
            raise RuntimeError(f"GitHub answered {status} for {path}: {body}")
        return body

    def last_audit_end(self) -> float:
        """When the latest completed cloud audit run ended: the window's start."""
        body = self._ok(
            f"/repos/{self._repo}/actions/workflows/cloud-audit.yml/runs?status=completed&per_page=1"
        )
        runs = body.get("workflow_runs", [])
        if not runs:
            raise RuntimeError("no completed cloud audit run: the cost window has no start")
        return _ts(runs[0]["updated_at"])

    def _jobs(self, run_id: int) -> list[dict]:
        jobs: list[dict] = []
        page = 1
        while True:
            body = self._ok(
                f"/repos/{self._repo}/actions/runs/{run_id}/jobs?per_page=100&page={page}"
            )
            batch = body.get("jobs", [])
            jobs += batch
            if len(batch) < 100:
                return jobs
            page += 1

    def intervals(
        self, jobs: Iterable[Job], start: float, end: float
    ) -> dict[Job, list[tuple[float, float]]]:
        """Each job's runs that overlap ``start``..``end`` (one still running ends at ``end``)."""
        out: dict[Job, list[tuple[float, float]]] = {j: [] for j in jobs}
        since = quote(f">={_iso(start - RUN_LOOKBACK_S)}")
        for workflow in sorted({j.workflow for j in out}):
            page = 1
            while True:
                body = self._ok(
                    f"/repos/{self._repo}/actions/workflows/{workflow}/runs"
                    f"?created={since}&per_page=100&page={page}"
                )
                runs = body.get("workflow_runs", [])
                for run in runs:
                    for job in self._jobs(run["id"]):
                        if not job.get("started_at"):
                            continue
                        began = _ts(job["started_at"])
                        ended = _ts(job["completed_at"]) if job.get("completed_at") else end
                        if ended < start - USE_SLACK_S or began > end:
                            continue
                        for wanted in out:
                            if wanted.workflow == workflow and wanted.name in job["name"]:
                                out[wanted].append((began, ended))
                if len(runs) < 100:
                    break
                page += 1
        return out


def judge(
    piece: Piece, read: CostRead, intervals: dict[Job, list[tuple[float, float]]]
) -> CostLine:
    """The piece's cost line, failed when it billed outside every run of its lanes, or when
    neither its cost nor its use can be read."""
    cost = read.cost if read.cost is not None else f"cost unreadable: {read.cost_unreadable}"
    if read.uses is None:
        return CostLine(
            piece.name,
            cost,
            f"cannot tell whether it billed: usage unreadable: {read.usage_unreadable}",
            True,
        )
    spans = [span for job in piece.users for span in intervals.get(job, [])]
    stray = [
        u
        for u in read.uses
        if not any(u.start <= e + USE_SLACK_S and u.end >= s - USE_SLACK_S for s, e in spans)
    ]
    if not stray:
        verdict = (
            "no use in the window"
            if not read.uses
            else (f"{len(read.uses)} use(s), each in a run of its lanes")
        )
        return CostLine(piece.name, cost, verdict, False)
    lanes = ", ".join(sorted({f"{j.workflow}: {j.name}" for j in piece.users}))
    shown = "; ".join(f"{u.what} at {_iso(u.start)}" for u in stray[:5])
    more = f"; and {len(stray) - 5} more" if len(stray) > 5 else ""
    return CostLine(
        piece.name, cost, f"billed with no run of its lanes ({lanes}): {shown}{more}", True
    )


def standing_costs(
    pieces: Iterable[Piece], runs: RunJobs, now: float
) -> tuple[float, list[CostLine]]:
    """Read each piece's cost and uses since the last audit ended, and judge each."""
    pieces = list(pieces)
    start = runs.last_audit_end()
    intervals = runs.intervals({j for p in pieces for j in p.users}, start, now)
    lines: list[CostLine] = []
    for piece in pieces:
        try:
            read = piece.read(start, now)
        except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
            read = CostRead(cost_unreadable=str(exc), usage_unreadable=str(exc))
        lines.append(judge(piece, read, intervals))
    return start, lines


class SnowflakeWarehouse:
    """The warehouse's state and last resume from SHOW WAREHOUSES, through a session that names no
    warehouse (resuming nothing). Only when it ran in the window is WAREHOUSE_METERING_HISTORY
    read, through the warehouse itself (a table function needs compute): the audit's own resume
    then falls inside the audit's job, before the next window starts."""

    name = "Snowflake warehouse"
    users = (WAREHOUSE_LANE, WAREHOUSE_UI, SNOWFLAKE_CLEANUP, AUDIT_JOB)

    def __init__(self, warehouse: str, sql: Callable[[str, list, bool], list[dict]]) -> None:
        self._warehouse = warehouse
        self._sql = sql

    def read(self, start: float, end: float) -> CostRead:
        pattern = self._warehouse.replace("'", "''")
        rows = [
            r
            for r in self._sql(f"SHOW WAREHOUSES LIKE '{pattern}'", [], False)
            if str(r["name"]).upper() == self._warehouse.upper()
        ]
        if len(rows) != 1:
            raise RuntimeError(f"{len(rows)} warehouses of the configured name, expected one")
        state = str(rows[0]["state"]).upper()
        resumed = rows[0].get("resumed_on")
        uses: list[Use] = []
        if state not in ("SUSPENDED", "SUSPENDING"):
            uses.append(Use(end, end, f"warehouse {state} now"))
        if resumed is not None and _ts(resumed) >= start:
            uses.append(Use(_ts(resumed), _ts(resumed), "warehouse resumed"))
        if not uses:
            last = _iso(_ts(resumed)) if resumed is not None else "never"
            return CostRead(
                cost=f"0 credits: suspended, last resumed {last}, before the window", uses=[]
            )
        try:
            meter = self._sql(
                "SELECT start_time, end_time, credits_used FROM TABLE("
                "INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY("
                "DATE_RANGE_START => TO_TIMESTAMP_LTZ(%s), DATE_RANGE_END => TO_TIMESTAMP_LTZ(%s), "
                "WAREHOUSE_NAME => %s))",
                [int(start), int(end), self._warehouse],
                True,
            )
        except Exception as exc:  # noqa: BLE001 -- reported as the reason the cost is unreadable
            return CostRead(cost_unreadable=f"WAREHOUSE_METERING_HISTORY: {exc}", uses=uses)
        credits = 0.0
        for row in meter:
            used = float(row["credits_used"])
            credits += used
            if used > 0:
                uses.append(
                    Use(_ts(row["start_time"]), _ts(row["end_time"]), f"{used:.4f} credits")
                )
        return CostRead(cost=f"{credits:.4f} credits (hourly metering)", uses=uses)


class DatabricksWarehouse:
    """The SQL warehouse's state and its query history (REST: neither starts it). Its cost is in
    system.billing.usage, which only a running SQL warehouse reads: reading it would start the
    stopped warehouse being audited."""

    name = "Databricks SQL warehouse"
    users = (WAREHOUSE_LANE, WAREHOUSE_UI)
    COST_UNREADABLE = (
        "system.billing.usage is read only through a running SQL warehouse, and reading it would "
        "start the stopped warehouse; the account billable-usage API needs an account admin, "
        "and the CI token (DATABRICKS_TOKEN) is a workspace token"
    )

    def __init__(self, warehouse_id: str, get: Callable[[str, dict], dict]) -> None:
        self._id = warehouse_id
        self._get = get

    def read(self, start: float, end: float) -> CostRead:
        state = str(self._get(f"/api/2.0/sql/warehouses/{self._id}", {})["state"]).upper()
        uses: list[Use] = []
        if state not in ("STOPPED", "STOPPING", "DELETED", "DELETING"):
            uses.append(Use(end, end, f"warehouse {state} now"))
        params: dict[str, Any] = {
            "filter_by.warehouse_ids": self._id,
            "filter_by.query_start_time_range.start_time_ms": int(start * 1000),
            "filter_by.query_start_time_range.end_time_ms": int(end * 1000),
            "max_results": 1000,
        }
        while True:
            body = self._get("/api/2.0/sql/history/queries", params)
            for q in body.get("res", []):
                began = q["query_start_time_ms"] / 1000
                ended = (q.get("query_end_time_ms") or q["query_start_time_ms"]) / 1000
                uses.append(Use(began, ended, f"query {q.get('query_id', '?')}"))
            if not body.get("has_next_page"):
                break
            params = {"page_token": body["next_page_token"], "max_results": 1000}
        return CostRead(cost_unreadable=self.COST_UNREADABLE, uses=uses)


def _gb_months(size: float, start: float, end: float) -> float:
    return size / 1e9 * (end - start) / (730 * 3600)


class GcsBucket:
    """BIGQUERY_TEST_GCS_BUCKET: what it stores (it bills by the byte-month) and every object
    written in the window (a write outside a run is a use outside a run)."""

    name = "GCS bucket (BIGQUERY_TEST_GCS_BUCKET)"
    users = (WAREHOUSE_LANE,)

    def __init__(self, list_blobs: Callable[[], Iterable[Any]]) -> None:
        self._list = list_blobs

    def read(self, start: float, end: float) -> CostRead:
        count = size = 0
        uses: list[Use] = []
        for blob in self._list():
            count += 1
            size += int(blob.size or 0)
            written = _ts(blob.updated)
            if written >= start:
                uses.append(Use(written, written, f"object {blob.name} written"))
        cost = (
            f"{count} object(s), {size} bytes stored "
            f"({_gb_months(size, start, end):.6f} GB-months in the window)"
        )
        return CostRead(cost=cost, uses=uses)


class AthenaBucket:
    """ATHENA_S3_BUCKET: what it stores, the objects written in the window, and the Athena queries
    run in the window (their results land in the bucket; each bills by the bytes it scanned)."""

    name = "Athena S3 bucket (ATHENA_S3_BUCKET)"
    users = (ATHENA_LANE,)

    def __init__(self, bucket: str, s3: Any, athena: Any) -> None:
        self._bucket = bucket
        self._s3 = s3
        self._athena = athena

    def _queries(self, start: float) -> list[dict]:
        found: list[dict] = []
        token: str | None = None
        while True:
            page = self._athena.list_query_executions(
                MaxResults=50, **({"NextToken": token} if token else {})
            )
            ids = page.get("QueryExecutionIds", [])
            if not ids:
                return found
            got = self._athena.batch_get_query_execution(QueryExecutionIds=ids)
            executions = got.get("QueryExecutions", [])
            older = False
            for q in executions:
                submitted = _ts(q["Status"]["SubmissionDateTime"])
                if submitted >= start:
                    found.append(q)
                else:
                    older = True
            token = page.get("NextToken")
            # Newest first: a page reaching before the window ends the walk.
            if older or not token:
                return found

    def read(self, start: float, end: float) -> CostRead:
        count = size = 0
        uses: list[Use] = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self._bucket):
            for obj in page.get("Contents", []):
                count += 1
                size += int(obj["Size"])
                written = _ts(obj["LastModified"])
                if written >= start:
                    uses.append(Use(written, written, f"object {obj['Key']} written"))
        scanned = billed = 0
        queries = self._queries(start)
        for q in queries:
            status = q["Status"]
            began = _ts(status["SubmissionDateTime"])
            ended = _ts(status["CompletionDateTime"]) if status.get("CompletionDateTime") else end
            bytes_ = int(q.get("Statistics", {}).get("DataScannedInBytes", 0))
            scanned += bytes_
            billed += max(bytes_, ATHENA_MIN_BYTES)
            uses.append(Use(began, ended, f"Athena query {q['QueryExecutionId']}"))
        cost = (
            f"{count} object(s), {size} bytes stored "
            f"({_gb_months(size, start, end):.6f} GB-months in the window); "
            f"{len(queries)} Athena quer(ies) scanned {scanned} bytes "
            f"(${billed * ATHENA_USD_PER_BYTE:.4f} at $5/TB, 10 MB minimum each)"
        )
        return CostRead(cost=cost, uses=uses)


class DataZoneDomain:
    """The DataZone domain's account: its daily Amazon DataZone cost (Cost Explorer) and the
    DataZone API calls of the window (CloudTrail), both with the DataZone test user's keys."""

    name = "AWS DataZone domain provisa-e2e-datazone"
    users = (WAREHOUSE_LANE,)
    SERVICE = "Amazon DataZone"

    def __init__(self, ce: Any, cloudtrail: Any) -> None:
        self._ce = ce
        self._ct = cloudtrail

    def read(self, start: float, end: float) -> CostRead:
        read = CostRead()
        uses: list[Use] = []
        cost_ok = trail_ok = False
        first = datetime.fromtimestamp(start, timezone.utc).date()
        last = datetime.fromtimestamp(end, timezone.utc).date() + timedelta(days=1)
        try:
            body = self._ce.get_cost_and_usage(
                TimePeriod={"Start": first.isoformat(), "End": last.isoformat()},
                Granularity="DAILY",
                Metrics=["UnblendedCost"],
                Filter={"Dimensions": {"Key": "SERVICE", "Values": [self.SERVICE]}},
            )
            total = 0.0
            for day in body.get("ResultsByTime", []):
                amount = float(day["Total"]["UnblendedCost"]["Amount"])
                total += amount
                if amount > 0:
                    d0 = _ts(f"{day['TimePeriod']['Start']}T00:00:00+00:00")
                    d1 = _ts(f"{day['TimePeriod']['End']}T00:00:00+00:00")
                    uses.append(
                        Use(d0, d1, f"${amount:.4f} billed on {day['TimePeriod']['Start']}")
                    )
            read.cost = f"${total:.4f} ({self.SERVICE}, Cost Explorer, daily, since {first})"
            cost_ok = True
        except Exception as exc:  # noqa: BLE001 -- reported as the reason the cost is unreadable
            read.cost_unreadable = f"Cost Explorer GetCostAndUsage: {exc}"
        try:
            for page in self._ct.get_paginator("lookup_events").paginate(
                LookupAttributes=[
                    {"AttributeKey": "EventSource", "AttributeValue": "datazone.amazonaws.com"}
                ],
                StartTime=datetime.fromtimestamp(start, timezone.utc),
                EndTime=datetime.fromtimestamp(end, timezone.utc),
            ):
                for event in page.get("Events", []):
                    at = _ts(event["EventTime"])
                    uses.append(Use(at, at, f"DataZone {event['EventName']}"))
            trail_ok = True
        except Exception as exc:  # noqa: BLE001 -- reported as the reason the usage is unreadable
            read.usage_unreadable = f"CloudTrail LookupEvents: {exc}"
        if cost_ok or trail_ok:
            read.uses = uses
        return read


class SingleStoreWorkspace:
    """The SingleStore workspace the warehouse lane reads. Helios reports billing and compute time
    only through its Management API, whose key no workflow holds; the CI's SQL credentials read
    neither (and a SQL connection resumes a suspended workspace)."""

    name = "SingleStore workspace"
    users = (WAREHOUSE_LANE,)

    def read(self, start: float, end: float) -> CostRead:
        return CostRead(
            cost_unreadable=(
                "Helios billing is read through the Management API, and no workflow passes a "
                "SingleStore Management API key; the CI holds SQL credentials only"
            ),
            usage_unreadable=(
                "the SQL credentials read no compute-time history, and a SQL connection would "
                "resume a suspended workspace"
            ),
        )


class MotherDuckAccount:
    """The MotherDuck account the warehouse lane reads: MotherDuck offers no billing API, and its
    queries are read from MD_INFORMATION_SCHEMA.QUERY_HISTORY (a Business-plan view)."""

    name = "MotherDuck"
    users = (WAREHOUSE_LANE, AUDIT_JOB)
    COST_UNREADABLE = "MotherDuck offers no billing API a token can read"

    def __init__(self, sql: Callable[[str, list], list[dict]]) -> None:
        self._sql = sql

    def read(self, start: float, end: float) -> CostRead:
        try:
            rows = self._sql(
                "SELECT query_id, start_time, end_time FROM md_information_schema.query_history "
                "WHERE start_time >= to_timestamp(?) ORDER BY start_time",
                [start],
            )
        except Exception as exc:  # noqa: BLE001 -- reported as the reason the usage is unreadable
            return CostRead(
                cost_unreadable=self.COST_UNREADABLE,
                usage_unreadable=f"MD_INFORMATION_SCHEMA.QUERY_HISTORY: {exc}",
            )
        uses = [
            Use(
                _ts(r["start_time"]),
                _ts(r["end_time"] or r["start_time"]),
                f"query {r['query_id']}",
            )
            for r in rows
        ]
        return CostRead(cost_unreadable=self.COST_UNREADABLE, uses=uses)


R2_QUERY = """
query ($account: String!, $bucket: String!, $start: Time!, $end: Time!) {
  viewer {
    accounts(filter: {accountTag: $account}) {
      ops: r2OperationsAdaptiveGroups(
        limit: 10000
        filter: {bucketName: $bucket, datetime_geq: $start, datetime_leq: $end}
      ) {
        sum { requests }
        dimensions { datetimeHour actionType }
      }
      storage: r2StorageAdaptiveGroups(
        limit: 1
        filter: {bucketName: $bucket, datetime_geq: $start, datetime_leq: $end}
        orderBy: [datetime_DESC]
      ) {
        max { payloadSize objectCount }
      }
    }
  }
}
"""


class R2Bucket:
    """The R2 bucket the warehouse lane's external-link tests write under run prefixes
    (tests/integration/r2_objects.py): its stored bytes and its operations per hour, from
    Cloudflare's GraphQL analytics."""

    users = (WAREHOUSE_LANE,)

    def __init__(self, bucket: str, graphql: Callable[[str, dict], dict]) -> None:
        self._bucket = bucket
        self._graphql = graphql
        self.name = f"Cloudflare R2 bucket {bucket}"

    def read(self, start: float, end: float) -> CostRead:
        try:
            body = self._graphql(
                R2_QUERY, {"bucket": self._bucket, "start": _iso(start), "end": _iso(end)}
            )
            if body.get("errors"):
                raise RuntimeError(json.dumps(body["errors"]))
            accounts = body["data"]["viewer"]["accounts"]
            if len(accounts) != 1:
                raise RuntimeError(f"{len(accounts)} accounts answered, expected one")
        except Exception as exc:  # noqa: BLE001 -- reported; both cost and use come from here
            why = f"Cloudflare GraphQL analytics: {exc}"
            return CostRead(cost_unreadable=why, usage_unreadable=why)
        account = accounts[0]
        uses: list[Use] = []
        by_action: dict[str, int] = {}
        for group in account.get("ops", []):
            n = int(group["sum"]["requests"])
            action = group["dimensions"]["actionType"]
            hour = _ts(group["dimensions"]["datetimeHour"])
            by_action[action] = by_action.get(action, 0) + n
            if n > 0:
                uses.append(Use(hour, hour + 3600, f"{n} {action} request(s) in the hour"))
        stored = account.get("storage", [])
        size = int(stored[0]["max"]["payloadSize"]) if stored else 0
        objects = int(stored[0]["max"]["objectCount"]) if stored else 0
        ops = ", ".join(f"{a} {n}" for a, n in sorted(by_action.items())) or "none"
        cost = (
            f"{objects} object(s), {size} bytes stored "
            f"({_gb_months(size, start, end):.6f} GB-months in the window); requests: {ops}"
        )
        return CostRead(cost=cost, uses=uses)


def cost_summary(start: float, lines: list[CostLine]) -> list[str]:
    out = [
        "",
        f"### Standing pieces: cost since the last audit ended ({_iso(start)})",
        "",
        "| piece | cost | verdict |",
        "|---|---|---|",
    ]
    for line in lines:
        mark = "FAIL: " if line.failed else ""
        out.append(f"| {line.piece} | {line.cost} | {mark}{line.verdict} |")
    return out


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


# The R2 bucket the external-link tests write under run prefixes (tests/integration/r2_objects.py
# names it; PROVISA_R2_TEST_BUCKET is set by no workflow).
R2_TEST_BUCKET = "pubs"
# The DataZone domain's region (tests/integration/test_metadata_export_datazone_e2e.py REGION).
DATAZONE_REGION = "us-east-1"


def _snowflake_sql(env: dict[str, str]) -> Callable[[str, list, bool], list[dict]]:
    """A statement run in a fresh session: with no warehouse (resuming nothing), or, when asked,
    on the configured warehouse and database (a table function needs compute)."""

    def run(statement: str, params: list, warehouse: bool) -> list[dict]:
        import snowflake.connector

        kwargs: dict[str, str] = {
            "account": env["SNOWFLAKE_ACCOUNT"],
            "user": env["SNOWFLAKE_USER"],
            "password": env["SNOWFLAKE_PASSWORD"],
        }
        if warehouse:
            kwargs["warehouse"] = env["SNOWFLAKE_WAREHOUSE"]
            kwargs["database"] = env["SNOWFLAKE_DATABASE"]
        conn = snowflake.connector.connect(**kwargs)
        try:
            cur = conn.cursor()
            cur.execute(statement, params or None)
            columns = [d[0].lower() for d in cur.description or []]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    return run


def _databricks_get(env: dict[str, str]) -> Callable[[str, dict], dict]:
    def get(path: str, params: dict) -> dict:
        import httpx

        response = httpx.get(
            f"https://{env['DATABRICKS_SERVER_HOSTNAME']}{path}",
            params=params,
            headers={"Authorization": f"Bearer {env['DATABRICKS_TOKEN']}"},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Databricks answered {response.status_code} for {path}: {response.text[:500]}"
            )
        return response.json()

    return get


def _databricks_warehouse_id(http_path: str) -> str:
    match = re.search(r"/warehouses/([0-9a-fA-F]+)", http_path)
    if match is None:
        raise RuntimeError("DATABRICKS_HTTP_PATH does not name a SQL warehouse")
    return match.group(1)


def _motherduck_sql(env: dict[str, str]) -> Callable[[str, list], list[dict]]:
    def run(statement: str, params: list) -> list[dict]:
        import duckdb

        conn = duckdb.connect("md:", config={"motherduck_token": env["MOTHERDUCK_API_TOKEN"]})
        try:
            cur = conn.execute(statement, params)
            columns = [d[0].lower() for d in cur.description or []]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    return run


def _cloudflare_graphql(env: dict[str, str]) -> Callable[[str, dict], dict]:
    def post(query: str, variables: dict) -> dict:
        import httpx

        response = httpx.post(
            "https://api.cloudflare.com/client/v4/graphql",
            json={
                "query": query,
                "variables": {**variables, "account": env["CLOUDFLARE_ACCOUNT_ID"]},
            },
            headers={"Authorization": f"Bearer {env['CLOUDFLARE_API_TOKEN']}"},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Cloudflare answered {response.status_code}: {response.text[:500]}")
        return response.json()

    return post


class _Unstartable:
    """A piece whose reader could not be built: both its reads fail with why."""

    users: tuple[Job, ...] = ()

    def __init__(self, name: str, why: str) -> None:
        self.name = name
        self._why = why

    def read(self, start: float, end: float) -> CostRead:
        return CostRead(cost_unreadable=self._why, usage_unreadable=self._why)


def live_pieces(env: dict[str, str]) -> list[Piece]:
    """Every standing piece, read with the credentials the live lanes already hold. A piece whose
    credentials are missing is reported, by name, as unreadable, and fails the audit."""

    def snowflake() -> Piece:
        _require(env, "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                 "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE")  # fmt: skip
        return SnowflakeWarehouse(env["SNOWFLAKE_WAREHOUSE"], _snowflake_sql(env))

    def databricks() -> Piece:
        _require(env, "DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
        return DatabricksWarehouse(
            _databricks_warehouse_id(env["DATABRICKS_HTTP_PATH"]), _databricks_get(env)
        )

    def gcs() -> Piece:
        _require(env, "BIGQUERY_TEST_GCS_BUCKET", "GOOGLE_CLOUD_PROJECT",
                 "GOOGLE_APPLICATION_CREDENTIALS")  # fmt: skip
        refuse_cloud_dev([env["GOOGLE_CLOUD_PROJECT"]])
        from google.cloud import storage

        client = storage.Client(project=env["GOOGLE_CLOUD_PROJECT"])
        bucket = env["BIGQUERY_TEST_GCS_BUCKET"]
        return GcsBucket(lambda: client.list_blobs(bucket))

    def athena() -> Piece:
        _require(env, "ATHENA_AWS_ACCESS_KEY_ID", "ATHENA_AWS_SECRET_ACCESS_KEY",
                 "ATHENA_REGION", "ATHENA_S3_BUCKET")  # fmt: skip
        import boto3

        session = boto3.Session(
            aws_access_key_id=env["ATHENA_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=env["ATHENA_AWS_SECRET_ACCESS_KEY"],
            region_name=env["ATHENA_REGION"],
        )
        return AthenaBucket(env["ATHENA_S3_BUCKET"], session.client("s3"), session.client("athena"))

    def datazone() -> Piece:
        _require(env, "PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID",
                 "PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY")  # fmt: skip
        import boto3

        session = boto3.Session(
            aws_access_key_id=env["PROVISA_E2E_DATAZONE_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=env["PROVISA_E2E_DATAZONE_AWS_SECRET_ACCESS_KEY"],
            region_name=DATAZONE_REGION,
        )
        return DataZoneDomain(session.client("ce"), session.client("cloudtrail"))

    def motherduck() -> Piece:
        _require(env, "MOTHERDUCK_API_TOKEN")
        return MotherDuckAccount(_motherduck_sql(env))

    def r2() -> Piece:
        _require(env, "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN")
        return R2Bucket(R2_TEST_BUCKET, _cloudflare_graphql(env))

    builders: list[tuple[str, Callable[[], Piece]]] = [
        (SnowflakeWarehouse.name, snowflake),
        (DatabricksWarehouse.name, databricks),
        (GcsBucket.name, gcs),
        (AthenaBucket.name, athena),
        (DataZoneDomain.name, datazone),
        (SingleStoreWorkspace.name, SingleStoreWorkspace),
        (MotherDuckAccount.name, motherduck),
        (f"Cloudflare R2 bucket {R2_TEST_BUCKET}", r2),
    ]
    pieces: list[Piece] = []
    for name, build in builders:
        try:
            pieces.append(build())
        except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
            pieces.append(_Unstartable(name, f"cannot start: {exc}"))
    return pieces


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="The paid-cloud audit of the test scopes")
    parser.add_argument("--dry-run", action="store_true", help="list and classify, delete nothing")
    parser.add_argument(
        "--providers",
        default="azure,aws,gcp,standing",
        help="azure, aws, gcp (resources to delete) and standing (the standing pieces' costs)",
    )
    args = parser.parse_args(argv)
    env = dict(os.environ)
    check_scopes(SCOPES, env)
    _require(env, "GITHUB_REPOSITORY", "GITHUB_TOKEN")
    runs = GitHubRuns(env["GITHUB_REPOSITORY"], _github_get)
    report = Report()
    providers: list[Provider] = []
    asked = [p.strip() for p in args.providers.split(",") if p.strip()]
    for which in [p for p in asked if p != "standing"]:
        try:
            providers += live_providers(SCOPES, env, [which])
        except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
            report.errors.append(f"{which}: cannot start: {exc}")
    found = audit(providers, runs, time.time(), dry_run=args.dry_run)
    report.rows += found.rows
    report.errors += found.errors
    report.absent += found.absent
    if "standing" in asked:
        try:
            report.cost_start, report.costs = standing_costs(
                live_pieces(env), RunJobs(env["GITHUB_REPOSITORY"], _github_get), time.time()
            )
        except Exception as exc:  # noqa: BLE001 -- reported, and fails the audit
            report.errors.append(f"standing pieces: {exc}")
    text = summary(report, dry_run=args.dry_run)
    print(text)
    target = env.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as fh:
            fh.write(text)
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
