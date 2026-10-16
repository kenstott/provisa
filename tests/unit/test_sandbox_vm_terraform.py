# Copyright (c) 2026 Kenneth Stott
# Canary: 4f9fa68f-e7ce-41d0-b926-55aae0a632c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The sandbox VM (terraform/gcp-saas/sandbox.tf, REQ-2006): the sandbox tier and the demos'
stores on a VM of their own, against the coordinator's control plane, waking and stopping by
the Compute API -- and nothing changed for a deployment that does not ask for it."""

# Requirements: REQ-2006

from __future__ import annotations

from pathlib import Path

_TF = Path(__file__).resolve().parents[2] / "terraform" / "gcp-saas"


def test_the_sandbox_vm_runs_the_tier_alone_against_the_coordinator():
    sandbox = (_TF / "sandbox.tf").read_text()
    for line in (
        "export COMPOSE_PROFILES=sandbox",
        'export CONFIG_DB_HOST="${local.coordinator_ip}"',
        'export PROVISA_REDIS_HOST="${local.coordinator_ip}"',
        'export PROVISA_MINIO_HOST="${local.coordinator_ip}"',
        'export PROVISA_SANDBOX_INSTANCE="${local.sandbox_instance_path}"',
        'export PROVISA_SANDBOX_IDLE_STOP_SECONDS="${var.sandbox_idle_stop_minutes * 60}"',
        "--role control-plane",
    ):
        assert line in sandbox, line
    # The VM's own control plane is not set up there: no database, no object-store disk.
    startup = sandbox[sandbox.index('resource "google_compute_instance" "sandbox"') :]
    assert "control_db_setup" not in startup and "object_store_mount" not in startup
    assert "desired_status = null" in startup


def test_the_coordinator_hands_the_tier_over_only_when_asked():
    sandbox = (_TF / "sandbox.tf").read_text()
    main = (_TF / "main.tf").read_text()
    assert "${local.coordinator_sandbox_exports}" in main
    assert (
        'coordinator_sandbox_exports    = var.sandbox_vm ? local.coordinator_sandbox_vm_exports : ""'
        in sandbox
    )
    for line in (
        "export COMPOSE_PROFILES=control",
        'export PROVISA_SANDBOX_API_URL="https://${var.sandbox_internal_ip}:8000"',
        # REQ-2068: the regions sandbox tier, on the same VM.
        'export PROVISA_SANDBOX_REGIONS_API_URL="https://${var.sandbox_internal_ip}:8100"',
    ):
        assert line in sandbox, line
    variables = (_TF / "variables.tf").read_text()
    block = variables[variables.index('variable "sandbox_vm"') :]
    assert "default     = false" in block[: block.index("}\n") + 2]


def test_the_vm_may_be_started_and_stopped_by_the_node_account_and_on_it_alone():
    sandbox = (_TF / "sandbox.tf").read_text()
    role = sandbox[sandbox.index('resource "google_project_iam_custom_role" "sandbox_power"') :]
    for permission in ("start", "stop", "resume", "get"):
        assert f'"compute.instances.{permission}"' in role
    assert 'resource "google_compute_instance_iam_member" "sandbox_power"' in sandbox
    assert "instance_name = google_compute_instance.sandbox[0].name" in sandbox


def test_the_vm_runs_the_retail_demos_stores():
    """REQ-2068: the regions tier's demo is synced with the others, its stores run there."""
    sandbox = (_TF / "sandbox.tf").read_text()
    assert "PROVISA_DEMO_BUNDLES=weblogs,files,sonnets,reviews,retail" in sandbox
