# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6a8d33-9e2c-4b57-a0d4-c7b3e5f2a918
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The coordinator's warehouse and the catalog that names its tables share one disk, and one
lifecycle (terraform/gcp-saas/main.tf).

The telemetry warehouse (MinIO) and the control DB holding the ``otel`` catalog's table rows both
live on the object-store disk. A wake that finds its startup script unchanged runs none of it,
and Docker's restart policy brings the stack back -- before the disk is mounted, unless Docker
waits for it. A MinIO started first served the empty directory beneath the mountpoint to a
catalog that still named every table: ICEBERG_MISSING_METADATA on every telemetry statement."""

from __future__ import annotations

from pathlib import Path

_MAIN = (Path(__file__).resolve().parents[2] / "terraform" / "gcp-saas" / "main.tf").read_text()


def _local(name: str) -> str:
    start = _MAIN.index(f"  {name} = <<-SHELL")
    return _MAIN[start : _MAIN.index("\n  SHELL", start)]


def test_docker_waits_for_the_object_store_disk():
    mount = _local("object_store_mount")
    assert "RequiresMountsFor=/var/lib/provisa-object-store" in mount
    assert "/etc/systemd/system/docker.service.d/" in mount
    assert "systemctl daemon-reload" in mount


def test_the_warehouse_and_the_catalogs_database_are_on_that_disk():
    assert "/var/lib/provisa-object-store/control-db:/var/lib/postgresql/data" in _local(
        "control_db_setup"
    )
    assert "export PROVISA_OBJECT_STORE_DIR=/var/lib/provisa-object-store" in _local(
        "object_store_mount"
    )


def test_the_disk_is_mounted_before_the_control_db_is_set_up():
    coordinator = _MAIN[_MAIN.index('resource "google_compute_instance" "coordinator"') :]
    assert coordinator.index("${local.object_store_mount}") < coordinator.index(
        "${local.control_db_setup}"
    )
