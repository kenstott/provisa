# Copyright (c) 2026 Kenneth Stott
# Canary: 9c14a7e3-58bd-4f0a-9a26-72e0b1c4d8f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1339: no catalog is committed, and none is written to disk.

The engine keeps the catalogs Provisa creates in memory (catalog.store=memory), so Trino writes no
``<name>.properties`` anywhere and nothing mounts a catalog directory. The checks on what is
tracked under /trino/catalog stay, for a checkout that still holds files the earlier file store
wrote.

Every file in that directory is produced by Trino's FileCatalogStore when Provisa issues
CREATE CATALOG under catalog.management=dynamic. Their connection-url is derived from the
deployment's own env (TrinoPgBackedConnector.details reads PG_HOST), so a copy captured on one
machine is wrong on every other one.

catalog.create_catalog used to return early when the catalog already existed, which made such a
file WIN over the correct registration rather than be corrected by it. That is how
cloud.provisa.dev ended up with inquiries_sqlite / petstore_api / pet_store_sqlite pointing at the
dev compose network's `postgres:5432` while the landed SQLite and OpenAPI demo data lived in the
deployment's Cloud SQL instance: the catalogs resolved, held no `default` schema, and every demo
query died with FederationError SCHEMA_NOT_FOUND "Schema 'default' does not exist". REQ-1354 made
registration drop-and-recreate, so a dynamic file no longer wins — but a file Trino loads
statically still cannot be dropped, and now fails registration loudly instead of shadowing it.

Guards the repo, not a runtime path — the artifacts were swept in by blanket "commit pending
changes" chores twice before, so only a check on what is TRACKED keeps them out.
"""

# Requirements: REQ-1339, REQ-2073

from __future__ import annotations

import subprocess
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_CATALOG_DIR = "trino/catalog"


def _tracked(pathspec: str) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "--", pathspec],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [line for line in out.splitlines() if line]


def test_no_catalog_properties_file_is_tracked():
    tracked = _tracked(f"{_CATALOG_DIR}/*.properties")
    assert tracked == [], (
        "Trino writes these; committing one ships a connection-url that names the authoring "
        f"machine's data plane and shadows dynamic registration: {tracked}"
    )


# Every Trino config Provisa starts an engine on: the compose stack's, the rendered one, the
# isolated and Kubernetes coordinators', the perf demo's, and the chart's (its own test).
_ENGINE_CONFIGS = (
    "trino/etc/config.properties",
    "trino/etc/worker/config.properties",
    "demo/named/perf/trino/config.properties",
)


def _engine_configs() -> dict[str, str]:
    from provisa.api import trino_setup
    from provisa.federation import isolated_provisioner, k8s_provisioner

    configs = {path: (_REPO / path).read_text() for path in _ENGINE_CONFIGS}
    configs["trino_setup coordinator"] = trino_setup._COORDINATOR_TEMPLATE
    configs["trino_setup worker"] = trino_setup._WORKER_TEMPLATE
    configs["isolated coordinator"] = isolated_provisioner._CONFIG_PROPERTIES
    configs["k8s coordinator"] = k8s_provisioner._CONFIG_PROPERTIES
    return configs


def test_every_engine_keeps_its_catalogs_in_memory():
    # A catalog carries its source's password; Trino's default store writes it to
    # catalog.config-dir. In memory, none reaches the disk (Provisa reissues them on a restart).
    for name, config in _engine_configs().items():
        assert "catalog.management=dynamic" in config, name
        assert "catalog.store=memory" in config, name


def test_nothing_mounts_a_catalog_directory():
    import re

    import yaml

    catalog_dir = re.compile(r"/etc/trino/catalog(/|:|$)")
    for compose in sorted(_REPO.glob("docker-compose*.yml")):
        for service, spec in (yaml.safe_load(compose.read_text()).get("services") or {}).items():
            for volume in spec.get("volumes") or []:
                assert not catalog_dir.search(str(volume)), (compose.name, service, volume)


def test_gitignore_covers_the_whole_directory():
    # Narrower globs (e2e_*, test_*, ui_*) let the demo catalogs through — the ignore has to be
    # the whole extension, since every name in here is chosen by a source id we do not control.
    ignored = subprocess.run(
        ["git", "check-ignore", f"{_CATALOG_DIR}/anything_at_all.properties"],
        cwd=_REPO,
        capture_output=True,
        text=True,
    )
    assert ignored.returncode == 0, ".gitignore must cover trino/catalog/*.properties"
