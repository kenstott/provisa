# Copyright (c) 2026 Kenneth Stott
# Canary: 22761483-a357-4a44-9d36-b0a22d806881
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Kafka catalog's client properties, which can hold a SASL password, are for Trino alone.

Trino's Kafka connector reads its client settings only from a file. The file is 0600 in a 0700
directory, given to Trino's user where the app runs as root, and in the containerized stack it is
on a tmpfs the app and the engine share. What earlier releases left on disk (the file catalog
store's directory, the Kafka client files) is removed when the stack starts or cloud-dev is
deployed.
"""

# Requirements: REQ-147

from __future__ import annotations

import os
import stat
from pathlib import Path

import yaml

from provisa.core import trino_catalog_files
from provisa.core.trino_catalog_files import TRINO_UID, write_kafka_client_file

_REPO = Path(__file__).resolve().parents[2]
_SASL = 'sasl.jaas.config=... password="s3cret";\n'


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_the_file_is_readable_by_its_owner_alone(tmp_path):
    path = tmp_path / "kafka-client" / "org_acme__events.properties"
    write_kafka_client_file(path, _SASL)
    assert path.read_text() == _SASL
    assert _mode(path) == 0o600
    assert _mode(path.parent) == 0o700


def test_a_file_an_earlier_write_left_open_is_narrowed(tmp_path):
    path = tmp_path / "kafka-client" / "org_acme__events.properties"
    path.parent.mkdir(mode=0o755)
    path.write_text("old")
    path.chmod(0o644)
    write_kafka_client_file(path, _SASL)
    assert path.read_text() == _SASL
    assert _mode(path) == 0o600
    assert _mode(path.parent) == 0o700


def test_as_root_the_file_is_given_to_trinos_user(tmp_path, monkeypatch):
    given: list[tuple[str, int, int]] = []
    monkeypatch.setattr(trino_catalog_files.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        trino_catalog_files.os, "fchown", lambda fd, uid, gid: given.append(("file", uid, gid))
    )
    monkeypatch.setattr(
        trino_catalog_files.os, "chown", lambda p, uid, gid: given.append(("dir", uid, gid))
    )
    write_kafka_client_file(tmp_path / "kafka-client" / "c.properties", _SASL)
    assert given == [("file", TRINO_UID, TRINO_UID), ("dir", TRINO_UID, TRINO_UID)]


def test_not_as_root_ownership_is_left_with_the_writer(tmp_path, monkeypatch):
    def _refuse(*a):
        raise AssertionError("only root gives a file away")

    monkeypatch.setattr(trino_catalog_files.os, "geteuid", lambda: 501)
    monkeypatch.setattr(trino_catalog_files.os, "fchown", _refuse)
    monkeypatch.setattr(trino_catalog_files.os, "chown", _refuse)
    path = tmp_path / "kafka-client" / "c.properties"
    write_kafka_client_file(path, _SASL)
    assert path.stat().st_uid == os.geteuid()


def _compose(name: str) -> dict:
    return yaml.safe_load((_REPO / name).read_text())


def test_the_app_and_the_engine_share_the_client_files_on_a_tmpfs():
    app = _compose("docker-compose.app.yml")
    volume = app["volumes"]["trino_kafka_client"]
    assert volume["driver_opts"]["type"] == "tmpfs"
    assert f"uid={TRINO_UID}" in volume["driver_opts"]["o"]
    assert "mode=0700" in volume["driver_opts"]["o"]
    assert (
        "trino_kafka_client:/app/trino-host/kafka-client" in app["services"]["provisa"]["volumes"]
    )
    for engine in ("trino", "trino-worker"):
        assert "trino_kafka_client:/etc/trino/kafka-client:ro" in app["services"][engine]["volumes"]


def test_the_stack_start_removes_what_earlier_releases_left_on_disk():
    init = _compose("docker-compose.core.yml")["services"]["trino-exchange-init"]
    assert "rm -rf /engine-tree/catalog $${PROVISA_DEAD_ENGINE_FILES}" in init["command"]
    assert any(v.endswith(":/engine-tree") for v in init["volumes"])
    app_init = _compose("docker-compose.app.yml")["services"]["trino-exchange-init"]
    assert app_init["environment"]["PROVISA_DEAD_ENGINE_FILES"] == (
        "/engine-tree/kafka-client/*.properties"
    )


def test_every_cloud_deploy_removes_them_from_the_node():
    script = (_REPO / "scripts" / "deploy-cloud.sh").read_text()
    for target in ("cfg)", "all)", "patch)"):
        line = script.split(f"\n  {target}", 1)[1].split(";;", 1)[0]
        assert "clear_dead_engine_files" in line, target
