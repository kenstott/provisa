# Copyright (c) 2026 Kenneth Stott
# Canary: cd6629c3-7632-45fc-906b-6b304eb8e0dc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An expiring environment's own copies of its file-backed sources (REQ-1620).

Filesystem only — the fork's database half is exercised where a real org schema exists. What is
asserted here is the part that decides which files move and where they land, and that discarding
the environment is discarding a directory.
"""

from __future__ import annotations

import pytest

from provisa.core.env_source_files import (
    FILE_TYPES,
    _local_path,
    discard_file_sources,
    env_files_dir,
    env_files_root,
)
from provisa.core.environments import EnvironmentNameError


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    return tmp_path


# --- where the files live -------------------------------------------------------


def test_root_follows_the_deployments_data_dir(data_dir):
    assert env_files_root() == data_dir / "env_files"


def test_each_environment_gets_its_own_directory(data_dir):
    assert env_files_dir("acme", "feature_x") == data_dir / "env_files" / "acme" / "feature_x"


def test_two_environments_of_one_org_do_not_share(data_dir):
    assert env_files_dir("acme", "feature_x") != env_files_dir("acme", "feature_y")


def test_a_name_that_is_not_an_environment_never_becomes_a_path(data_dir):
    # This function's result is a directory something else will later delete, so a separator
    # smuggled through the name is refused here rather than trusted from the caller.
    with pytest.raises(EnvironmentNameError):
        env_files_dir("acme", "../../etc")


def test_a_name_that_is_not_an_org_never_becomes_a_path(data_dir):
    with pytest.raises(Exception):
        env_files_dir("../acme", "feature_x")


# --- which sources are forkable -------------------------------------------------


def test_the_file_backed_types_are_the_ones_this_deployment_holds():
    assert FILE_TYPES == frozenset({"csv", "parquet", "sqlite", "files"})


def test_a_bare_path_is_local():
    assert str(_local_path("/data/orders.csv")) == "/data/orders.csv"


def test_a_file_url_is_local():
    assert str(_local_path("file:///data/orders.csv")) == "/data/orders.csv"


def test_a_windows_drive_letter_is_not_read_as_a_scheme():
    assert _local_path("C:/data/orders.csv") is not None


@pytest.mark.parametrize("raw", ["s3://bucket/orders.parquet", "https://h/orders.csv"])
def test_a_remote_store_is_not_forkable(raw):
    # Nothing to copy and nothing to clean up: the deployment does not own those bytes.
    assert _local_path(raw) is None


# --- retiring is removing a directory -------------------------------------------


def test_discard_removes_the_environments_files(data_dir):
    directory = env_files_dir("acme", "feature_x")
    directory.mkdir(parents=True)
    (directory / "orders.sqlite").write_bytes(b"rows")
    assert discard_file_sources("acme", "feature_x") is True
    assert not directory.exists()


def test_discard_leaves_the_other_environments_files(data_dir):
    kept = env_files_dir("acme", "feature_y")
    kept.mkdir(parents=True)
    (kept / "orders.sqlite").write_bytes(b"rows")
    env_files_dir("acme", "feature_x").mkdir(parents=True)
    discard_file_sources("acme", "feature_x")
    assert (kept / "orders.sqlite").exists()


def test_discard_reports_when_there_was_nothing_to_remove(data_dir):
    assert discard_file_sources("acme", "never_created") is False
