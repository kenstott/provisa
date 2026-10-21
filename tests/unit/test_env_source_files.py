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

from tests.helpers import ready_org_folder

from provisa.core.env_source_files import (
    FILE_TYPES,
    _local_path,
    discard_file_sources,
    env_files_dir,
)
from provisa.core.environments import EnvironmentNameError


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    ready_org_folder("acme")  # REQ-1992: as the org's runtime makes it ready
    return tmp_path


# --- where the files live -------------------------------------------------------


def test_each_environment_gets_its_own_directory_in_its_orgs_folder(data_dir):
    """REQ-1992: an environment's file copies are in its org's folder on the node."""
    expected = data_dir / "orgs" / "acme" / "env_files" / "feature_x"
    assert env_files_dir("acme", "feature_x") == expected


def test_an_org_whose_folder_is_not_ready_is_refused_by_name(data_dir):
    from provisa.core.org_files import OrgFolderUnavailable

    with pytest.raises(OrgFolderUnavailable, match="globex"):
        env_files_dir("globex", "feature_x")


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


def test_an_uploaded_or_downloaded_file_is_its_place_in_the_orgs_folder(data_dir):
    """REQ-1997, REQ-2113: a source given its file by upload or download keeps the path
    ``{files}/<source>/<name>``; a fork copies the file it names, not a path that is nowhere."""
    from provisa.core.org_files import org_root

    found = _local_path("{files}/supplies/supplies.parquet", "acme")
    assert found == org_root("acme") / "files" / "supplies" / "supplies.parquet"


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
