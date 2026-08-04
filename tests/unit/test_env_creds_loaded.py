# Copyright (c) 2026 Kenneth Stott
# Canary: 57a1539c-5131-4889-b5a2-1b6ba914d9a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Guard: credentials that exist in .env must be live in the pytest process.

Root cause this locks down: the cloud-DW e2es gate on os.environ in module-level skipif
conditions evaluated at collection. .env loading used to live only in scripts/test-all's
warehouse lane, so every other invocation (bare `pytest tests/integration`, IDE run,
single-file rerun) collected them credential-less. The run then reported a clean green
suite while 15 cloud tests had silently not executed against credentials sitting on disk.

If this test fails, a credential is present in .env but absent from the process -- the
tests gated on it are phantom-skipping, not passing.
"""

import os
from pathlib import Path

import pytest

from tests.env_creds import _ENV_FILE, _parse_env_file, load_provider_creds


def test_every_env_provider_cred_is_exported():
    if not _ENV_FILE.is_file():
        pytest.skip(".env absent (CI); nothing on disk to phantom-skip against")

    on_disk = _parse_env_file(_ENV_FILE)
    if not on_disk:
        pytest.skip(".env carries no external-provider credentials")

    missing = [k for k, v in on_disk.items() if v and not os.environ.get(k)]
    assert not missing, (
        f"credentials present in .env but not loaded into the pytest process: {missing}. "
        "Tests gated on these are skipping while the creds exist -- "
        "tests/conftest.py must call tests.env_creds.load_provider_creds() at import."
    )


def test_local_stack_vars_are_never_loaded_from_env_file():
    """The whitelist must not pull in anything that repoints the isolated Docker stack."""
    if not _ENV_FILE.is_file():
        pytest.skip(".env absent (CI)")

    forbidden = ("PG_", "POSTGRES_", "TRINO_", "REDIS_", "MINIO_", "KAFKA_", "PROVISA_ENGINE")
    leaked = [k for k in _parse_env_file(_ENV_FILE) if k.startswith(forbidden)]
    assert not leaked, f"local-stack vars leaked through the cred whitelist: {leaked}"


def test_sharepoint_cert_path_is_absolute(monkeypatch):
    """SP_CERT_PATH is authored relative; the bridge must resolve it or the e2e skips by cwd."""
    monkeypatch.setenv("SP_CERT_PATH", "./sharepoint.pfx")
    monkeypatch.delenv("SHAREPOINT_CERT_PATH", raising=False)
    load_provider_creds()
    resolved = os.environ.get("SHAREPOINT_CERT_PATH")
    if resolved is None:
        pytest.skip(".env absent, so load_provider_creds() returns before the bridge")
    assert Path(resolved).is_absolute()
