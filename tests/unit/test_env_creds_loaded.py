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

import ast
import os
from pathlib import Path

import pytest

from tests import env_creds
from tests.env_creds import _parse_env_file, env_file, load_provider_creds

_TESTS = Path(__file__).resolve().parents[1]


@pytest.fixture
def clean_env(monkeypatch):
    """No provider credential, and no named .env, in the process for the test."""
    for key in list(os.environ):
        if key.startswith(env_creds._CRED_PREFIXES) or key.startswith("SHAREPOINT_"):
            monkeypatch.delenv(key)
    monkeypatch.delenv("PROVISA_ENV_FILE", raising=False)
    monkeypatch.delenv("PROVISA_GSHEETS_LIVE", raising=False)


def test_the_test_session_loads_the_credentials_at_import():
    """tests/conftest.py calls load_provider_creds() at module level, before any module-level
    skipif reads os.environ."""
    tree = ast.parse((_TESTS / "conftest.py").read_text())
    calls = [
        node.value.func.id
        for node in tree.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
    ]
    assert "load_provider_creds" in calls


def test_every_provider_cred_in_the_file_is_exported(tmp_path, clean_env):
    dotenv = tmp_path / ".env"
    dotenv.write_text("SNOWFLAKE_ACCOUNT=acct\nexport DATABRICKS_TOKEN='tok'\n# GOOGLE_X=no\n")
    loaded = load_provider_creds(dotenv)
    assert sorted(loaded) == ["DATABRICKS_TOKEN", "SNOWFLAKE_ACCOUNT"]
    assert (os.environ["SNOWFLAKE_ACCOUNT"], os.environ["DATABRICKS_TOKEN"]) == ("acct", "tok")


def test_an_exported_value_wins_over_the_file(tmp_path, clean_env, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text("SNOWFLAKE_ACCOUNT=from-file\n")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "exported")
    assert load_provider_creds(dotenv) == []
    assert os.environ["SNOWFLAKE_ACCOUNT"] == "exported"


def test_an_exported_empty_value_wins_over_the_file_too(tmp_path, clean_env, monkeypatch):
    """Exporting a variable empty is how a run says "this credential has no value" (a
    password-less certificate); the file's value must not fill it back in."""
    dotenv = tmp_path / ".env"
    dotenv.write_text("SP_CERT_PASSWORD=from-file\n")
    monkeypatch.setenv("SP_CERT_PASSWORD", "")
    assert load_provider_creds(dotenv) == []
    assert os.environ["SP_CERT_PASSWORD"] == ""


def test_local_stack_vars_are_never_loaded_from_env_file(tmp_path, clean_env, monkeypatch):
    """The whitelist must not pull in anything that repoints the isolated Docker stack."""
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "PG_HOST=dev-db\nPOSTGRES_HOST=dev-db\nTRINO_HOST=dev\nREDIS_URL=redis://dev\n"
        "MINIO_ENDPOINT=dev\nKAFKA_BOOTSTRAP=dev\nPROVISA_ENGINE=trino\nSNOWFLAKE_USER=u\n"
    )
    forbidden = ("PG_", "POSTGRES_", "TRINO_", "REDIS_", "MINIO_", "KAFKA_", "PROVISA_ENGINE")
    for key in (
        "PG_HOST",
        "POSTGRES_HOST",
        "TRINO_HOST",
        "REDIS_URL",
        "MINIO_ENDPOINT",
        "KAFKA_BOOTSTRAP",
        "PROVISA_ENGINE",
    ):
        monkeypatch.delenv(key, raising=False)
    assert [k for k in _parse_env_file(dotenv) if k.startswith(forbidden)] == []
    assert load_provider_creds(dotenv) == ["SNOWFLAKE_USER"]
    assert not [k for k in os.environ if k.startswith(forbidden) and os.environ[k] == "dev-db"]


def test_sharepoint_cert_path_resolves_beside_the_env_file(tmp_path, clean_env):
    """SP_CERT_PATH is authored relative to the .env; the bridge resolves it there, so it is
    found from any cwd and from a worktree reading the primary checkout's .env."""
    dotenv = tmp_path / "primary" / ".env"
    dotenv.parent.mkdir()
    dotenv.write_text("SP_CERT_PATH=./sharepoint.pfx\nSP_CLIENT_ID=cid\n")
    load_provider_creds(dotenv)
    assert os.environ["SHAREPOINT_CERT_PATH"] == str(tmp_path / "primary" / "sharepoint.pfx")
    assert os.environ["SHAREPOINT_CLIENT_ID"] == "cid"


def test_a_worktree_reads_the_primary_checkouts_env_file(tmp_path, clean_env):
    primary = tmp_path / "primary"
    (primary / ".git" / "worktrees" / "feature").mkdir(parents=True)
    (primary / ".env").write_text("SNOWFLAKE_ACCOUNT=acct\n")
    worktree = tmp_path / "primary" / ".claude" / "worktrees" / "feature"
    worktree.mkdir(parents=True)
    (worktree / ".git").write_text(f"gitdir: {primary / '.git' / 'worktrees' / 'feature'}\n")
    assert env_file(worktree) == primary / ".env"


def test_a_checkouts_own_env_file_and_a_named_one_come_first(tmp_path, clean_env, monkeypatch):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    assert env_file(checkout) is None  # no .env anywhere: nothing to read, nothing loaded
    (checkout / ".env").write_text("")
    assert env_file(checkout) == checkout / ".env"
    named = tmp_path / "named.env"
    monkeypatch.setenv("PROVISA_ENV_FILE", str(named))
    assert env_file(checkout) == named
