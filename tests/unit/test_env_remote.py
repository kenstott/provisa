# Copyright (c) 2026 Kenneth Stott
# Canary: 8c5b2e10-71a4-4d6f-9b3c-0e4a17d5f682
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The configure-time probe of an org's mirror, and the creation it offers (REQ-1537)."""

import pytest

from provisa.core import env_remote


class TestLocalPath:
    def test_a_directory_holding_no_repository_is_reported_missing_and_creatable(self, tmp_path):
        """The airgapped mirror (REQ-294): no API, so creation is always within reach."""
        probe = env_remote.probe(str(tmp_path / "mirror.git"))

        assert probe.exists is False
        assert probe.kind == env_remote.PATH
        assert probe.creatable is True

    def test_creating_a_local_remote_makes_a_bare_repository_the_next_probe_finds(self, tmp_path):
        target = str(tmp_path / "mirror.git")

        after = env_remote.create(target)

        assert after.exists is True
        assert (tmp_path / "mirror.git" / "HEAD").exists()
        assert env_remote.probe(target).exists is True

    def test_a_working_clone_counts_as_a_repository(self, tmp_path):
        """A push can reach either layout, so refusing the non-bare one would report a mirror that
        works as one that does not."""
        (tmp_path / "clone" / ".git").mkdir(parents=True)
        (tmp_path / "clone" / ".git" / "HEAD").write_text("ref: refs/heads/main\n")

        assert env_remote.probe(str(tmp_path / "clone")).exists is True

    def test_creating_over_an_existing_repository_is_refused(self, tmp_path):
        target = str(tmp_path / "mirror.git")
        env_remote.create(target)

        with pytest.raises(env_remote.RemoteError, match="already holds"):
            env_remote.create(target)

    def test_a_file_url_is_the_same_local_path(self, tmp_path):
        env_remote.create(f"file://{tmp_path}/mirror.git")

        assert (tmp_path / "mirror.git" / "HEAD").exists()


class TestHostedProbe:
    def test_a_missing_github_repository_carrying_a_credential_is_offered(self, monkeypatch):
        monkeypatch.setenv("GIT_TOKEN", "ghp_secret")
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: False)

        probe = env_remote.probe("https://${env:GIT_TOKEN}@github.com/acme/model.git")

        assert probe.exists is False
        assert probe.kind == env_remote.GITHUB
        assert probe.creatable is True
        assert probe.target == "acme/model"

    def test_a_reachable_repository_is_never_offered_for_creation(self, monkeypatch):
        monkeypatch.setenv("GIT_TOKEN", "ghp_secret")
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: True)

        probe = env_remote.probe("https://${env:GIT_TOKEN}@github.com/acme/model.git")

        assert probe.exists is True
        assert probe.creatable is False

    def test_a_host_whose_api_is_not_spoken_is_reported_but_not_offered(self, monkeypatch):
        """Guessing that a self-hosted domain speaks the GitLab API would send the org's token to
        an endpoint chosen by pattern-matching a hostname."""
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: False)

        probe = env_remote.probe("https://user:tok@git.acme.internal/acme/model.git")

        assert probe.exists is False
        assert probe.kind == env_remote.OTHER
        assert probe.creatable is False

    def test_a_credential_free_url_cannot_be_offered(self, monkeypatch):
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: False)

        probe = env_remote.probe("https://github.com/acme/model.git")

        assert probe.creatable is False

    def test_no_resolved_credential_reaches_the_caller(self, monkeypatch):
        """The stored value is a reference; the resolved token is used and never reported
        (REQ-125, REQ-1525)."""
        monkeypatch.setenv("GIT_TOKEN", "ghp_secret")
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: False)

        probe = env_remote.probe("https://${env:GIT_TOKEN}@github.com/acme/model.git")

        assert "ghp_secret" not in str(probe.as_dict())
        assert "github.com/acme/model.git" in probe.detail

    def test_a_url_naming_no_repository_is_an_error_rather_than_a_probe(self):
        with pytest.raises(env_remote.RemoteError, match="names no repository"):
            env_remote.probe("https://github.com/acme")

    def test_an_unreachable_remote_is_missing_rather_than_an_exception(self, monkeypatch):
        """Absent repository, wrong credential and dead host all mean the mirror is not working,
        which is what the operator is being shown."""

        def _boom(_url):
            raise OSError("connection refused")

        monkeypatch.setattr(
            "dulwich.porcelain.ls_remote", lambda url, **kw: _boom(url), raising=True
        )

        assert env_remote._refs_reachable("https://github.com/acme/model.git") is False


class TestHostedCreate:
    def test_an_unknown_host_is_refused_with_the_repository_named(self, monkeypatch):
        with pytest.raises(env_remote.RemoteError, match="git.acme.internal is neither"):
            env_remote.create("https://user:tok@git.acme.internal/acme/model.git")

    def test_a_credential_free_url_is_refused(self):
        with pytest.raises(env_remote.RemoteError, match="carries no credential"):
            env_remote.create("https://github.com/acme/model.git")

    def test_an_org_owned_repository_uses_the_organization_endpoint(self, monkeypatch):
        """``/user/repos`` would create ``thetokenowner/model`` instead — the right name at the
        wrong address."""
        calls = _fake_github(monkeypatch, login="somebody-else", status=201)
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: True)

        env_remote.create("https://user:tok@github.com/acme/model.git")

        assert calls["post_url"] == "https://api.github.com/orgs/acme/repos"
        assert calls["json"] == {"name": "model", "private": True}

    def test_a_repository_in_the_tokens_own_account_uses_the_user_endpoint(self, monkeypatch):
        calls = _fake_github(monkeypatch, login="acme", status=201)
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: True)

        env_remote.create("https://user:tok@github.com/acme/model.git")

        assert calls["post_url"] == "https://api.github.com/user/repos"

    def test_a_refused_creation_raises_rather_than_reporting_success(self, monkeypatch):
        _fake_github(monkeypatch, login="acme", status=422)

        with pytest.raises(env_remote.RemoteError, match="422"):
            env_remote.create("https://user:tok@github.com/acme/model.git")

    def test_a_rejected_credential_is_named_as_such(self, monkeypatch):
        _fake_github(monkeypatch, login="acme", status=201, me_status=401)

        with pytest.raises(env_remote.RemoteError, match="rejected the remote's credential"):
            env_remote.create("https://user:tok@github.com/acme/model.git")

    def test_gitlab_creates_into_the_namespace_the_url_names(self, monkeypatch):
        calls = _fake_gitlab(monkeypatch, namespace_status=200, status=201)
        monkeypatch.setattr(env_remote, "_refs_reachable", lambda url: True)

        env_remote.create("https://oauth2:tok@gitlab.com/acme/model.git")

        assert calls["json"]["namespace_id"] == 77
        assert calls["json"]["path"] == "model"
        assert calls["json"]["visibility"] == "private"

    def test_an_unresolvable_gitlab_namespace_is_refused(self, monkeypatch):
        """Falling back to the token's personal namespace would put the org's mirror somewhere
        nobody agreed to."""
        _fake_gitlab(monkeypatch, namespace_status=404, status=201)

        with pytest.raises(env_remote.RemoteError, match="could not resolve the namespace"):
            env_remote.create("https://oauth2:tok@gitlab.com/acme/model.git")


class _Resp:
    def __init__(self, status: int, payload: dict):
        self.status_code = status
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


def _client(monkeypatch, get, post):
    """Replace ``httpx.Client`` with one whose GET and POST are the callables given."""
    import httpx

    class _C:
        def __init__(self, **_kw):
            # Bound per instance: a class body cannot see the enclosing function's names.
            self.get = get
            self.post = post

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(httpx, "Client", _C)


def _fake_github(monkeypatch, *, login: str, status: int, me_status: int = 200) -> dict:
    calls: dict = {}

    def _get(url, headers=None, **_kw):
        calls["get_url"] = url
        return _Resp(me_status, {"login": login})

    def _post(url, headers=None, json=None, **_kw):
        calls["post_url"] = url
        calls["json"] = json
        return _Resp(status, {"full_name": "acme/model"})

    _client(monkeypatch, _get, _post)
    return calls


def _fake_gitlab(monkeypatch, *, namespace_status: int, status: int) -> dict:
    calls: dict = {}

    def _get(url, headers=None, **_kw):
        calls["get_url"] = url
        return _Resp(namespace_status, {"id": 77})

    def _post(url, headers=None, json=None, **_kw):
        calls["post_url"] = url
        calls["json"] = json
        return _Resp(status, {"id": 1})

    _client(monkeypatch, _get, _post)
    return calls
