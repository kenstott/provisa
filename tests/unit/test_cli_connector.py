# Copyright (c) 2026 Kenneth Stott
# Canary: b7d2f4a1-6c38-4e19-a5d7-1f0c9e3b8a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""`provisa connector build|check|rebuild` (REQ-2221) and the config's `connectors:` block.

git is real, against a repository in a temporary directory. The image build (REQ-2098, tested on its
own) is stubbed where these pin what it is given and what comes of its answer.
"""

# Requirements: REQ-2221

from __future__ import annotations

import base64
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from provisa import cli, cli_connector
from provisa.codesource.manifest import Manifest, Requirement, manifest_json
from provisa.core.models import ConnectorConfig, ConnectorRequirement

pytestmark = pytest.mark.unit

_DIGEST = "sha256:" + "a" * 64
_NEW_DIGEST = "sha256:" + "b" * 64
_CODE_HASH = "d" * 64
_NEW_CODE_HASH = "e" * 64
_IMAGE = "registry.example/data/crm"


def _git(repo: Path, *args: str) -> str:
    identity = ["-c", "user.name=t", "-c", "user.email=t@example.com"]
    done = subprocess.run(
        ["git", *identity, *args], cwd=repo, check=True, capture_output=True, text=True
    )
    return done.stdout.strip()


def _commit(repo: Path, name: str, text: str = "") -> str:
    target = repo / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", f"add {name}")
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "origin"
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    _commit(root, "pyproject.toml", "[project]\nname = 'crm'\nversion = '1'\n")
    return root


def _entry(repo: Path, commit: str, **overrides: Any) -> dict:
    return {
        "name": "crm",
        "git": str(repo),
        "ref": "main",
        "commit": commit,
        "language": "python",
        "image": _IMAGE,
        "digest": _DIGEST,
        "code_hash": _CODE_HASH,
        "arch": "amd64",
        **overrides,
    }


def _config(tmp_path: Path, *entries: dict, before: str = "", after: str = "") -> Path:
    path = tmp_path / "model.yaml"
    path.write_text(before + yaml.safe_dump({"connectors": list(entries)}, sort_keys=False) + after)
    return path


def _build_args(repo: Path, *extra: str) -> list[str]:
    return [
        "connector", "build", str(repo), "--name", "crm", "--ref", "main",
        "--image", _IMAGE, "--arch", "amd64", *extra,
    ]  # fmt: skip


@pytest.fixture
def built(monkeypatch) -> list[dict]:
    """The image build stubbed: what each call was given, and a digest and requires as its answer."""
    calls: list[dict] = []

    def fake(entry, root, language, workdir, registry, package_registries, standard=None):
        calls.append(
            {
                "entry": entry,
                "files": sorted(p.name for p in root.iterdir()),
                "language": language,
                "registry": registry,
                "package_registries": package_registries,
            }
        )
        requires = [
            ConnectorRequirement(name="db_uri", secret=True),
            ConnectorRequirement(name="region"),
            ConnectorRequirement(name="script"),
        ]
        if standard is None:
            return _NEW_DIGEST, requires[:1], _NEW_CODE_HASH
        return _NEW_DIGEST, cli_connector._with_file_values(requires, standard), _NEW_CODE_HASH

    monkeypatch.setattr(cli_connector, "_build_and_push", fake)
    return calls


class TestBuild:
    def test_prints_a_block_pinned_to_the_ref_head(self, repo, built, capsys):
        head = _git(repo, "rev-parse", "HEAD")
        assert cli.main(_build_args(repo)) == 0
        block = yaml.safe_load(capsys.readouterr().out)
        assert block["connectors"] == [
            {
                "name": "crm",
                "git": str(repo),
                "ref": "main",
                "commit": head,
                "language": "python",
                "image": _IMAGE,
                "digest": _NEW_DIGEST,
                "code_hash": _NEW_CODE_HASH,
                "arch": "amd64",
                "requires": [{"name": "db_uri", "secret": True}],
            }
        ]
        assert built[0]["language"] == "python"

    def test_builds_the_commit_it_is_told(self, repo, built, capsys):
        first = _git(repo, "rev-parse", "HEAD")
        _commit(repo, "later.txt")
        assert cli.main(_build_args(repo, "--commit", first)) == 0
        assert yaml.safe_load(capsys.readouterr().out)["connectors"][0]["commit"] == first
        assert "later.txt" not in built[0]["files"]

    @pytest.mark.parametrize(
        ("standard", "language"),
        [("go.mod", "go"), ("package.json", "node"), ("requirements.txt", "python")],
    )
    def test_detects_the_language_from_standard_files(
        self, tmp_path, built, capsys, standard, language
    ):
        root = tmp_path / "project"
        root.mkdir()
        _git(root, "init", "-q", "-b", "main")
        _commit(root, standard)
        assert cli.main(_build_args(root)) == 0
        assert built[0]["language"] == language

    def test_a_project_off_the_pattern_is_refused_saying_what_is_missing(
        self, tmp_path, built, capsys
    ):
        root = tmp_path / "project"
        root.mkdir()
        _git(root, "init", "-q", "-b", "main")
        _commit(root, "README.md")
        assert cli.main(_build_args(root)) == 1
        err = capsys.readouterr().err
        assert "is not a standard project" in err
        for name in ("go.mod", "package.json", "pyproject.toml", "requirements.txt"):
            assert name in err
        assert built == []

    def test_two_languages_at_one_root_are_refused(self, repo, built, capsys):
        _commit(repo, "go.mod")
        assert cli.main(_build_args(repo)) == 1
        assert "--path" in capsys.readouterr().err
        assert built == []

    def test_path_names_a_project_in_a_monorepo(self, repo, built, capsys):
        _commit(repo, "connectors/crm/go.mod")
        assert cli.main(_build_args(repo, "--path", "connectors/crm")) == 0
        assert built[0]["language"] == "go"
        assert yaml.safe_load(capsys.readouterr().out)["connectors"][0]["path"] == "connectors/crm"

    def test_a_missing_path_is_refused(self, repo, built, capsys):
        assert cli.main(_build_args(repo, "--path", "nowhere")) == 1
        assert "no directory nowhere" in capsys.readouterr().err

    def test_build_command_produces_the_project(self, repo, built, capsys):
        script = "import pathlib; pathlib.Path('out').mkdir(); pathlib.Path('out/go.mod').touch()"
        _commit(repo, "gen.py", script)
        command = f"{sys.executable} gen.py"
        assert cli.main(_build_args(repo, "--path", "out", "--build-command", command)) == 0
        assert built[0]["language"] == "go"

    def test_a_failing_build_command_is_refused_with_its_status(self, repo, built, capsys):
        command = f"{sys.executable} -c 'raise SystemExit(7)'"
        assert cli.main(_build_args(repo, "--build-command", command)) == 1
        assert "status 7" in capsys.readouterr().err
        assert built == []

    def test_an_unknown_ref_is_refused(self, repo, built, capsys):
        args = _build_args(repo)
        args[args.index("main")] = "nosuch"
        assert cli.main(args) == 1
        assert "no branch or tag nosuch" in capsys.readouterr().err

    def test_a_ref_both_branch_and_tag_is_refused(self, repo, built, capsys):
        _git(repo, "tag", "main")
        assert cli.main(_build_args(repo)) == 1
        assert "both a branch and a tag" in capsys.readouterr().err

    def test_a_credential_in_the_address_is_usage_and_never_printed(self, built, capsys):
        args = [
            "connector", "build", "https://user:hunter2@git.example/crm.git", "--name", "crm",
            "--ref", "main", "--image", _IMAGE, "--arch", "amd64",
        ]  # fmt: skip
        assert cli.main(args) == 2
        captured = capsys.readouterr()
        assert "credential" in captured.err
        assert "hunter2" not in captured.err + captured.out
        assert built == []

    def _registries(self, tmp_path: Path, *entries: dict) -> Path:
        path = tmp_path / "indexes.yaml"
        path.write_text(yaml.safe_dump({"registries": list(entries)}))
        return path

    def test_package_registries_name_indexes_and_read_credentials_from_the_environment(
        self, tmp_path, repo, built, capsys, monkeypatch
    ):
        monkeypatch.setenv("ACME_PYPI", "s3cret-token")
        file = self._registries(
            tmp_path,
            {
                "kind": "pypi",
                "address": "https://pypi.acme.dev/simple",
                "credential_env": "ACME_PYPI",
            },
        )
        assert cli.main(_build_args(repo, "--package-registries", str(file))) == 0
        (given,) = built[0]["package_registries"]
        assert (given.kind, given.address, given.credential) == (
            "pypi",
            "https://pypi.acme.dev/simple",
            "s3cret-token",
        )
        captured = capsys.readouterr()
        assert "s3cret-token" not in captured.out + captured.err

    def test_a_credential_written_in_the_file_is_refused_without_repeating_it(
        self, tmp_path, repo, built, capsys
    ):
        file = self._registries(
            tmp_path,
            {
                "kind": "pypi",
                "address": "https://pypi.acme.dev/simple",
                "credential": "hunter2",
                "credential_env": "ACME_PYPI",
            },
        )
        assert cli.main(_build_args(repo, "--package-registries", str(file))) == 2
        captured = capsys.readouterr()
        assert "credential" in captured.err
        assert "hunter2" not in captured.out + captured.err
        assert built == []

    def test_an_unset_credential_variable_is_usage_naming_the_variable(
        self, tmp_path, repo, built, capsys, monkeypatch
    ):
        monkeypatch.delenv("ACME_PYPI", raising=False)
        file = self._registries(
            tmp_path,
            {
                "kind": "npm",
                "address": "https://npm.acme.dev",
                "scope": "@acme",
                "credential_env": "ACME_PYPI",
            },
        )
        assert cli.main(_build_args(repo, "--package-registries", str(file))) == 2
        assert "ACME_PYPI" in capsys.readouterr().err
        assert built == []

    def test_an_unknown_index_kind_and_an_unreadable_file_are_usage(
        self, tmp_path, repo, built, capsys, monkeypatch
    ):
        monkeypatch.setenv("ACME_PYPI", "t")
        file = self._registries(
            tmp_path, {"kind": "maven", "address": "https://m", "credential_env": "ACME_PYPI"}
        )
        assert cli.main(_build_args(repo, "--package-registries", str(file))) == 2
        assert "kind" in capsys.readouterr().err
        assert cli.main(_build_args(repo, "--package-registries", str(tmp_path / "none"))) == 2
        assert built == []

    def test_rebuild_takes_the_same_flag(self, tmp_path, repo, built, monkeypatch):
        monkeypatch.setenv("ACME_PYPI", "t")
        file = self._registries(
            tmp_path,
            {"kind": "pypi", "address": "https://p/simple", "credential_env": "ACME_PYPI"},
        )
        head = _git(repo, "rev-parse", "HEAD")
        config = _config(tmp_path, _entry(repo, head))
        assert (
            cli.main(
                ["connector", "rebuild", "--config", str(config), "--package-registries", str(file)]
            )
            == 0
        )
        assert built[0]["package_registries"][0].address == "https://p/simple"

    def test_with_no_flag_there_are_no_named_indexes(self, repo, built, capsys):
        assert cli.main(_build_args(repo)) == 0
        assert built[0]["package_registries"] == []

    def test_registry_password_comes_from_the_named_variable(
        self, repo, built, capsys, monkeypatch
    ):
        monkeypatch.setenv("MY_CI_VAR", "s3cret")
        args = _build_args(repo, "--registry-user", "bot", "--registry-password-env", "MY_CI_VAR")
        assert cli.main(args) == 0
        assert built[0]["registry"] == {"username": "bot", "password": "s3cret"}
        captured = capsys.readouterr()
        assert "s3cret" not in captured.out + captured.err

    def test_an_unset_password_variable_is_usage(self, repo, built, capsys, monkeypatch):
        monkeypatch.delenv("MY_CI_VAR", raising=False)
        args = _build_args(repo, "--registry-user", "bot", "--registry-password-env", "MY_CI_VAR")
        assert cli.main(args) == 2
        assert "MY_CI_VAR" in capsys.readouterr().err
        assert built == []

    def test_a_registry_user_without_a_password_is_usage(self, repo, built):
        assert cli.main(_build_args(repo, "--registry-user", "bot")) == 2
        assert built == []

    def test_git_missing_is_refused_by_name(self, repo, built, capsys, monkeypatch):
        monkeypatch.setenv("PATH", "")
        assert cli.main(_build_args(repo)) == 1
        assert "git is not on the PATH" in capsys.readouterr().err


class TestBuildAndPush:
    """What the image build is given and what is taken from it."""

    def test_reuses_the_image_build_and_pushes_to_the_named_repository(self, tmp_path, monkeypatch):
        from provisa.codesource import image_build, languages, oci

        seen: dict = {}
        manifest = Manifest((), {}, (Requirement("db_uri", True), Requirement("region", False)))

        def prepare(source_id, config, code, lock, ctx):
            seen["prepared"] = (source_id, config.language, config.content_hash, code.path)
            return SimpleNamespace(content_hash="c" * 64, manifest=manifest)

        def assemble(plan, code, lock, arch, workdir):
            seen["plan"] = plan
            seen["arch"] = arch
            return SimpleNamespace(image="the-image", tag="the-tag")

        def push(registry, repository, tag, image):
            seen["push"] = (registry.host, registry.username, repository, tag, image)
            return _NEW_DIGEST

        language = SimpleNamespace(prepare=lambda: prepare, runtimes=("python",))
        monkeypatch.setitem(languages.LANGUAGES, "python", language)
        monkeypatch.setattr(image_build, "assemble", assemble)
        monkeypatch.setattr(oci.Registry, "push", push)

        entry = ConnectorConfig(**_entry(Path("/r"), "a" * 40))
        named = [object()]
        digest, requires, code_hash = cli_connector._build_and_push(
            entry,
            tmp_path,
            "python",
            tmp_path / "image",
            {"username": "bot", "password": "p"},
            named,
        )

        assert digest == _NEW_DIGEST
        assert code_hash == "c" * 64
        assert [(r.name, r.secret) for r in requires] == [("db_uri", True), ("region", False)]
        assert seen["prepared"] == ("crm", "python", None, str(tmp_path))
        assert seen["plan"].code_hash == "c" * 64
        # What the handshake is later checked by is the code's own manifest, as built.
        assert seen["plan"].manifest == manifest_json(manifest)
        assert seen["plan"].registries == named
        assert seen["arch"] == "amd64"
        assert seen["push"] == ("registry.example", "bot", "data/crm", "the-tag", "the-image")


class TestCheck:
    def test_all_current_exits_zero(self, tmp_path, repo, capsys):
        head = _git(repo, "rev-parse", "HEAD")
        config = _config(tmp_path, _entry(repo, head))
        assert cli.main(["connector", "check", "--config", str(config)]) == 0
        assert capsys.readouterr().out == f"crm\tcurrent\t{head}\n"

    def test_behind_its_ref_exits_one_naming_both_commits(self, tmp_path, repo, capsys):
        pinned = _git(repo, "rev-parse", "HEAD")
        head = _commit(repo, "later.txt")
        config = _config(tmp_path, _entry(repo, pinned))
        assert cli.main(["connector", "check", "--config", str(config)]) == 1
        assert capsys.readouterr().out == f"crm\tbehind\t{pinned}\t{head}\n"

    def test_follows_an_annotated_tag_to_its_commit(self, tmp_path, repo, capsys):
        head = _git(repo, "rev-parse", "HEAD")
        _git(repo, "tag", "-a", "v1", "-m", "one")
        config = _config(tmp_path, _entry(repo, head, ref="v1"))
        assert cli.main(["connector", "check", "--config", str(config)]) == 0

    def test_an_unreadable_ref_exits_one_and_the_others_are_still_read(
        self, tmp_path, repo, capsys
    ):
        head = _git(repo, "rev-parse", "HEAD")
        config = _config(
            tmp_path,
            _entry(repo, head, name="gone", ref="nosuch"),
            _entry(repo, head, name="crm"),
        )
        assert cli.main(["connector", "check", "--config", str(config)]) == 1
        captured = capsys.readouterr()
        assert captured.out.splitlines() == [f"gone\tunknown\t{head}", f"crm\tcurrent\t{head}"]
        assert "no branch or tag nosuch" in captured.err

    def test_reads_only_the_block_so_no_secret_is_resolved(self, tmp_path, repo, capsys):
        head = _git(repo, "rev-parse", "HEAD")
        config = _config(
            tmp_path, _entry(repo, head), before="auth:\n  key: ${secret:never-resolved}\n"
        )
        assert cli.main(["connector", "check", "--config", str(config)]) == 0

    def test_an_empty_block_has_nothing_to_report(self, tmp_path, capsys):
        config = tmp_path / "model.yaml"
        config.write_text("connectors: []\n")
        assert cli.main(["connector", "check", "--config", str(config)]) == 0
        assert capsys.readouterr().out == ""

    @pytest.mark.parametrize(
        ("text", "message"),
        [
            ("sources: []\n", "no connectors: block"),
            ("connectors: [\n", "not valid YAML"),
            ("connectors:\n- name: crm\n", "Field required"),
        ],
    )
    def test_an_unusable_config_is_usage(self, tmp_path, capsys, text, message):
        config = tmp_path / "model.yaml"
        config.write_text(text)
        assert cli.main(["connector", "check", "--config", str(config)]) == 2
        assert message in capsys.readouterr().err

    def test_a_missing_file_is_usage(self, tmp_path, capsys):
        assert cli.main(["connector", "check", "--config", str(tmp_path / "none.yaml")]) == 2
        assert "cannot read" in capsys.readouterr().err

    def test_repeated_names_are_usage(self, tmp_path, repo, capsys):
        head = _git(repo, "rev-parse", "HEAD")
        config = _config(tmp_path, _entry(repo, head), _entry(repo, head))
        assert cli.main(["connector", "check", "--config", str(config)]) == 2
        assert "more than once" in capsys.readouterr().err

    def test_a_refused_value_is_not_repeated_in_the_message(self, tmp_path, capsys):
        config = tmp_path / "model.yaml"
        entry = _entry(Path("/r"), "a" * 40, git="https://u:hunter2@git.example/x")
        config.write_text(yaml.safe_dump({"connectors": [entry]}))
        assert cli.main(["connector", "check", "--config", str(config)]) == 2
        assert "hunter2" not in capsys.readouterr().err


class TestRebuild:
    def test_asks_the_build_at_the_pinned_commit_even_when_current(
        self, tmp_path, repo, built, capsys
    ):
        pinned = _git(repo, "rev-parse", "HEAD")
        _commit(repo, "later.txt")
        config = _config(tmp_path, _entry(repo, pinned))
        assert cli.main(["connector", "rebuild", "--config", str(config)]) == 0
        out = yaml.safe_load(capsys.readouterr().out)["connectors"][0]
        assert (out["commit"], out["digest"]) == (pinned, _NEW_DIGEST)
        assert out["requires"] == [{"name": "db_uri", "secret": True}]
        assert "later.txt" not in built[0]["files"]

    def test_latest_builds_the_refs_head(self, tmp_path, repo, built, capsys):
        pinned = _git(repo, "rev-parse", "HEAD")
        head = _commit(repo, "later.txt")
        config = _config(tmp_path, _entry(repo, pinned))
        assert cli.main(["connector", "rebuild", "--config", str(config), "--latest"]) == 0
        assert yaml.safe_load(capsys.readouterr().out)["connectors"][0]["commit"] == head
        assert "later.txt" in built[0]["files"]

    def test_every_connector_is_asked_and_a_failure_keeps_its_entry(
        self, tmp_path, repo, built, capsys
    ):
        head = _git(repo, "rev-parse", "HEAD")
        missing = "1" * 40
        config = _config(
            tmp_path,
            _entry(repo, missing, name="broken"),
            _entry(repo, head, name="crm"),
        )
        assert cli.main(["connector", "rebuild", "--config", str(config)]) == 1
        captured = capsys.readouterr()
        broken, crm = yaml.safe_load(captured.out)["connectors"]
        assert (broken["commit"], broken["digest"]) == (missing, _DIGEST)
        assert crm["digest"] == _NEW_DIGEST
        assert "connector broken:" in captured.err

    def test_write_replaces_only_the_block(self, tmp_path, repo, built, capsys):
        head = _git(repo, "rev-parse", "HEAD")
        config = _config(
            tmp_path,
            _entry(repo, head),
            before="# the model\nsources: []\n\n",
            after="\n# the roles\nroles: []\n",
        )
        assert cli.main(["connector", "rebuild", "--config", str(config), "--write"]) == 0
        assert capsys.readouterr().out == ""
        text = config.read_text()
        assert text.startswith("# the model\nsources: []\n\n")
        assert text.endswith("\n# the roles\nroles: []\n")
        written = yaml.safe_load(text)["connectors"][0]
        assert written["digest"] == _NEW_DIGEST

    def test_write_that_would_not_read_back_leaves_the_file_and_prints_the_block(
        self, tmp_path, repo, built, capsys
    ):
        head = _git(repo, "rev-parse", "HEAD")
        entry = yaml.safe_dump([_entry(repo, head, name="one")], sort_keys=False)
        other = yaml.safe_dump([_entry(repo, head, name="two")], sort_keys=False)
        config = tmp_path / "model.yaml"
        original = f"connectors:\n{entry}# a comment between entries\n{other}"
        config.write_text(original)
        assert cli.main(["connector", "rebuild", "--config", str(config), "--write"]) == 1
        captured = capsys.readouterr()
        assert config.read_text() == original
        assert len(yaml.safe_load(captured.out)["connectors"]) == 2
        assert "printed instead" in captured.err

    def test_an_unusable_config_is_usage_and_builds_nothing(self, tmp_path, built):
        config = tmp_path / "model.yaml"
        config.write_text("sources: []\n")
        assert cli.main(["connector", "rebuild", "--config", str(config)]) == 2
        assert built == []


class TestConfigBlock:
    def _valid(self, **overrides: Any) -> dict:
        return _entry(Path("/r"), "a" * 40) | {"git": "https://git.example/crm.git"} | overrides

    def test_a_full_entry_is_read(self):
        entry = ConnectorConfig(
            **self._valid(
                path="connectors/crm",
                build_command="make dist",
                requires=[{"name": "db_uri", "secret": True}, {"name": "region"}],
            )
        )
        assert entry.requires[0].secret is True
        assert entry.requires[1].secret is False

    @pytest.mark.parametrize(
        "git",
        ["git@github.com:acme/crm.git", "ssh://git@host/crm.git", "https://git.example/crm.git"],
    )
    def test_addresses_without_a_credential_are_accepted(self, git):
        assert ConnectorConfig(**self._valid(git=git)).git == git

    @pytest.mark.parametrize(
        "bad",
        [
            {"git": "https://token@git.example/crm.git"},
            {"git": ""},
            {"commit": "abc123"},
            {"digest": "sha256:short"},
            {"digest": "a" * 64},
            {"image": "crm"},
            {"image": "data/crm"},
            {"image": "registry.example/data/crm:latest"},
            {"image": "registry.example/data/crm@sha256:abc"},
            {"arch": "riscv"},
            {"ref": ""},
            {"ref": "a" * 40},
            {"path": "/abs"},
            {"path": "../out"},
            {"path": "a/../../b"},
            {"name": "Has Spaces"},
            {"requires": [{"name": "not valid"}]},
            {"requires": [{"name": "a"}, {"name": "a", "secret": True}]},
            {"requires": [{"name": "a", "default": "x"}]},
            {"unknown_field": 1},
        ],
    )
    def test_refused(self, bad):
        with pytest.raises(ValidationError):
            ConnectorConfig(**self._valid(**bad))

    @pytest.mark.parametrize("image", ["localhost:5000/crm", "registry.example:443/a/b"])
    def test_a_registry_with_a_port_is_accepted(self, image):
        assert ConnectorConfig(**self._valid(image=image)).image == image

    def test_the_config_refuses_a_name_used_twice(self):
        from provisa.core.models import ProvisaConfig

        entry = self._valid()
        with pytest.raises(ValidationError, match="more than once"):
            ProvisaConfig(sources=[], domains=[], tables=[], roles=[], connectors=[entry, entry])

    def test_the_config_holds_the_block(self):
        from provisa.core.models import ProvisaConfig

        held = ProvisaConfig(
            sources=[], domains=[], tables=[], roles=[], connectors=[self._valid()]
        )
        assert held.connectors[0].name == "crm"
        assert ProvisaConfig(sources=[], domains=[], tables=[], roles=[]).connectors == []


_ICON = b"<svg xmlns='http://www.w3.org/2000/svg'/>"
_FILE = """
description: Our CRM, its accounts and contacts.
icon: icon.svg
requires:
  region: {default: eu-west-1}
  script: {multiline: true, fixed: "print(1)"}
settings:
  replicate: {default: 0}
  cache_ttl: {fixed: 300}
"""


def _with_file(repo: Path, text: str = _FILE, icon: bytes | None = _ICON) -> None:
    if icon is not None:
        (repo / "icon.svg").write_bytes(icon)
    _commit(repo, "connector.yaml", text)


class TestStandardFile:
    def test_its_values_are_read_into_the_block(self, repo, built, capsys):
        _with_file(repo)
        assert cli.main(_build_args(repo)) == 0
        block = yaml.safe_load(capsys.readouterr().out)["connectors"][0]
        assert block["description"] == "Our CRM, its accounts and contacts."
        assert block["icon"] == ("data:image/svg+xml;base64," + base64.b64encode(_ICON).decode())
        assert block["requires"] == [
            {"name": "db_uri", "secret": True},
            {"name": "region", "default_value": "eu-west-1"},
            {"name": "script", "multiline": True, "fixed_value": "print(1)"},
        ]
        assert block["settings"] == [
            {"name": "replicate", "default_value": 0},
            {"name": "cache_ttl", "fixed_value": 300},
        ]

    def test_the_block_it_prints_loads_as_a_connector(self, repo, built, capsys):
        _with_file(repo)
        assert cli.main(_build_args(repo)) == 0
        ConnectorConfig(**yaml.safe_load(capsys.readouterr().out)["connectors"][0])

    def test_a_project_with_no_file_builds_as_before(self, repo, built, capsys):
        assert cli.main(_build_args(repo)) == 0
        block = yaml.safe_load(capsys.readouterr().out)["connectors"][0]
        assert not {"description", "icon", "settings"} & set(block)
        assert block["requires"] == [{"name": "db_uri", "secret": True}]

    @pytest.mark.parametrize(
        ("text", "named"),
        [
            ("nonsense: 1\n", "nonsense"),
            ("tables: [a]\n", "tables"),
            ("requires: {nope: {default: a}}\n", "nope"),
            ("requires: {db_uri: {default: a}}\n", "db_uri"),
            ("requires: {db_uri: {multiline: true}}\n", "db_uri"),
            ("requires: {region: {default: a, fixed: b}}\n", "region"),
            ("requires: {region: {default: 5}}\n", "requires.region.default"),
            ("settings: {made_up: {default: 1}}\n", "made_up"),
            ("settings: {replicate: {default: -2}}\n", "replicate"),
            ("settings: {cache_enabled: {fixed: 1}}\n", "cache_enabled"),
            ("settings: {change_signal: {default: psychic}}\n", "change_signal"),
            ("settings: {off_peak_tz: {default: Mars/Base}}\n", "off_peak_tz"),
            ("settings: {cache_ttl: {default: 1, fixed: 2}}\n", "cache_ttl"),
            ("settings: {cache_ttl: {}}\n", "cache_ttl"),
            ("icon: missing.svg\n", "missing.svg"),
            ("icon: ../secret.svg\n", "secret.svg"),
            ("icon: notes.txt\n", "notes.txt"),
            ("- a\n- b\n", "connector.yaml"),
            ("a: [\n", "connector.yaml"),
        ],
    )
    def test_a_bad_file_is_refused_by_name(self, repo, built, capsys, text, named):
        _with_file(repo, text, None)
        assert cli.main(_build_args(repo)) == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "connector.yaml" in captured.err and named in captured.err

    def test_a_file_whose_requires_the_code_lacks_is_refused_before_a_build(
        self, tmp_path, monkeypatch
    ):
        from provisa.codesource import image_build

        assembled: list[object] = []
        monkeypatch.setattr(image_build, "assemble", lambda *a: assembled.append(a))
        standard = cli_connector._StandardFile(
            "", "", {"nope": cli_connector._RequireValues(False, "a", None)}, []
        )
        with pytest.raises(cli_connector._Refused, match="nope"):
            cli_connector._with_file_values([ConnectorRequirement(name="db_uri")], standard)
        assert assembled == []

    def test_an_icon_that_is_a_link_is_refused(self, repo, built, capsys, tmp_path):
        outside = tmp_path / "outside.svg"
        outside.write_bytes(_ICON)
        (repo / "icon.svg").symlink_to(outside)
        _commit(repo, "connector.yaml", "icon: icon.svg\n")
        assert cli.main(_build_args(repo)) == 1
        assert "symbolic link" in capsys.readouterr().err

    def test_an_icon_too_large_is_refused(self, repo, built, capsys):
        _with_file(repo, "icon: icon.svg\n", b"x" * 200_000)
        assert cli.main(_build_args(repo)) == 1
        assert "icon.svg" in capsys.readouterr().err

    def test_a_rebuild_after_the_file_is_removed_leaves_none_of_it(
        self, repo, built, capsys, tmp_path
    ):
        _with_file(repo)
        head = _git(repo, "rev-parse", "HEAD")
        stale = _entry(repo, head, description="old", icon="data:image/png;base64,AAAA")
        (repo / "connector.yaml").unlink()
        (repo / "icon.svg").unlink()
        _git(repo, "add", "-A")
        _git(repo, "commit", "-q", "-m", "drop the file")
        path = _config(tmp_path, stale)
        assert cli.main(["connector", "rebuild", "--config", str(path), "--latest"]) == 0
        block = yaml.safe_load(capsys.readouterr().out)["connectors"][0]
        assert "description" not in block and "icon" not in block
