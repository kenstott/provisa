"""REQ-2269: under ``provisa run`` the native launch profile's engine is a default.

Precedence: the environment, then the config file's ``federation_engine``, then the profile's
engine. The profile resolver is replaced with its resolved env so no embedded PostgreSQL starts.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from provisa import cli
from provisa.core import desktop_profile
from provisa.core.desktop_profile import LaunchProfile

_PROFILE_ENV = {"PROVISA_ENGINE": "duckdb", "PROVISA_REDIS_EMBEDDED": "1"}


@pytest.fixture
def native_profile(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    def _load(preset: str, **_: object) -> LaunchProfile:
        assert preset == "native"
        return LaunchProfile(
            name="native",
            env=dict(_PROFILE_ENV),
            control_plane_store="embedded_pg",
            sources=[],
        )

    monkeypatch.setattr(desktop_profile, "load_profile", _load)
    # Skip the offline DuckDB extension staging: it is not under test here.
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "ext"))
    for key in ("PROVISA_ENGINE", "PROVISA_REDIS_EMBEDDED", "PROVISA_CONFIG"):
        monkeypatch.delenv(key, raising=False)
    data_dir = tmp_path / "native"
    data_dir.mkdir()
    return data_dir


def _config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "provisa.yaml"
    path.write_text(body)
    return path


def test_env_engine_wins_over_config_and_profile(
    native_profile: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PROVISA_ENGINE", "pg")
    monkeypatch.setenv("PROVISA_CONFIG", str(_config(tmp_path, "federation_engine: trino\n")))
    cli._apply_embedded_env(native_profile)
    assert os.environ["PROVISA_ENGINE"] == "pg"


def test_config_engine_wins_over_profile_default(
    native_profile: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PROVISA_CONFIG", str(_config(tmp_path, "federation_engine: trino\n")))
    cli._apply_embedded_env(native_profile)
    assert "PROVISA_ENGINE" not in os.environ
    # The rest of the profile still applies.
    assert os.environ["PROVISA_REDIS_EMBEDDED"] == "1"

    # With the env left silent, the engine resolver reads the config's engine.
    from provisa.federation import engine

    monkeypatch.setattr(
        "provisa.core.request_context._platform_config_provider",
        lambda: {"federation_engine": "trino"},
    )
    assert engine.configured_engine_kind() == "trino"


def test_profile_default_when_env_and_config_are_silent(
    native_profile: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PROVISA_CONFIG", str(_config(tmp_path, "sources: []\n")))
    cli._apply_embedded_env(native_profile)
    assert os.environ["PROVISA_ENGINE"] == "duckdb"


def test_profile_default_when_no_config_is_named(native_profile: Path) -> None:
    cli._apply_embedded_env(native_profile)
    assert os.environ["PROVISA_ENGINE"] == "duckdb"


def test_profile_default_when_the_named_config_does_not_exist_yet(
    native_profile: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PROVISA_CONFIG", str(tmp_path / "absent.yaml"))
    cli._apply_embedded_env(native_profile)
    assert os.environ["PROVISA_ENGINE"] == "duckdb"


def test_a_broken_config_fails_loud(
    native_profile: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import yaml

    monkeypatch.setenv("PROVISA_CONFIG", str(_config(tmp_path, "federation_engine: [trino\n")))
    with pytest.raises(yaml.YAMLError):
        cli._apply_embedded_env(native_profile)
