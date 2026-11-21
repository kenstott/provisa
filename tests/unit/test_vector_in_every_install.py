# Copyright (c) 2026 Kenneth Stott
# Canary: 7e50948c-72ad-4936-bd00-3646e6cf991f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Every install ships the embedding library the default config's embedding model needs
(maintainer 2026-11-17, "Ship it in every install").

The default config (config/provisa-install.yaml) registers a huggingface embedding model, which
runs on sentence-transformers over torch: the vector extra. Every native installer (DMG, AppImage,
Windows), ``pip install provisa[embedded]``, the container image and CI's environment carry it,
torch as the CPU-only build (PyPI's Linux torch is the CUDA one). An install without it refuses
such a model by name at startup.
"""

# Requirements: REQ-420, REQ-1008

from __future__ import annotations

import importlib.util
import re
import tomllib
from pathlib import Path

import pytest
import yaml

from provisa.vector.providers import (
    PROVIDER_LIBRARIES,
    EmbeddingLibraryMissing,
    require_provider_libraries,
)
from provisa.vector.registry import VectorModel

_ROOT = Path(__file__).resolve().parents[2]
_CPU_INDEX = "https://download.pytorch.org/whl/cpu"


def _text(relative: str) -> str:
    return (_ROOT / relative).read_text(encoding="utf-8")


def _pyproject() -> dict:
    return tomllib.loads(_text("pyproject.toml"))


def _extras() -> dict[str, list[str]]:
    return _pyproject()["project"]["optional-dependencies"]


# --- the extras ----------------------------------------------------------------------------------


def test_the_default_configs_embedding_providers_libraries_ship_in_the_embedded_extra():
    """Each provider of the default config's embedding models whose library is an extra has that
    extra inside provisa[embedded], the set every native installer installs."""
    models = yaml.safe_load(_text("config/provisa-install.yaml"))["vector_models"]
    needed = {
        PROVIDER_LIBRARIES[m["provider"]][0] for m in models if m["provider"] in PROVIDER_LIBRARIES
    }
    assert needed == {"vector"}
    assert {f"provisa[{extra}]" for extra in needed} <= set(_extras()["embedded"])


def test_the_vector_extra_names_torch_so_its_source_applies():
    assert set(_extras()["vector"]) == {"sentence-transformers", "torch"}


def test_ci_and_test_environments_carry_the_vector_extra():
    """Every CI job's environment is `uv sync --extra dev ...` (.github/actions/setup-python), and
    job worktrees sync the same: dev carries the extra."""
    assert "provisa[vector]" in _extras()["dev"]
    action = yaml.safe_load(_text(".github/actions/setup-python/action.yml"))
    assert action["inputs"]["extras"]["default"] == "dev"


def test_linux_torch_comes_from_the_cpu_index():
    uv = _pyproject()["tool"]["uv"]
    (index,) = [i for i in uv["index"] if i["name"] == "pytorch-cpu"]
    assert index == {"name": "pytorch-cpu", "url": _CPU_INDEX, "explicit": True}
    assert _pyproject()["tool"]["uv"]["sources"]["torch"] == [
        {"index": "pytorch-cpu", "marker": "sys_platform == 'linux'"}
    ]


def _lock_packages() -> list[dict]:
    return tomllib.loads(_text("uv.lock"))["package"]


def test_the_lock_holds_no_cuda_build():
    names = {p["name"] for p in _lock_packages()}
    cuda = sorted(n for n in names if n.startswith(("nvidia-", "cuda-")) or n == "triton")
    assert cuda == []


def test_the_lock_takes_linux_torch_as_the_cpu_build_and_the_others_from_pypi():
    torches = {p["version"]: p for p in _lock_packages() if p["name"] == "torch"}
    cpu = [v for v in torches if v.endswith("+cpu")]
    assert len(cpu) == 1, sorted(torches)
    assert torches[cpu[0]]["source"] == {"registry": _CPU_INDEX}
    wheels = [w["url"].rsplit("/", 1)[1] for w in torches[cpu[0]]["wheels"]]
    assert any("manylinux_2_28_x86_64" in w for w in wheels)
    pypi = [p for v, p in torches.items() if v != cpu[0]]
    assert [p["version"] for p in pypi] == [cpu[0].removesuffix("+cpu")]
    pypi_wheels = [w["url"].rsplit("/", 1)[1] for w in pypi[0]["wheels"]]
    assert any("macosx" in w and "arm64" in w for w in pypi_wheels)
    assert any("win_amd64" in w for w in pypi_wheels)
    assert not any("linux" in w for w in pypi_wheels)


def test_the_airgap_mirror_lock_holds_the_vector_extra_on_cpu_torch():
    lock = _text("packaging/pypi/requirements-embedded.lock")
    assert re.search(r"^sentence-transformers==", lock, re.M)
    assert re.search(r"^torch==\S+\+cpu ; sys_platform == 'linux'", lock, re.M)
    assert not re.search(r"^(nvidia-|triton==|cuda-)", lock, re.M)
    assert _CPU_INDEX in _text("packaging/pypi/MIRROR-CHECKLIST.md")


# --- the installers ------------------------------------------------------------------------------


def test_the_dmg_installs_the_embedded_extra_and_checks_the_embedding_library():
    script = _text("packaging/macos/build-dmg.sh")
    assert '"${built_wheel}[embedded]"' in script
    install = script.index('"${built_wheel}[embedded]"')
    check = script.index("import sentence_transformers, torch; assert torch.version.cuda is None")
    assert install < check


def test_the_appimage_wheelhouse_takes_cpu_torch_and_refuses_a_cuda_build():
    script = _text("packaging/linux/build-appimage.sh")
    fetch_torch = script.index(f'--index-url {_CPU_INDEX} "torch==${{torch_cpu}}"')
    fetch_rest = script.index('"${built_wheel}[embedded]" "torch==${torch_cpu}"')
    guard = script.index("the wheelhouse holds CUDA wheels")
    vector = script.index("the wheelhouse lacks the vector extra")
    assert fetch_torch < fetch_rest < guard < vector
    assert 'version = "\\(.*+cpu\\)"' in script  # the pinned version is uv.lock's +cpu torch


def test_the_linux_first_launch_installs_cpu_torch_before_provisa_and_checks_it():
    script = _text("packaging/linux/first-launch.sh")
    body = script[script.index("setup_native_venv() {") :]
    body = body[: body.index("\n}\n")]
    torch = body.index(f"--index-url {_CPU_INDEX} torch")
    provisa = body.index('"provisa[${extras}]${pin}"')
    check = body.index("import sentence_transformers, torch; assert torch.version.cuda is None")
    assert torch < provisa < check
    extras = script[script.index("_native_extras() {") :]
    extras = extras[: extras.index("\n}\n")]
    assert all(
        printed.startswith("embedded") for printed in re.findall(r"printf '([^']+)'", extras)
    )


def test_the_windows_installer_installs_the_vector_extra_and_checks_it():
    script = _text("packaging/windows/build-sfx.ps1")
    install = script.index('"$RepoRoot[vector]"')
    check = script.index("import sentence_transformers, torch; assert torch.version.cuda is None")
    assert install < check


def test_the_image_installs_the_vector_extra_on_cpu_torch():
    dockerfile = _text("Dockerfile")
    extras = re.search(r"^ARG PROVISA_EXTRAS=(\S+)$", dockerfile, re.M)
    assert extras is not None and "vector" in extras.group(1).split(",")
    assert "grep -E '^torch==[^ ]+\\+cpu '" in dockerfile
    assert f"--index-url {_CPU_INDEX}" in dockerfile


# --- the startup refusal -------------------------------------------------------------------------


def _hf(model_id: str = "sentence-transformers/all-MiniLM-L6-v2") -> VectorModel:
    return VectorModel(id=model_id, provider="huggingface", dimensions=384)


def test_a_huggingface_model_without_its_library_is_refused_by_name(monkeypatch):
    found: list[str] = []

    def find_spec(name):
        found.append(name)
        return None

    monkeypatch.setattr(importlib.util, "find_spec", find_spec)
    with pytest.raises(EmbeddingLibraryMissing) as refused:
        require_provider_libraries([_hf()])
    assert found == ["sentence_transformers"]
    assert refused.value.code == "ai_models.embedding_library_missing"
    assert refused.value.params == {
        "model": "sentence-transformers/all-MiniLM-L6-v2",
        "provider": "huggingface",
        "extra": "vector",
        "module": "sentence_transformers",
    }
    assert "pip install 'provisa[vector]'" in str(refused.value)


def test_a_huggingface_model_with_its_library_installed_is_accepted():
    require_provider_libraries([_hf()])


def test_a_network_providers_model_needs_no_library(monkeypatch):
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: pytest.fail(f"looked for {name}"))
    require_provider_libraries(
        [
            VectorModel(id="text-embedding-3-small", provider="openai", dimensions=2),
            VectorModel(id="nomic", provider="ollama", dimensions=2),
        ]
    )


def test_the_refusal_is_catalogued():
    import json

    catalog = json.loads(_text("provisa-ui/src/i18n/locales/en/serverErrors.json"))
    group, name = EmbeddingLibraryMissing.code.split(".")
    message = catalog["serverErrors"][group][name]
    assert set(re.findall(r"\{\{(\w+)\}\}", message)) == {"model", "provider", "extra", "module"}


def test_the_server_start_refuses_the_config_before_it_is_used():
    """app.py's start checks the parsed config's embedding models right after parsing it, before
    the config is held as the running one."""
    source = _text("provisa/api/app.py")
    parsed = source.index("config = parse_config_dict(raw_config)")
    checked = source.index("require_provider_libraries(config.vector_models)", parsed)
    held = source.index("state.config = config", parsed)
    assert parsed < checked < held
