# Copyright (c) 2026 Kenneth Stott
# Canary: 5f3c9e1a-7b2d-4e8f-9a61-2c4d8b0e7f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""packaging/duckdb-ext/collect.py fills the wheel's _ext tree with ONE platform's mirror (one wheel per
platform, maintainer decision 2026-11-06), in the layout DuckDB loads from: <version>/<platform>/<name>.duckdb_extension, and a named repository's
extension (crypto, from Query.Farm's, on DuckDB 2.0) under repositories/<repo>/ with its .info --
without which ``LOAD crypto FROM query_farm`` finds nothing."""

from __future__ import annotations

import importlib.util
import inspect
import os
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _collect():
    spec = importlib.util.spec_from_file_location(
        "duckdb_ext_collect", REPO / "packaging/duckdb-ext/collect.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["duckdb_ext_collect"] = module
    spec.loader.exec_module(module)
    return module


def _mirrored(root: Path, version: str) -> Path:
    plat = root / "osx-arm64" / f"v{version}" / "osx_arm64"
    (plat / "repositories" / "query_farm").mkdir(parents=True)
    (plat / "mssql.duckdb_extension").write_bytes(b"MSSQL")
    (plat / "repositories" / "query_farm" / "crypto.duckdb_extension").write_bytes(b"CRYPTO")
    (plat / "repositories" / "query_farm" / "crypto.duckdb_extension.info").write_bytes(b"INFO")
    (root / "osx-arm64" / "manifest.json").write_text("{}")
    return root


def test_collect_keeps_a_named_repositorys_layout_and_info(tmp_path, monkeypatch):
    collect = _collect()
    dest = tmp_path / "_ext"
    monkeypatch.setattr(collect, "_DEST", dest)
    version = collect._pinned_duckdb_version()

    assert collect.main(_mirrored(tmp_path / "mirror", version), "osx-arm64", 1) == 0

    plat = dest / f"v{version}" / "osx_arm64"
    assert (plat / "mssql.duckdb_extension").read_bytes() == b"MSSQL"
    assert (plat / "repositories/query_farm/crypto.duckdb_extension").read_bytes() == b"CRYPTO"
    assert (plat / "repositories/query_farm/crypto.duckdb_extension.info").read_bytes() == b"INFO"
    assert not (dest / "manifest.json").exists()


def test_collect_refuses_blobs_of_another_duckdb(tmp_path, monkeypatch):
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "_ext")

    assert collect.main(_mirrored(tmp_path / "mirror", "1.5.5"), "osx-arm64", 1) == 1


def test_collect_holds_only_the_named_platform_and_clears_the_previous_one(tmp_path, monkeypatch):
    # Built in a loop, one platform after another: a wheel must never carry the previous one's blobs.
    collect = _collect()
    dest = tmp_path / "_ext"
    monkeypatch.setattr(collect, "_DEST", dest)
    version = collect._pinned_duckdb_version()
    mirror = _mirrored(tmp_path / "mirror", version)
    linux = mirror / "linux-x64" / f"v{version}" / "linux_amd64"
    linux.mkdir(parents=True)
    (linux / "mssql.duckdb_extension").write_bytes(b"LINUX")

    assert collect.main(mirror, "osx-arm64", 1) == 0
    assert collect.main(mirror, "linux-x64", 1) == 0

    assert sorted(p.name for p in (dest / f"v{version}").iterdir()) == ["linux_amd64"]
    assert (dest / f"v{version}/linux_amd64/mssql.duckdb_extension").read_bytes() == b"LINUX"


def test_collect_refuses_a_mirror_of_another_platform(tmp_path, monkeypatch):
    # The linux-x64 wheel's tree must be the dir DuckDB names linux_amd64, or the runtime finds nothing.
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "_ext")
    version = collect._pinned_duckdb_version()
    mirror = tmp_path / "mirror"
    wrong = mirror / "linux-x64" / f"v{version}" / "osx_arm64"
    wrong.mkdir(parents=True)
    (wrong / "mssql.duckdb_extension").write_bytes(b"OSX")

    assert collect.main(mirror, "linux-x64", 1) == 1


def test_collect_refuses_an_unknown_label_and_a_missing_mirror(tmp_path, monkeypatch):
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "_ext")
    mirror = _mirrored(tmp_path / "mirror", collect._pinned_duckdb_version())

    assert collect.main(mirror, "osx-x64", 1) == 1
    assert collect.main(mirror, "windows-x64", 1) == 1


def _incompressible(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(os.urandom(size))


def _sized_mirror(tmp_path: Path, version: str, sizes: dict[str, int]) -> Path:
    plat = tmp_path / "mirror" / "linux-x64" / f"v{version}" / "linux_amd64"
    for name, size in sizes.items():
        _incompressible(plat / f"{name}.duckdb_extension", size * 1000)
    return tmp_path / "mirror"


def test_each_part_holds_only_its_own_extensions(tmp_path, monkeypatch, capsys):
    collect = _collect()
    dest = tmp_path / "_ext"
    monkeypatch.setattr(collect, "_DEST", dest)
    version = collect._pinned_duckdb_version()
    mirror = _sized_mirror(
        tmp_path, version, {"mssql": 5, "bigquery": 5, "mongo": 5, "firebird": 5}
    )

    assert collect.main(mirror, "linux-x64", 1, budget=100_000) == 0
    packed = dest / f"v{version}" / "linux_amd64"
    assert sorted(p.name for p in packed.glob("*.duckdb_extension")) == [
        "bigquery.duckdb_extension",
        "mssql.duckdb_extension",
    ]
    assert collect.main(mirror, "linux-x64", 2, budget=100_000) == 0
    assert sorted(p.name for p in packed.glob("*.duckdb_extension")) == [
        "firebird.duckdb_extension",
        "mongo.duckdb_extension",
    ]
    assert "not mirrored: gsheets, airport" in capsys.readouterr().out


def test_a_part_past_its_budget_stops_the_build_by_name(tmp_path, monkeypatch, capsys):
    # Never left out silently: the part is moved in PARTS.
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "_ext")
    version = collect._pinned_duckdb_version()
    mirror = _sized_mirror(tmp_path, version, {"mssql": 60, "bigquery": 60})
    assert collect.main(mirror, "linux-x64", 1, budget=100_000) == 1
    assert "linux-x64 part 1 is" in capsys.readouterr().err


def test_a_mirrored_extension_in_no_part_stops_the_build_by_name(tmp_path, monkeypatch, capsys):
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "_ext")
    version = collect._pinned_duckdb_version()
    mirror = _sized_mirror(tmp_path, version, {"mssql": 1, "zz_other": 1})
    assert collect.main(mirror, "linux-x64", 1) == 1
    assert "zz_other: in no part" in capsys.readouterr().err


def test_an_unknown_part_is_refused(tmp_path, monkeypatch):
    collect = _collect()
    monkeypatch.setattr(collect, "_DEST", tmp_path / "_ext")
    assert collect.main(tmp_path, "linux-x64", 99) == 1


def test_the_parts_hold_every_extension_once_and_each_platform_fits():
    collect = _collect()
    assert collect.PRIORITY == (
        "crypto",
        "mssql",
        "snowflake",
        "bigquery",
        "mongo",
        "gsheets",
        "airport",
        "firebird",
    )
    assert len(set(collect.PRIORITY)) == len(collect.PRIORITY)
    # Compressed bytes measured for DuckDB 2.0 from the publishers' .gz files, largest platform.
    linux_amd64 = {
        "mssql": 20712503,
        "snowflake": 16655074,
        "bigquery": 33105376,
        "mongo": 20775300,
        "firebird": 16743763,
        "crypto": 6553109,
        "airport": 31106067,
    }
    budget = collect.MAX_WHEEL_BYTES - collect.HEADROOM_BYTES
    for part in collect.PARTS:
        assert sum(linux_amd64.get(name, 0) for name in part) < budget


def test_the_part_count_is_the_one_the_stage_command_fetches():
    from provisa.federation.duckdb_extensions import EXT_PACKAGE_PARTS, EXT_PACKAGE_RELEASE

    collect = _collect()
    assert EXT_PACKAGE_PARTS == len(collect.PARTS)
    version = tomllib.loads((REPO / "packaging/duckdb-ext/pyproject.toml").read_text())["project"]
    assert version["version"] == EXT_PACKAGE_RELEASE


def test_set_version_writes_the_part_into_pyproject(tmp_path, monkeypatch):
    collect = _collect()
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.3.0"  # release\n')
    monkeypatch.setattr(collect, "_HERE", tmp_path)
    assert collect.set_version(2) == 0
    assert 'version = "0.3.0.2"' in (tmp_path / "pyproject.toml").read_text()
    assert collect.set_version(1) == 0  # a second part builds over the first's version line
    assert 'version = "0.3.0.1"' in (tmp_path / "pyproject.toml").read_text()


def test_the_fill_budget_leaves_room_under_pypis_limit():
    collect = _collect()
    assert 0 < collect.HEADROOM_BYTES < collect.MAX_WHEEL_BYTES
    default = inspect.signature(collect.main).parameters["budget"].default
    assert default == collect.MAX_WHEEL_BYTES - collect.HEADROOM_BYTES


def test_deflated_size_is_what_deflate_makes_of_the_file(tmp_path):
    import zlib

    collect = _collect()
    blob = tmp_path / "x.duckdb_extension"
    blob.write_bytes(b"A" * 100_000 + os.urandom(10_000))
    expected = len(zlib.compress(blob.read_bytes(), wbits=-15))
    assert collect._deflated_size(blob) == expected


def _wheels(dist: Path, sizes: dict[str, int]) -> Path:
    dist.mkdir()
    for tag, size in sizes.items():
        for part in range(1, len(_collect().PARTS) + 1):
            with (dist / f"provisa_duckdb_ext-0.3.0.{part}-py3-none-{tag}.whl").open("wb") as f:
                f.truncate(size)
    return dist


def _all_tags(collect, size: int) -> dict[str, int]:
    return {tag: size for _plat, tag in collect.PLATFORMS.values()}


def test_check_wheels_passes_one_wheel_per_platform_under_the_limit(tmp_path):
    collect = _collect()
    dist = _wheels(tmp_path / "dist", _all_tags(collect, collect.MAX_WHEEL_BYTES - 1))

    assert collect.check_wheels(dist) == 0


def test_check_wheels_fails_a_wheel_at_pypis_limit_by_name(tmp_path, capsys):
    collect = _collect()
    sizes = _all_tags(collect, 1000)
    sizes["manylinux_2_28_x86_64"] = collect.MAX_WHEEL_BYTES
    dist = _wheels(tmp_path / "dist", sizes)

    assert collect.check_wheels(dist) == 1
    assert (
        "provisa_duckdb_ext-0.3.0.1-py3-none-manylinux_2_28_x86_64.whl is 100.0 MB"
        in capsys.readouterr().err
    )


def test_check_wheels_fails_a_missing_platform_and_a_universal_wheel(tmp_path, capsys):
    collect = _collect()
    sizes = _all_tags(collect, 1000)
    del sizes["win_amd64"]
    sizes["any"] = 1000
    dist = _wheels(tmp_path / "dist", sizes)

    assert collect.check_wheels(dist) == 1
    err = capsys.readouterr().err
    assert "windows-x64 part 1: expected one *.1-py3-none-win_amd64.whl, found 0" in err
    assert "provisa_duckdb_ext-0.3.0.1-py3-none-any.whl: not a platform wheel" in err


def test_the_100_mb_limit_is_pypis():
    # "400 File too large. Limit for project 'provisa-duckdb-ext' is 100 MB" (run 42238598659).
    assert _collect().MAX_WHEEL_BYTES == 100 * 1000 * 1000


def test_collect_carries_a_repositorys_trust_anchor_with_its_extensions(tmp_path, monkeypatch):
    collect = _collect()
    dest = tmp_path / "_ext"
    monkeypatch.setattr(collect, "_DEST", dest)
    version = collect._pinned_duckdb_version()
    mirror = _mirrored(tmp_path / "mirror", version)
    repo = mirror / "osx-arm64" / f"v{version}" / "osx_arm64" / "repositories" / "query_farm"
    (repo / "query_farm.duckdb_extension.repo.json").write_text("{}")

    assert collect.main(mirror, "osx-arm64", 1) == 0

    packed = dest / f"v{version}" / "osx_arm64" / "repositories" / "query_farm"
    assert (packed / "query_farm.duckdb_extension.repo.json").read_text() == "{}"
    # It belongs to the repository, not a packed extension of its own name.
    assert not list(dest.rglob("query_farm.duckdb_extension"))
