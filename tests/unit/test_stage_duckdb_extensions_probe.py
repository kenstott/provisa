# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b2a1e-8c4d-4e7a-9b15-d2c0f7a94e61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-294 (amended 2026-11-10): the release build's probe on a host with no pgwire bundle (Linux
arm64) expects each pgwire connector to be refused with the runtime's own refusal, and fails on any
other outcome. The platforms come from the runtime's BUNDLE_VARIANTS, never a list in the probe."""

from __future__ import annotations

import asyncio

import pytest

from provisa.federation.duckdb_extensions import ExtensionProbe
from provisa.runtime_deps import pgwire_bundles
from scripts import stage_duckdb_extensions as probe_script


def _linux_arm64(monkeypatch) -> None:
    monkeypatch.setattr(pgwire_bundles.platform, "system", lambda: "Linux")
    monkeypatch.setattr(pgwire_bundles.platform, "machine", lambda: "aarch64")


def _arm64_refusal(monkeypatch) -> str:
    refusal = _arm64_refusal(monkeypatch)
    assert refusal is not None
    return refusal


def _linux_x86(monkeypatch) -> None:
    monkeypatch.setattr(pgwire_bundles.platform, "system", lambda: "Linux")
    monkeypatch.setattr(pgwire_bundles.platform, "machine", lambda: "x86_64")


def _row(source_type: str, *, available: bool, reason: str, extension: str = "postgres"):
    return ExtensionProbe(
        key=source_type,
        source_type=source_type,
        extension=extension,
        available=available,
        reason=reason,
        remediation=None if available else "fix it",
    )


def test_the_refusal_on_linux_arm64_is_the_runtimes_own_naming_the_built_platforms(monkeypatch):
    refusal = _arm64_refusal(monkeypatch)
    with pytest.raises(pgwire_bundles.BundleUnavailable) as runtime:
        pgwire_bundles.bundle_variant()
    assert refusal == str(runtime.value)
    assert "Linux/aarch64" in refusal
    for variant in pgwire_bundles.BUNDLE_VARIANTS.values():
        assert variant in refusal


def test_a_platform_the_runtime_builds_bundles_for_expects_no_refusal(monkeypatch):
    _linux_x86(monkeypatch)
    assert probe_script.pgwire_platform_refusal() is None


def test_the_platforms_follow_the_runtimes_variants_not_a_list_in_the_probe(monkeypatch):
    _linux_arm64(monkeypatch)
    monkeypatch.setitem(pgwire_bundles.BUNDLE_VARIANTS, ("Linux", "aarch64"), "linux-arm64")
    assert probe_script.pgwire_platform_refusal() is None


@pytest.mark.parametrize("source_type", sorted(pgwire_bundles.BUNDLE_CONNECTOR))
def test_each_pgwire_connector_refused_exactly_as_expected_passes(monkeypatch, source_type):
    refusal = _arm64_refusal(monkeypatch)
    row = _row(source_type, available=False, reason=refusal)
    assert probe_script.grade(row, {}, refusal) == ("NONE", False)


def test_a_pgwire_connector_that_loads_where_no_bundle_is_published_fails(monkeypatch):
    refusal = _arm64_refusal(monkeypatch)
    row = _row("files", available=True, reason="file bundle cached (engine-v0.123.2)")
    assert probe_script.grade(row, {}, refusal) == ("FAIL", True)


def test_a_pgwire_connector_failing_for_another_reason_fails(monkeypatch):
    refusal = _arm64_refusal(monkeypatch)
    row = _row("files", available=False, reason="postgres did not load: IOException")
    assert probe_script.grade(row, {}, refusal) == ("FAIL", True)


def test_a_pgwire_connector_whose_extension_has_no_build_still_fails_on_linux_arm64(monkeypatch):
    refusal = _arm64_refusal(monkeypatch)
    row = _row("files", available=False, reason="postgres did not load: IOException")
    assert probe_script.grade(row, {"postgres": "no build"}, refusal) == ("FAIL", True)


def test_a_non_pgwire_connector_is_graded_as_before_on_linux_arm64(monkeypatch):
    refusal = _arm64_refusal(monkeypatch)
    loaded = _row("mongodb", available=True, reason="mongo loaded", extension="mongo")
    failed = _row("mongodb", available=False, reason="did not load", extension="mongo")
    unbuilt = _row("google_sheets", available=False, reason="did not load", extension="gsheets")
    assert probe_script.grade(loaded, {}, refusal) == ("OK  ", False)
    assert probe_script.grade(failed, {}, refusal) == ("FAIL", True)
    assert probe_script.grade(unbuilt, {"gsheets": "no build"}, refusal) == ("SKIP", False)


def test_where_bundles_are_published_a_refused_pgwire_connector_fails(monkeypatch):
    _linux_x86(monkeypatch)
    row = _row("files", available=False, reason="no pgwire bundle is published for Linux/aarch64")
    assert probe_script.grade(row, {}, None) == ("FAIL", True)
    loaded = _row("files", available=True, reason="file bundle cached")
    assert probe_script.grade(loaded, {}, None) == ("OK  ", False)


def _run_main(monkeypatch, probes, capsys) -> tuple[int, str]:
    async def fake_probes():
        return probes

    monkeypatch.setattr(probe_script, "install", lambda: {})
    monkeypatch.setattr(probe_script, "probe_extensions", fake_probes)
    code = asyncio.run(probe_script._main())
    return code, capsys.readouterr().out


def test_linux_arm64_build_passes_when_every_pgwire_connector_is_refused(monkeypatch, capsys):
    refusal = _arm64_refusal(monkeypatch)
    probes = [
        _row(t, available=False, reason=refusal) for t in sorted(pgwire_bundles.BUNDLE_CONNECTOR)
    ]
    probes.append(_row("mongodb", available=True, reason="mongo loaded", extension="mongo"))
    code, out = _run_main(monkeypatch, probes, capsys)
    assert code == 0, out
    assert f"{len(pgwire_bundles.BUNDLE_CONNECTOR)} pgwire connectors refused as expected" in out
    assert "[FAIL]" not in out


def test_linux_arm64_build_fails_when_a_pgwire_connector_loads(monkeypatch, capsys):
    refusal = _arm64_refusal(monkeypatch)
    probes = [
        _row("files", available=True, reason="file bundle cached"),
        _row("jira", available=False, reason=refusal),
    ]
    code, out = _run_main(monkeypatch, probes, capsys)
    assert code == 1
    assert "[FAIL] files" in out
    assert f"expected the refusal: {refusal}" in out
