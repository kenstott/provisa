# Copyright (c) 2026 Kenneth Stott
# Canary: 2872b1bb-4464-49c6-97bf-a75b4e771caf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org's spilled and temporary data is written in the org's folder (REQ-1992), where the org's
folder encryption covers it: DuckDB's temp_directory for an engine built for the org."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def org_folder(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PROVISA_ORG_FILES_NODE_ID", "node-a")
    folder = tmp_path / "orgs" / "acme"
    folder.mkdir(parents=True)
    return folder


def test_an_orgs_duckdb_spills_inside_its_folder(org_folder):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
    from provisa.federation.engine import build_engine

    engine = build_engine("duckdb")
    engine.temp_org = "acme"
    runtime = DuckDBFederationRuntime(store_engine=engine)
    (spill,) = runtime._con.execute("SELECT current_setting('temp_directory')").fetchone()
    assert Path(spill) == org_folder / "nodes" / "node-a" / "tmp"
    assert Path(spill).is_dir()


def test_an_engine_built_for_no_org_names_no_folder():
    from provisa.federation.engine import build_engine

    assert build_engine("duckdb").temp_org is None


async def test_an_uploaded_archive_is_unpacked_inside_the_orgs_folder(
    control_plane, org_folder, monkeypatch
):
    import io
    import zipfile

    from sqlalchemy import insert

    from provisa.codesource import uploads
    from provisa.core.schema_admin import orgs

    with control_plane.engine().begin() as conn:
        conn.execute(insert(orgs).values(id="acme", name="acme"))
    seen: list[Path] = []
    real = uploads.unpack

    def _unpack(filename, data, into):
        seen.append(Path(into))
        return real(filename, data, into)

    monkeypatch.setattr(uploads, "unpack", _unpack)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.js", "1")
    await uploads.put(control_plane.platform, "acme", "b.zip", buf.getvalue(), actor=None)
    (scratch,) = seen
    assert scratch.parent == org_folder / "nodes" / "node-a" / "tmp"
