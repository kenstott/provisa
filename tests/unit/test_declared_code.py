# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A configuration's declared code archives are kept as its org's uploads (REQ-2101).

code/<name>.tar.gz beside a config.yaml is kept as the org's upload under the content hash of the
code it unpacks to -- the hash the config's code source names -- once, however often the org's
model is built."""

# Requirements: REQ-2101
from __future__ import annotations

import io
import tarfile

import pytest

from provisa.codesource import uploads
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.declared_code import adopt_declared_code, declared_archives

pytestmark = pytest.mark.unit


def _tar_gz(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


@pytest.fixture
def plane(control_plane, tmp_path, monkeypatch):
    from provisa.core.schema_admin import orgs

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    (tmp_path / "data" / "orgs" / "sbxtplchurnprod").mkdir(parents=True)
    db = control_plane.platform
    with db.engine.begin() as conn:
        conn.execute(orgs.insert().values(id="sbxtplchurnprod", name="churnprod template"))
    return db


async def test_each_declared_archive_is_kept_as_the_orgs_upload(plane, tmp_path):
    demo = tmp_path / "churnprod"
    (demo / "code").mkdir(parents=True)
    model = _tar_gz({"churn_model/churn_model.py": b"def churn_risk():\n    return []\n"})
    twin = _tar_gz({"churn-model-js/package.json": b'{"name": "churn-model-js"}\n'})
    (demo / "code" / "churn_model.tar.gz").write_bytes(model)
    (demo / "code" / "churn-model-js.tar.gz").write_bytes(twin)
    (demo / "code" / "README.txt").write_text("not an archive")

    assert [p.name for p in declared_archives(demo)] == [
        "churn-model-js.tar.gz",
        "churn_model.tar.gz",
    ]
    kept = await adopt_declared_code(plane, "sbxtplchurnprod", demo)

    assert (await uploads.read(plane, "sbxtplchurnprod", kept[1])).data == model
    assert (await uploads.read(plane, "sbxtplchurnprod", kept[0])).data == twin
    # The model built again keeps each once, under the same hash.
    assert await adopt_declared_code(plane, "sbxtplchurnprod", demo) == kept


async def test_a_directory_without_code_declares_none(plane, tmp_path):
    assert await adopt_declared_code(plane, "sbxtplchurnprod", tmp_path) == []


async def test_an_archive_that_cannot_be_used_is_refused_by_name(plane, tmp_path):
    (tmp_path / "code").mkdir()
    (tmp_path / "code" / "broken.tar.gz").write_bytes(b"not a tar")
    with pytest.raises(BundleRefused):
        await adopt_declared_code(plane, "sbxtplchurnprod", tmp_path)
