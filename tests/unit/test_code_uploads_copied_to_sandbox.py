# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo sandbox keeps its template's uploads (REQ-1993, REQ-2006).

Uploads are kept per organisation, outside its schema, so the model clone does not carry them; a
sandbox's code sources name the template's uploads by content hash, so they are copied beside it."""

# Requirements: REQ-1993, REQ-2006
from __future__ import annotations

import io
import zipfile

import pytest

from provisa.codesource import uploads

pytestmark = pytest.mark.unit


def _zip(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return buf.getvalue()


@pytest.fixture
def plane(control_plane, tmp_path, monkeypatch):
    from provisa.core.schema_admin import orgs

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    db = control_plane.platform
    with db.engine.begin() as conn:
        for org in ("sbxtplchurn", "sbx01", "other"):
            (tmp_path / "data" / "orgs" / org).mkdir(parents=True)
            conn.execute(orgs.insert().values(id=org, name=org))
    return db


async def test_a_sandbox_reads_the_uploads_its_template_keeps(plane):
    archive = _zip({"churn/model.py": b"def score():\n    return 1\n"})
    found = await uploads.put(plane, "sbxtplchurn", "churn.zip", archive, actor=None)
    lock = await uploads.put_file(plane, "sbxtplchurn", "uv.lock", b"version = 1\n", actor=None)
    await uploads.put(plane, "other", "x.zip", _zip({"x/x.py": b"X = 1\n"}), actor=None)

    copied = await uploads.copy(plane, "sbxtplchurn", "sbx01")

    assert sorted(copied) == sorted([found, lock])
    assert (await uploads.read(plane, "sbx01", found)).data == archive
    assert await uploads.read_file(plane, "sbx01", lock) == b"version = 1\n"
    # Copying again keeps each once.
    assert sorted(await uploads.copy(plane, "sbxtplchurn", "sbx01")) == sorted([found, lock])
