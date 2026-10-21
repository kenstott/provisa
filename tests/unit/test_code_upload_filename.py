# Copyright (c) 2026 Kenneth Stott
# Canary: 530bf3dc-c387-4b11-a2b7-60820a54c266
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An uploaded code archive keeps the name of the file it came from (REQ-2101): its source's
mapping carries it beside the content hash, so the source's form can say what the code is."""

# Requirements: REQ-2101

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.codesource.declared_code import declared_archives
from provisa.codesource.source import config_of

pytestmark = pytest.mark.unit

_NAMED = Path(__file__).resolve().parents[2] / "demo" / "named"


def test_an_uploaded_archives_filename_is_kept_beside_its_hash():
    config = config_of(
        "python",
        {
            "runtime": "python",
            "code": {"kind": "upload", "upload": "h", "filename": "model.tar.gz"},
        },
    )
    assert config.code == {"kind": "upload", "upload": "h", "filename": "model.tar.gz"}


def _uploaded_code() -> list[tuple[str, str, dict]]:
    found = []
    for config in sorted(_NAMED.glob("*/config.yaml")):
        for source in yaml.safe_load(config.read_text()).get("sources") or []:
            code = (source.get("mapping") or {}).get("code") or {}
            if code.get("kind") == "upload":
                found.append((config.parent.name, source["id"], code))
    return found


def test_there_is_uploaded_demo_code_to_check():
    assert _uploaded_code()


@pytest.mark.parametrize("demo, source_id, code", _uploaded_code(), ids=lambda v: str(v)[:40])
def test_each_demos_uploaded_code_names_the_archive_it_ships(demo, source_id, code):
    shipped = {p.name for p in declared_archives(_NAMED / demo)}
    assert code.get("filename") in shipped, (
        f"{demo}/{source_id}: {code.get('filename')!r} not in {shipped}"
    )


@pytest.mark.parametrize("demo, source_id, code", _uploaded_code(), ids=lambda v: str(v)[:40])
def test_each_demos_uploaded_code_is_pinned_to_its_archives_content(
    demo, source_id, code, tmp_path
):
    """Pinned as a source saved in the UI is (content_hash), so it counts as saved there and its
    image can be built; the pin is the hash of what its archive unpacks to."""
    from provisa.codesource.bundle import content_hash
    from provisa.codesource.uploads import unpack

    mapping = next(
        s["mapping"]
        for s in yaml.safe_load((_NAMED / demo / "config.yaml").read_text())["sources"]
        if s["id"] == source_id
    )
    archive = _NAMED / demo / "code" / code["filename"]
    assert mapping.get("content_hash") == content_hash(
        unpack(archive.name, archive.read_bytes(), tmp_path)
    )
