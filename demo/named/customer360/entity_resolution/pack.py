# Copyright (c) 2026 Kenneth Stott
# Canary: 81dd22a4-0c30-4394-a7c1-883033d2b2d0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pack the entity-resolution code source into the demo's code/ archive and print its content
hash, the upload config.yaml names it by (source entity_resolution):

    .venv/bin/python3 demo/named/customer360/entity_resolution/pack.py

The archive holds the package under one top-level directory, entries sorted, owners and times
zeroed, so the same files pack to the same bytes on every host."""

# Requirements: REQ-2034

import gzip
import io
import sys
import tarfile
import tempfile
from pathlib import Path

HERE = Path(__file__).parent
CODE = HERE.parent / "code"
NAME = HERE.name
#: What the archive holds: the package's modules (its pyproject's only-include) and the pyproject.
FILES = ("entity_resolution.py", "er_job.py", "er_source.py", "pyproject.toml")
ARCHIVE = f"{NAME}.tar.gz"


def archive() -> bytes:
    """The package's archive, the same bytes for the same files."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        top = tarfile.TarInfo(NAME)
        top.type, top.mode = tarfile.DIRTYPE, 0o755
        tar.addfile(top)
        for file in sorted(FILES):
            data = (HERE / file).read_bytes()
            info = tarfile.TarInfo(f"{NAME}/{file}")
            info.size, info.mode = len(data), 0o644
            tar.addfile(info, io.BytesIO(data))
    packed = io.BytesIO()
    with gzip.GzipFile(fileobj=packed, mode="wb", mtime=0) as gz:
        gz.write(raw.getvalue())
    return packed.getvalue()


def upload_hash(data: bytes) -> str:
    """The content hash an upload of ``data`` is kept under."""
    from provisa.codesource.bundle import content_hash, unpack

    with tempfile.TemporaryDirectory() as scratch:
        return content_hash(unpack(ARCHIVE, data, Path(scratch)))


def main() -> int:
    data = archive()
    CODE.mkdir(exist_ok=True)
    (CODE / ARCHIVE).write_bytes(data)
    print(NAME, upload_hash(data))
    return 0


if __name__ == "__main__":
    sys.exit(main())
