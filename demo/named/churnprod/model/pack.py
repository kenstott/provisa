# Copyright (c) 2026 Kenneth Stott
# Canary: b73a9032-783d-4fb4-acae-a2fc0aca774f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pack the two packages into the demo's code/ archives and print each one's content hash, the
upload config.yaml names it by:

    .venv/bin/python3 demo/named/churnprod/model/pack.py

Each archive holds its package under one top-level directory, entries sorted, owners and times
zeroed, so the same files pack to the same bytes on every host."""

import gzip
import io
import sys
import tarfile
import tempfile
from pathlib import Path

HERE = Path(__file__).parent
CODE = HERE.parent / "code"
PACKAGES = {
    "churn_model": ("churn_model.py", "model.joblib", "pyproject.toml", "uv.lock"),
    "churn-model-js": ("package.json", "index.js", "index.d.ts", "model.js"),
}


def _archive(name: str, files: tuple[str, ...]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        top = tarfile.TarInfo(name)
        top.type, top.mode = tarfile.DIRTYPE, 0o755
        tar.addfile(top)
        for file in sorted(files):
            data = (HERE / name / file).read_bytes()
            info = tarfile.TarInfo(f"{name}/{file}")
            info.size, info.mode = len(data), 0o644
            tar.addfile(info, io.BytesIO(data))
    packed = io.BytesIO()
    with gzip.GzipFile(fileobj=packed, mode="wb", mtime=0) as gz:
        gz.write(raw.getvalue())
    return packed.getvalue()


def main() -> None:
    from provisa.codesource.bundle import content_hash, unpack

    CODE.mkdir(exist_ok=True)
    for name, files in PACKAGES.items():
        data = _archive(name, files)
        (CODE / f"{name}.tar.gz").write_bytes(data)
        with tempfile.TemporaryDirectory() as scratch:
            print(name, content_hash(unpack(f"{name}.tar.gz", data, Path(scratch))))


if __name__ == "__main__":
    sys.exit(main())
