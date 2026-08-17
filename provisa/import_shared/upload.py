# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1d4f60-5c72-4a19-9e3d-2f7a6c08b4d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Turn ONE uploaded artifact into the input the Hasura v2 / DDN converters already take (REQ-1483).

Both converters read a directory — the layout their CLIs are pointed at. An administrator using the
admin surface uploads a file: a zip of that directory, or the single consolidated document the
Hasura metadata API returns. This module is the only place that difference is resolved, so the
interactive importer and the CLIs run the same parsers over the same shapes.
"""

from __future__ import annotations

import io
import json
import tempfile
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

HASURA_V2 = "hasura_v2"
DDN = "ddn"

# An uploaded archive expands into a process temp directory, so the expanded size is bounded here
# rather than by the request-body limit alone (a zip bomb is small on the wire).
MAX_EXPANDED_BYTES = 256 * 1024 * 1024


class UploadError(ValueError):
    """The upload cannot be turned into converter input — bad archive, unknown kind, empty."""


@dataclass(frozen=True)
class StagedUpload:
    """What was uploaded, in the form the converters read.

    ``root`` is a directory for every archive upload and for a single ``.hml``; ``document`` is the
    parsed consolidated Hasura export when one file carried the whole metadata, and is ``None``
    otherwise. Exactly one of the two is what the caller converts.
    """

    flavor: str
    root: Path | None
    document: Any | None


def _extract_zip(data: bytes, dest: Path) -> None:
    """Expand ``data`` under ``dest``, refusing entries that escape it."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise UploadError("uploaded archive is not a readable zip") from exc
    total = 0
    with archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            total += info.file_size
            if total > MAX_EXPANDED_BYTES:
                raise UploadError(
                    f"archive expands to more than {MAX_EXPANDED_BYTES // (1024 * 1024)} MB"
                )
            target = (dest / info.filename).resolve()
            if not target.is_relative_to(dest.resolve()):
                raise UploadError(f"archive entry escapes the extraction directory: {info.filename}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(info))


def _single_root(root: Path) -> Path:
    """A zip made from a project folder holds one top-level directory; descend into it.

    Both converters resolve names relative to the root they are given — the DDN parser takes the
    first path component as the subgraph — so the wrapper directory the zip preserves has to be
    stepped through, or every subgraph would be named after the folder someone happened to zip.
    Exactly ONE level is stepped through: a deeper descent would eat the project's own top-level
    directory (``app/`` in a single-subgraph project) and rename the subgraph again.
    """
    entries = [p for p in root.iterdir() if not p.name.startswith((".", "__MACOSX"))]
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return root


def detect_flavor(root: Path) -> str:
    """Which converter reads this directory: a DDN project (``.hml`` files) or Hasura v2 metadata."""
    if next(root.rglob("*.hml"), None) is not None:
        return DDN
    for name in ("tables.yaml", "databases", "actions.yaml", "metadata.yaml"):
        if (root / name).exists():
            return HASURA_V2
    # A DDN project exported as .yaml carries `kind:` documents; v2 metadata files never do.
    for path in root.rglob("*.yaml"):
        head = path.read_text(encoding="utf-8", errors="replace")[:4096]
        if "\nkind:" in head or head.startswith("kind:"):
            return DDN
        break
    raise UploadError(
        "upload does not look like Hasura v2 metadata or a DDN project — expected tables.yaml / "
        "databases/ / actions.yaml, or .hml files"
    )


def _load_document(data: bytes, filename: str) -> Any:
    text = data.decode("utf-8")
    if filename.endswith(".json"):
        return json.loads(text)
    return yaml.safe_load(text)


@contextmanager
def staged_upload(filename: str, data: bytes, flavor: str = "auto") -> Generator[StagedUpload]:
    """Stage ``data`` for conversion, yielding a :class:`StagedUpload` valid for the block.

    ``flavor`` is ``auto`` (decide from the content), ``hasura_v2`` or ``ddn``. The temp directory
    an archive expands into is removed on exit, so nothing an administrator uploads outlives the
    request that converted it.
    """
    if not data:
        raise UploadError("uploaded file is empty")
    name = filename.lower()
    with tempfile.TemporaryDirectory(prefix="provisa-import-") as tmp:
        dest = Path(tmp)
        if name.endswith(".zip"):
            _extract_zip(data, dest)
            root = _single_root(dest)
            resolved = detect_flavor(root) if flavor == "auto" else flavor
            yield StagedUpload(flavor=resolved, root=root, document=None)
            return
        if name.endswith(".hml") or flavor == DDN:
            # A single .hml is a one-file DDN project: the parser globs a directory, so give it one.
            target = dest / (Path(filename).name or "project.hml")
            target.write_bytes(data)
            yield StagedUpload(flavor=DDN, root=dest, document=None)
            return
        if not name.endswith((".yaml", ".yml", ".json")):
            raise UploadError(f"unsupported upload {filename!r}: expected .zip, .yaml, .yml, .json or .hml")
        try:
            doc = _load_document(data, name)
        except (yaml.YAMLError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise UploadError(f"uploaded file is not valid YAML/JSON: {exc}") from exc
        if not isinstance(doc, dict):
            raise UploadError("a single-file Hasura export must be a mapping")
        yield StagedUpload(flavor=HASURA_V2, root=None, document=doc)
