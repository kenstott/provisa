# Copyright (c) 2026 Kenneth Stott
# Canary: 8f4a2c6d-1e9b-4a7c-9d3f-6b2e8a1c4d7f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kaggle dataset staging (REQ-1780/1781/1782) — v1 is CSV/Parquet only.

Kaggle datasets are ingested as plain ``csv``/``parquet`` Sources: Trino ATTACHes them live
through ``TrinoCsvConnector``/``TrinoParquetConnector`` (a directory glob-scan), exactly like any
manually-created file source. This module owns the one Kaggle-specific step those types don't
otherwise need: fetching the bundle and unzipping it onto local disk, one file per its own
subdirectory (matching ``_TrinoSingleFileConnector``'s documented assumption that a source's own
directory holds exactly the one file it names — see trino_connectors.py's ``_TrinoSingleFileConnector.details``).

A bundle containing a SQLite/.db file is rejected outright (v1 scope cut) rather than partially
staged — a clear, actionable error beats a silently incomplete import.
"""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

from provisa.kaggle.client import download_dataset, get_dataset_metadata

# Requirements: REQ-1780, REQ-1781, REQ-1782

_SUPPORTED_EXTENSIONS = (".csv", ".parquet")
_UNSUPPORTED_EXTENSIONS = (".sqlite", ".sqlite3", ".db")

# REQ-1819: the one shared secret name every Kaggle-staging caller (admin GraphQL, MCP chat) reads
# the API token from — see provisa/api/admin/schema_common.py's _stage_kaggle_if_needed, the single
# choke point (inside create_source itself) that actually enforces staging on every creation path.
KAGGLE_TOKEN_SECRET_NAME = "kaggle_api_token"


class UnsupportedKaggleDataset(Exception):
    """The dataset bundle contains a file type this v1 connector cannot ingest."""


def _data_dir() -> Path:
    # REQ-1780: same $PROVISA_DATA_DIR-else-~/.provisa convention as
    # provisa/core/env_source_files.py:77 and provisa/federation/engine.py:571.
    return Path(os.environ.get("PROVISA_DATA_DIR") or (Path.home() / ".provisa"))


def _file_names(meta: dict) -> list[str]:
    files = meta.get("datasetFiles") or meta.get("files") or []
    names = [f.get("name") or f.get("fileName") for f in files if isinstance(f, dict)]
    return [n for n in names if n]


def staged_mtime(root: Path) -> float | None:  # REQ-1787
    """Newest mtime among files already staged under *root*, or ``None`` if nothing is staged yet.

    The staged files ARE the record of when this dataset was last fetched -- no separate
    "last refreshed at" needs to be persisted; comparing this against Kaggle's own
    ``lastUpdated`` (client.get_dataset_last_updated) is sufficient to know whether a refresh
    would actually change anything."""
    mtimes = [p.stat().st_mtime for p in root.rglob("*") if p.is_file()]
    return max(mtimes) if mtimes else None


async def stage_dataset(token: str, owner: str, ref: str) -> Path:  # REQ-1780, REQ-1781, REQ-1782
    """Download *owner/ref* and unzip its CSV/Parquet members onto local disk, one file per its
    own subdirectory under ``<data_dir>/kaggle/<owner>/<ref>/<file-stem>/<file-name>``.

    Raises :class:`UnsupportedKaggleDataset` if the bundle's file listing names a SQLite/.db
    file — v1 does not stage or partially stage such a bundle. Idempotent: re-running this is
    the (on-demand, v1) refresh mechanism — each file is overwritten in place.

    Returns the dataset's root staging directory (the parent of every per-file subdirectory),
    ready for :func:`provisa.file_source.crawler.crawl_directory`.
    """
    meta = await get_dataset_metadata(token, owner, ref)
    names = _file_names(meta)

    unsupported = [n for n in names if n.lower().endswith(_UNSUPPORTED_EXTENSIONS)]
    if unsupported:
        raise UnsupportedKaggleDataset(
            f"kaggle dataset {owner}/{ref} contains a SQLite file ({unsupported[0]!r}), not yet "
            f"supported — only CSV/Parquet Kaggle datasets can be added"
        )

    staged_names = [n for n in names if n.lower().endswith(_SUPPORTED_EXTENSIONS)]
    if not staged_names:
        raise UnsupportedKaggleDataset(
            f"kaggle dataset {owner}/{ref} has no CSV or Parquet file to stage (found: {names})"
        )

    bundle = await download_dataset(token, owner, ref)
    root = _data_dir() / "kaggle" / owner / ref
    root.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(bundle)) as zf:
        for name in staged_names:
            stem = Path(name).stem
            file_dir = root / stem
            file_dir.mkdir(parents=True, exist_ok=True)
            dest = file_dir / Path(name).name
            dest.write_bytes(zf.read(name))

    return root
