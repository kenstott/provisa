# Copyright (c) 2026 Kenneth Stott
# Canary: f1e2d3c4-b5a6-7890-cdef-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Directory crawler for file-based sources (Issue #28).

Recursively walks a directory (local or fsspec URI) and introspects all
supported files: .csv, .parquet, .sqlite, .db

Returns a list of discovered table descriptors ready for bulk registration.
"""

from __future__ import annotations

import fnmatch
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from provisa.file_source.source import FileSourceConfig, discover_schema

# Requirements: REQ-012, REQ-016, REQ-250, REQ-252

log = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS: dict[str, str] = {
    ".csv": "csv",
    ".parquet": "parquet",
    ".sqlite": "sqlite",
    ".db": "sqlite",
}


def _source_type_for_path(path: str) -> str | None:
    """Return source_type string for a file path, or None if unsupported."""
    ext = Path(path).suffix.lower()
    return SUPPORTED_EXTENSIONS.get(ext)


def _is_fsspec_uri(path: str) -> bool:
    """Return True if path looks like an fsspec URI (e.g. s3://, ftp://)."""
    return "://" in path and not path.startswith("file://")


def _walk_local(root: str, max_depth: int | None) -> list[str]:
    """Walk a local directory and return matching file paths."""
    root_path = Path(root)
    if not root_path.is_dir():
        raise ValueError(f"Not a directory: {root!r}")

    results: list[str] = []
    _walk_local_recursive(root_path, root_path, 0, max_depth, results)
    return results


def _walk_local_recursive(
    root: Path,
    current: Path,
    depth: int,
    max_depth: int | None,
    results: list[str],
) -> None:
    if max_depth is not None and depth > max_depth:
        return
    for entry in sorted(current.iterdir()):
        if entry.is_file() and entry.suffix.lower() in SUPPORTED_EXTENSIONS:
            results.append(str(entry))
        elif entry.is_dir():
            _walk_local_recursive(root, entry, depth + 1, max_depth, results)


def _walk_fsspec(
    root: str,
    max_depth: int | None,
    *,
    simple_links: bool = True,
    same_domain: bool = True,
    exclude_pattern: str | None = None,
) -> list[str]:
    """Walk an fsspec URI and return matching file paths.

    ``simple_links``, ``same_domain``, and ``exclude_pattern`` are only meaningful for
    ``http(s)://`` roots (REQ-1785): ``simple_links`` is passed to
    ``fsspec.implementations.http.HTTPFileSystem`` directly (loose regex link matching
    vs. real ``<a href>`` anchors only); ``same_domain``/``exclude_pattern`` have no
    native fsspec equivalent since plain HTTP has no directory concept — an unrestricted
    crawl can otherwise wander into off-target subpaths/domains via arbitrary page links,
    unlike a local/S3/FTP walk which is structurally confined to its root.
    """
    import fsspec  # pyright: ignore[reportMissingImports]

    fs, base_path = fsspec.core.url_to_fs(root, simple_links=simple_links)
    all_files: list[str] = []
    root_netloc = urlparse(root).netloc
    _walk_fsspec_recursive(
        fs,
        base_path,
        base_path,
        0,
        max_depth,
        all_files,
        root,
        same_domain=same_domain,
        root_netloc=root_netloc,
        exclude_pattern=exclude_pattern,
    )
    return all_files


def _walk_fsspec_recursive(
    fs: Any,
    base: str,
    current: str,
    depth: int,
    max_depth: int | None,
    results: list[str],
    uri_prefix: str,
    *,
    same_domain: bool = True,
    root_netloc: str = "",
    exclude_pattern: str | None = None,
) -> None:
    if max_depth is not None and depth > max_depth:
        return

    protocol = uri_prefix.split("://")[0]

    for entry in fs.ls(current, detail=True):
        entry_path: str = entry["name"]
        entry_type: str = entry.get("type", "")
        entry_uri = f"{protocol}://{entry_path}"

        if same_domain and root_netloc and urlparse(entry_uri).netloc != root_netloc:
            continue
        if exclude_pattern is not None and fnmatch.fnmatch(Path(entry_path).name, exclude_pattern):
            continue

        if entry_type == "file":
            suffix = Path(entry_path).suffix.lower()
            if suffix in SUPPORTED_EXTENSIONS:
                results.append(entry_uri)
        elif entry_type == "directory":
            _walk_fsspec_recursive(
                fs,
                base,
                entry_path,
                depth + 1,
                max_depth,
                results,
                uri_prefix,
                same_domain=same_domain,
                root_netloc=root_netloc,
                exclude_pattern=exclude_pattern,
            )


def _introspect_file(file_path: str, source_type: str) -> list[dict]:  # REQ-252
    """Call discover_schema for the given file; return columns list."""
    cfg = FileSourceConfig(
        id=f"_crawl_{Path(file_path).stem}",
        source_type=source_type,
        path=file_path,
    )
    return discover_schema(cfg)


def _build_table_entry(file_path: str, source_type: str, columns: list[dict]) -> dict:
    """Build a single discovered table descriptor."""
    stem = Path(file_path).stem
    if source_type == "sqlite":
        # SQLite may have multiple tables; columns include "table" key
        by_table: dict[str, list[dict]] = {}
        for col in columns:
            tbl = col.get("table", stem)
            by_table.setdefault(tbl, []).append(
                {
                    "name": col["name"],
                    "type": col["type"],
                    "nullable": col.get("nullable", True),
                }
            )
        return {
            "name": stem,
            "path": file_path,
            "type": source_type,
            "tables": [{"name": tbl, "columns": cols} for tbl, cols in by_table.items()],
        }
    else:
        return {
            "name": stem,
            "path": file_path,
            "type": source_type,
            "tables": [
                {
                    "name": stem,
                    "columns": [
                        {
                            "name": c["name"],
                            "type": c["type"],
                            "nullable": c.get("nullable", True),
                        }
                        for c in columns
                    ],
                }
            ],
        }


def crawl_directory(
    root: str,
    depth: int | None = None,
    *,
    pattern: str | None = None,
    recursive: bool = True,
    simple_links: bool = True,
    same_domain: bool = True,
    exclude_pattern: str | None = None,
) -> list[dict]:  # REQ-012, REQ-016, REQ-250, REQ-788, REQ-1785
    """Crawl *root* and return discovered table descriptors.

    Parameters
    ----------
    root:
        Local filesystem path or fsspec URI (e.g. ``s3://bucket/prefix/``).
    depth:
        Maximum recursion depth. ``None`` means unlimited.
    pattern:
        Optional filename glob (e.g. ``"*.csv"``) applied to each discovered
        file's basename. ``None`` matches every supported file.
    recursive:
        When ``False``, only the top-level directory is scanned (equivalent to
        ``depth=0``). When ``True`` (default), subdirectories are walked up to
        ``depth``.
    simple_links, same_domain, exclude_pattern:
        HTTP-crawl-only settings (REQ-1785), ignored for local/S3/FTP/SFTP roots.
        ``simple_links`` (default True) controls whether ``HTTPFileSystem`` matches
        any ``http(s)://`` URL-shaped text on the page or only real ``<a href>``
        anchors. ``same_domain`` (default True) restricts link-following to the
        root URL's own domain. ``exclude_pattern`` is an fnmatch glob applied to
        each linked entry's basename to skip known-irrelevant subpaths/files.

    Returns
    -------
    list of dicts, each containing::

        {
            "name": str,          # file stem
            "path": str,          # absolute path / URI
            "type": str,          # "csv" | "parquet" | "sqlite"
            "tables": [
                {
                    "name": str,
                    "columns": [{"name": str, "type": str, "nullable": bool}]
                }
            ]
        }

    Raises ``ValueError`` if *root* is not a directory.
    Raises ``OSError`` / fsspec errors for inaccessible paths.
    """
    effective_depth = 0 if not recursive else depth

    if _is_fsspec_uri(root):
        file_paths = _walk_fsspec(
            root,
            effective_depth,
            simple_links=simple_links,
            same_domain=same_domain,
            exclude_pattern=exclude_pattern,
        )
    else:
        file_paths = _walk_local(root, effective_depth)

    results: list[dict] = []
    for fp in file_paths:
        source_type = _source_type_for_path(fp)
        if source_type is None:
            continue
        if pattern is not None and not fnmatch.fnmatch(Path(fp).name, pattern):
            continue
        columns = _introspect_file(fp, source_type)
        entry = _build_table_entry(fp, source_type, columns)
        results.append(entry)
        log.debug("Crawled %s → %d table(s)", fp, len(entry["tables"]))

    return results
