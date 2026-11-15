# Copyright (c) 2026 Kenneth Stott
# Canary: f35d19f4-6fbb-42b3-9795-fa388b732f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The save-time refusal of a files-glob table whose files do not share a column set (REQ-788).

The config load applies the same rule (config_loader._validate_file_globs); this is its
admin-mutation counterpart, so registering or updating a glob table through the API is refused by
name exactly as a config load is."""

# Requirements: REQ-788

from __future__ import annotations

import logging
from typing import Any

from provisa.api.admin.types import MutationResult

log = logging.getLogger(__name__)


async def table_file_glob_refusal(conn: Any, model: Any) -> MutationResult | None:
    """A failing MutationResult when ``model`` declares ``file_glob`` and its source's matched
    files do not share a column set (schema.file_columns_differ), the glob matches nothing, or the
    source is not a files source; None otherwise."""
    glob = getattr(model, "file_glob", None)
    if not glob:
        return None
    from provisa.core.repositories import source as source_repo
    from provisa.core.secrets import resolve_secrets
    from provisa.file_source.files_glob import (
        FileColumnsDiffer,
        columns_of_file,
        matched_files,
        validate_glob_table,
    )

    source = await source_repo.get(conn, model.source_id)
    if source is None or source["type"] not in ("files", "csv", "parquet"):
        return MutationResult(
            success=False,
            message=f"table {model.table_name!r}: file_glob is defined only for a files source, "
            f"not {(source or {}).get('type')!r}",
            code="schema.file_glob_not_files_source",
            params={"table": model.table_name, "source": model.source_id},
        )
    try:
        files = matched_files(resolve_secrets(source["path"] or ""), glob)
        validate_glob_table(glob, files, columns_of_file)
    except FileColumnsDiffer as exc:
        return MutationResult(
            success=False, message=str(exc), code=exc.code, params=dict(exc.params)
        )
    except (ValueError, OSError) as exc:
        return MutationResult(
            success=False,
            message=f"table {model.table_name!r}: {exc}",
            code="schema.file_glob_unreadable",
            params={"table": model.table_name, "source": model.source_id, "error": str(exc)},
        )
    return None


def _files_of(source_path: str, glob: str) -> list[str]:
    """The readable files ``glob`` matches under the source's directory (directories and files
    of no file-source type left out), as full paths."""
    import os

    from provisa.file_source.crawler import _source_type_for_path
    from provisa.file_source.files_glob import matched_files

    remote = "://" in source_path and not source_path.startswith("file://")
    if not remote and not os.path.exists(source_path) and not any(c in source_path for c in "*?["):
        # A source path that is not there holds no file: the directory above it is never walked.
        return []
    return [
        f
        for f in matched_files(source_path, glob)
        if _source_type_for_path(f) is not None and (remote or os.path.isfile(f))
    ]


def file_glob_preview(
    source_path: str, glob: str | None, tables: list[str] | None
) -> dict[str, Any]:
    """What one table over many files of a files source would be, before it is registered
    (REQ-788, COMBINING FILES IS ITS OWN ACTION): the glob (as typed, or derived from the listed
    ``tables`` picked), the files it matches (relative to the source directory, each with the
    listed table it is), the columns they all share, the files whose columns differ, and the
    files a derived glob matches besides the picked ones. A glob matching nothing answers no
    files; a picked table naming no file is answered in ``unmatched_tables``."""
    from provisa.file_source.files_glob import (
        column_agreement,
        columns_of_file,
        derive_glob,
        file_keys,
        relative_to_base,
        table_key,
    )

    def rel(paths: list[str]) -> dict[str, str]:
        return {relative_to_base(source_path, p): p for p in paths}

    table_of: dict[str, str] = {}
    unmatched: list[str] = []
    extra: list[str] = []
    if glob:
        matched = rel(_files_of(source_path, glob))
    else:
        every = rel(_files_of(source_path, "**/*"))
        picked: list[str] = []
        for name in tables or []:
            hits = [r for r in every if table_key(name) in file_keys(r)]
            if not hits:
                unmatched.append(name)
                continue
            picked.append(hits[0])
            table_of[hits[0]] = name
        if unmatched or not picked:
            return {
                "glob": "",
                "files": [],
                "shared_columns": [],
                "differing": [],
                "extra_files": [],
                "unmatched_tables": unmatched,
                "reference_table": None,
            }
        glob, extra = derive_glob(picked, lambda g: list(rel(_files_of(source_path, g))))
        matched = rel(_files_of(source_path, glob))
    for r in matched:
        if r not in table_of:
            table_of[r] = next(
                (name for name in tables or [] if table_key(name) in file_keys(r)),
                sorted(file_keys(r), key=len)[0],
            )
    per_file: dict[str, list[str]] = {}
    unreadable: list[dict] = []
    for r, full in matched.items():
        try:
            per_file[r] = columns_of_file(full)
        except (ValueError, OSError) as exc:
            unreadable.append({"file": r, "missing": [], "extra": [], "error": str(exc)})
    shared, differing = column_agreement(per_file)
    differing_files = {d["file"] for d in differing}
    reference = next((r for r in per_file if r not in differing_files), None)
    return {
        "glob": glob,
        "files": [{"file": r, "table": table_of[r]} for r in matched],
        "shared_columns": shared,
        "differing": [{**d, "error": None} for d in differing] + unreadable,
        "extra_files": extra,
        "unmatched_tables": [],
        "reference_table": table_of[reference] if reference else None,
    }


async def source_files_path(conn: Any, source_id: str) -> str:
    """A files source's path, its secrets resolved; refused by name for any other source."""
    from provisa.core.repositories import source as source_repo
    from provisa.core.secrets import resolve_secrets

    source = await source_repo.get(conn, source_id)
    if source is None or source["type"] not in ("files", "csv", "parquet"):
        raise ValueError(
            f"source {source_id!r}: files are combined into one table only on a files source"
        )
    return resolve_secrets(source["path"] or "")
