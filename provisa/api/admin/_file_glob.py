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
        FileOutsideSourceGlob,
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
    except (FileColumnsDiffer, FileOutsideSourceGlob) as exc:
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


async def table_outside_source_glob_refusal(conn: Any, model: Any) -> MutationResult | None:
    """A failing MutationResult when ``model`` is a table of a files source whose path is a glob
    and the source does not offer it: its file is outside the source's glob, so it is not a
    table of the source (REQ-788). The tables offered are the source's own listing, the one the
    Register Table form shows. A ``file_glob`` table is checked by :func:`table_file_glob_refusal`;
    another source, or a path that is no glob, is not checked here."""
    if getattr(model, "file_glob", None):
        return None
    from provisa.core import org_files
    from provisa.core.repositories import source as source_repo
    from provisa.core.secrets import resolve_secrets
    from provisa.file_source.files_glob import split_source_path

    source = await source_repo.get(conn, model.source_id)
    if source is None or source["type"] != "files" or not source["path"]:
        return None
    path = org_files.resolve_path(resolve_secrets(source["path"]))
    if split_source_path(path)[1] is None:
        return None
    from provisa.api.admin.schema_query import offered_tables

    offered = {t.name for t in await offered_tables(model.source_id, model.schema_name)}
    if model.table_name in offered:
        return None
    return MutationResult(
        success=False,
        message=(
            f"table {model.table_name!r} is not a table of source {model.source_id!r}: its file "
            f"is outside the source's glob {source['path']!r}"
        ),
        code="schema.table_outside_source_glob",
        params={"table": model.table_name, "source": model.source_id, "glob": source["path"]},
    )
