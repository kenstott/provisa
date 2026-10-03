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
            f"not {(source or {}).get('type')!r} (REQ-788)",
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
