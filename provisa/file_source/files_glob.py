# Copyright (c) 2026 Kenneth Stott
# Canary: 7482cd5f-1df2-4b1b-9351-1aa5390bf0bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A files-source table that is one logical table over a glob of files (REQ-788).

A table declares ``file_glob`` and is read as one relation spanning every file the glob matches
under the source's path. The matched files must share a column set; a file that differs is
refused by name, never silently null-filled. Discovery proposes such a group (files under the
source's path that share a column set); registration is where the operator accepts it.
"""

# Requirements: REQ-788

from __future__ import annotations

from collections.abc import Iterable, Sequence


class FileColumnsDiffer(ValueError):
    """Two files matched by one ``file_glob`` do not share a column set (REQ-788)."""

    code = "schema.file_columns_differ"

    def __init__(self, glob: str, file: str, missing: Sequence[str], extra: Sequence[str]) -> None:
        self.params = {
            "glob": glob,
            "file": file,
            "missing": ", ".join(missing),
            "extra": ", ".join(extra),
        }
        detail = []
        if missing:
            detail.append(f"missing {', '.join(missing)}")
        if extra:
            detail.append(f"extra {', '.join(extra)}")
        super().__init__(
            f"file {file!r} matched by glob {glob!r} has a different column set "
            f"({'; '.join(detail)}); every file in a files-glob table must have the same columns"
        )


def column_set(columns: Iterable[str]) -> frozenset[str]:
    """The comparable column set of one file: its column names, order-free."""
    return frozenset(columns)


def require_same_columns(glob: str, per_file: dict[str, Sequence[str]]) -> list[str]:
    """Check every matched file shares the first file's column set; raise
    :class:`FileColumnsDiffer` naming the first file that differs. Returns the agreed columns in
    the first file's order. ``per_file`` maps each matched file path to its column names; it must
    be non-empty (a glob that matches nothing is the caller's own error, not this one)."""
    if not per_file:
        raise ValueError(f"glob {glob!r} matched no files")
    files = list(per_file)
    first = files[0]
    agreed = list(per_file[first])
    agreed_set = column_set(agreed)
    for path in files[1:]:
        this = column_set(per_file[path])
        if this != agreed_set:
            raise FileColumnsDiffer(
                glob,
                path,
                missing=sorted(agreed_set - this),
                extra=sorted(this - agreed_set),
            )
    return agreed
