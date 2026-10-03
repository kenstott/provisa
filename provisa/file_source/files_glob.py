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

from collections.abc import Callable, Iterable, Sequence


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


def matched_files(source_path: str, file_glob: str) -> list[str]:
    """The files a table's ``file_glob`` matches, sorted. ``file_glob`` is evaluated under the
    source's directory: the directory of ``source_path`` when it names a file or is itself a
    glob, else ``source_path`` itself when it is a directory. A local path uses :mod:`glob`; an
    fsspec URI (``s3://`` …) uses fsspec's own globbing. The match is the caller's to act on —
    an empty match is not raised here."""
    import glob as _glob
    import os

    if "://" in source_path and not source_path.startswith("file://"):
        import fsspec

        base = source_path.rsplit("/", 1)[0] if not source_path.endswith("/") else source_path[:-1]
        fs, _, _ = fsspec.get_fs_token_paths(base)
        proto = source_path.split("://", 1)[0]
        return sorted(f"{proto}://{m}" for m in fs.glob(f"{base}/{file_glob}"))
    base = source_path if os.path.isdir(source_path) else os.path.dirname(source_path)
    return sorted(_glob.glob(os.path.join(base, file_glob), recursive=True))


def validate_glob_table(
    glob: str, files: list[str], columns_of: "Callable[[str], Sequence[str]]"
) -> list[str]:
    """The agreed columns of a files-glob table, or raise. ``columns_of`` introspects one file's
    column names. A glob that matches no file is a configuration error named here (REQ-788)."""
    if not files:
        raise ValueError(
            f"glob {glob!r} matched no files; a files-glob table must match at least one"
        )
    return require_same_columns(glob, {path: list(columns_of(path)) for path in files})


def columns_of_file(path: str) -> list[str]:
    """One file's column names, as the file-source introspector reads them (REQ-788). The file's
    own extension picks the reader (csv/parquet/sqlite), not the source's umbrella ``files`` type."""
    from provisa.file_source.crawler import _source_type_for_path
    from provisa.file_source.source import FileSourceConfig, discover_schema

    file_type = _source_type_for_path(path)
    if file_type is None:
        raise ValueError(f"file {path!r} has no supported file-source type (REQ-788)")
    cfg = FileSourceConfig(id="_glob", source_type=file_type, path=path)
    return [col["name"] for col in discover_schema(cfg)]


#: File extensions DuckDB reads as one relation over a list, by reader function.
_GLOB_READER = {"csv": "read_csv", "parquet": "read_parquet"}


def _glob_reader_fn(files: list[str]) -> str:
    """The DuckDB table function that reads ``files`` as one relation; raises when the matched
    files are not one homogeneous readable kind (REQ-788)."""
    import os

    exts = {os.path.splitext(f)[1].lower().lstrip(".") for f in files}
    kinds = {"csv": "csv", "tsv": "csv", "txt": "csv", "parquet": "parquet", "pq": "parquet"}
    families = {kinds.get(e) for e in exts}
    if families == {"csv"}:
        return "read_csv"
    if families == {"parquet"}:
        return "read_parquet"
    raise ValueError(
        f"a files-glob table must match one readable kind (csv or parquet); matched {sorted(exts)}"
    )


def duckdb_glob_relation(
    files: list[str], columns: list[str], source_file_column: str | None
) -> tuple[str, list[str]]:
    """The DuckDB ``SELECT`` that reads ``files`` as one relation, and its parameters. The
    projection is the table's declared columns in order, plus ``source_file_column`` from
    DuckDB's ``filename`` when declared — so the relation's shape is exactly the replica's. CSV
    is read with ``union_by_name=false``: the column-set rule already holds, so a positional
    union would hide a drift the validator refuses by name (REQ-788)."""
    fn = _glob_reader_fn(files)
    placeholders = ", ".join("?" for _ in files)
    opts = "union_by_name=false, filename=true, header=true, auto_detect=true"
    if fn == "read_parquet":
        opts = "filename=true"
    proj = ", ".join(f'"{c}"' for c in columns)
    if source_file_column:
        proj += f', filename AS "{source_file_column}"'
    sql = f"SELECT {proj} FROM {fn}([{placeholders}], {opts})"
    return sql, list(files)
