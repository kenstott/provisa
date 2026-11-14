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

from collections.abc import Callable, Iterable, Mapping, Sequence


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
        raise ValueError(f"file {path!r} has no supported file-source type")
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


def propose_glob_groups(discovered: list[dict], root: str) -> list[dict]:
    """Group crawled files that share a column set into candidate glob tables (REQ-788).

    ``discovered`` is :func:`provisa.file_source.crawler.crawl_directory`'s output (one descriptor
    per file). Files in the same directory with the same extension and the same column set are
    proposed as ONE logical table: ``{glob, columns, type, files}`` with the glob relative to
    ``root`` (what a table's ``file_glob`` is evaluated against). Only a group of two or more is
    proposed — a lone file is registered as itself, not a glob. The operator accepts a proposal;
    nothing is auto-registered (registration is the curation)."""
    import os

    groups: dict[tuple[str, str, frozenset[str]], dict] = {}
    for entry in discovered:
        path = entry["path"]
        tables = entry.get("tables") or []
        if len(tables) != 1:  # a multi-table file (e.g. sqlite) is not a glob member
            continue
        cols = [c["name"] for c in tables[0].get("columns", [])]
        if not cols:
            continue
        ext = os.path.splitext(path)[1].lower().lstrip(".")
        reldir = os.path.dirname(os.path.relpath(path, root)) if root else os.path.dirname(path)
        key = (reldir, ext, frozenset(cols))
        group = groups.setdefault(
            key,
            {
                "glob": f"{reldir}/*.{ext}" if reldir and reldir != "." else f"*.{ext}",
                "columns": cols,
                "type": entry.get("type"),
                "files": [],
            },
        )
        group["files"].append(path)
    return [g for g in groups.values() if len(g["files"]) >= 2]


def glob_freshness_token(stats: list[tuple[str, float, int]]) -> str:
    """A change token for a files-glob table from its matched files' stats (REQ-788): the file
    count, the latest mtime and the total size. A new, removed, grown or re-written file moves
    the token, so the replica is rebuilt; it is the files-glob analogue of the REQ-855 probe."""
    count = len(stats)
    max_mtime = max((m for _, m, _ in stats), default=0.0)
    total = sum(sz for _, _, sz in stats)
    return f"{count}:{max_mtime:.6f}:{total}"


def stat_matched_files(source_path: str, file_glob: str) -> list[tuple[str, float, int]]:
    """``(path, mtime, size)`` for each file the glob matches, sorted — the input to
    :func:`glob_freshness_token`. A local path is stat'd directly; an fsspec URI through its
    filesystem's ``info``."""
    import os

    files = matched_files(source_path, file_glob)
    if "://" in source_path and not source_path.startswith("file://"):
        import fsspec

        fs, _, _ = fsspec.get_fs_token_paths(source_path)
        out = []
        for f in files:
            info = fs.info(f)
            out.append((f, float(info.get("mtime", 0) or 0), int(info.get("size", 0) or 0)))
        return out
    return [(f, os.path.getmtime(f), os.path.getsize(f)) for f in files]


# --- Combining files into one table (REQ-788, COMBINING FILES IS ITS OWN ACTION) -------------

#: Compression suffixes the file adapter strips before a file's own extension.
_COMPRESSED = (".gz", ".bz2", ".xz", ".zst")


def relative_to_base(source_path: str, path: str) -> str:
    """``path`` (one of :func:`matched_files`' results) relative to the directory a table's
    ``file_glob`` is evaluated under, with ``/`` separators: what the glob itself names."""
    import os

    if "://" in source_path and not source_path.startswith("file://"):
        base = source_path if source_path.endswith("/") else source_path.rsplit("/", 1)[0] + "/"
        return path[len(base) :] if path.startswith(base) else path.rsplit("/", 1)[-1]
    base = source_path if os.path.isdir(source_path) else os.path.dirname(source_path)
    return os.path.relpath(path, base).replace(os.sep, "/")


def table_key(name: str) -> str:
    """A listed table's name or a file's stem, compared case- and punctuation-free."""
    import re

    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def file_keys(rel: str) -> set[str]:
    """The table names a file may be listed under: its stem, alone or with its directories."""
    lowered = rel
    for suffix in _COMPRESSED:
        if lowered.lower().endswith(suffix):
            lowered = lowered[: -len(suffix)]
    stem = lowered.rsplit(".", 1)[0] if "." in lowered.rsplit("/", 1)[-1] else lowered
    base = lowered.rsplit("/", 1)[-1]
    # A listing may name a file by its stem or by its whole name, alone or with its directories.
    return {
        table_key(stem),
        table_key(stem.rsplit("/", 1)[-1]),
        table_key(lowered),
        table_key(base),
    }


def column_agreement(
    per_file: Mapping[str, Sequence[str]],
) -> tuple[list[str], list[dict]]:
    """The columns every file carries (in the reference file's order) and each file whose column
    set differs from the reference, with what it lacks and what it adds. The reference is the
    column set most files carry (the first file's on a tie), so the odd one out is the one named."""
    if not per_file:
        return [], []
    sets = {path: column_set(cols) for path, cols in per_file.items()}
    counts: dict[frozenset[str], int] = {}
    for s in sets.values():
        counts[s] = counts.get(s, 0) + 1
    reference = max(counts, key=lambda s: (counts[s], -list(sets.values()).index(s)))
    ref_file = next(path for path, s in sets.items() if s == reference)
    everywhere = frozenset.intersection(*sets.values())
    shared = [c for c in per_file[ref_file] if c in everywhere]
    differing = [
        {
            "file": path,
            "missing": sorted(reference - s),
            "extra": sorted(s - reference),
        }
        for path, s in sets.items()
        if s != reference
    ]
    return shared, differing


def _common_prefix(names: list[str]) -> str:
    import os

    return os.path.commonprefix(names)


def _common_suffix(names: list[str]) -> str:
    import os

    return os.path.commonprefix([n[::-1] for n in names])[::-1]


def glob_candidates(picked: list[str]) -> list[str]:
    """Globs (in :mod:`glob`'s dialect, relative to the source directory) that match every picked
    file, most readable first: one name; a common prefix and suffix around ``*``; and, for names
    of one length, each differing position as a character class."""
    import glob as _glob
    import posixpath

    dirs = {posixpath.dirname(p) for p in picked}
    if len(dirs) == 1:
        only = dirs.pop()
        lead = _glob.escape(only) + "/" if only else ""
    else:
        common = posixpath.commonpath(list(dirs)) if all(dirs) else ""
        lead = (_glob.escape(common) + "/" if common else "") + "**/"
    names = sorted({posixpath.basename(p) for p in picked})
    if len(names) == 1:
        return [lead + _glob.escape(names[0])]
    out = []
    prefix = _common_prefix(names)
    suffix = _common_suffix(names)
    shortest = min(len(n) for n in names)
    suffix = suffix[max(0, len(prefix) + len(suffix) - shortest) :]
    out.append(lead + _glob.escape(prefix) + "*" + _glob.escape(suffix))
    if len({len(n) for n in names}) == 1:
        parts = []
        for i in range(len(names[0])):
            chars = sorted({n[i] for n in names})
            if len(chars) == 1:
                parts.append(_glob.escape(chars[0]))
            elif any(c in "[]!-^\\" for c in chars):
                parts = []
                break
            else:
                parts.append("[" + "".join(chars) + "]")
        if parts:
            out.append(lead + "".join(parts))
    return out


def derive_glob(picked: list[str], matches: "Callable[[str], list[str]]") -> tuple[str, list[str]]:
    """The narrowest glob over the ``picked`` files (relative paths) and the files it would match
    besides them. ``matches`` evaluates a glob as the server reads it (relative results). The first
    candidate that matches exactly the picked set wins; else the one adding the fewest files."""
    wanted = set(picked)
    best: tuple[str, list[str]] | None = None
    for glob in glob_candidates(picked):
        found = set(matches(glob))
        if not wanted <= found:
            continue
        extra = sorted(found - wanted)
        if not extra:
            return glob, []
        if best is None or len(extra) < len(best[1]):
            best = (glob, extra)
    if best is None:
        raise ValueError(f"no glob matches every picked file: {', '.join(sorted(picked))}")
    return best
