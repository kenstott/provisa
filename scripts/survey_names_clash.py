# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Offline survey (REQ-194): which shipped models would the names table refuse under the default
clash policy. Builds each config's whole-model context from its DECLARED tables and columns (no
server, no source introspection), then runs the names-table build. A table with no declared
columns is counted and left out: its columns come from introspection.

usage: survey_names_clash.py [config.yaml ...]   (default: every shipped config)
exit 1 when any model is refused or fails to load."""

from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from provisa.compiler import naming  # noqa: E402
from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.names_table import (  # noqa: E402
    NamesClashRefused,
    build_model_names,
)
from provisa.compiler.schema_types import SchemaInput  # noqa: E402
from provisa.executor.write_capability import table_write_ops  # noqa: E402
from provisa.core.config_loader import parse_config_dict, read_config_with_includes  # noqa: E402

_ENV = re.compile(r"\$\{env:([A-Za-z0-9_]+)\}")
_SECRET = re.compile(r"\$\{secret:([A-Za-z0-9_.-]+)\}")


def default_configs() -> list[Path]:
    found = sorted((ROOT / "demo/named").glob("*/config.yaml"))
    # capabilities, custom_connectors and pg_extension_catalog are catalogs, not models.
    found += [
        p
        for p in sorted((ROOT / "config").glob("*.yaml"))
        if p.name
        not in {"capabilities.yaml", "custom_connectors.yaml", "pg_extension_catalog.yaml"}
    ]
    return found


def _declare_env(path: Path) -> None:
    """A model names its environment values as ${env:X}; the survey needs only that they exist."""
    for name in _ENV.findall(path.read_text()):
        url = name.endswith(("_URL", "_DSN"))
        os.environ.setdefault(
            name, "postgresql://survey:survey@localhost:5432/survey" if url else "survey"
        )


def model_input(cfg) -> tuple[SchemaInput, int]:
    domains = [d.model_dump() for d in cfg.domains]
    tables, col_types, skipped = [], {}, 0
    ids: dict[str, int] = {}
    sources = {s.id: (s.type.value if hasattr(s.type, "value") else s.type) for s in cfg.sources}
    for tbl in cfg.tables:
        if not tbl.columns:
            skipped += 1
            continue
        tid = len(tables) + 1
        ids[tbl.table_name] = tid
        row = tbl.model_dump()
        row.update(id=tid, schema_name=tbl.schema_name, table_name=tbl.table_name)
        row["columns"] = [{**c.model_dump(), "column_name": c.name} for c in tbl.columns]
        stype = sources.get(tbl.source_id)
        # The engine route is not considered (no engine is started): a source writable only
        # through the engine gets no mutation names here.
        row["write_ops"] = sorted(
            table_write_ops(row, None if tbl.view_sql else stype, None, source_path=None)
        )
        row["write_returns_rows"] = False
        row["write_refused_forms"] = []
        tables.append(row)
        col_types[tid] = [
            ColumnMetadata(c.name, (c.data_type or "varchar"), True) for c in tbl.columns
        ]
    rels = []
    for r in cfg.relationships:
        src, tgt = ids.get(r.source_table_id), ids.get(r.target_table_id)
        if src is None or (tgt is None and not r.target_function_name):
            continue
        rels.append(
            {
                **r.model_dump(),
                "source_table_id": src,
                "target_table_id": tgt,
                "cardinality": r.cardinality.value,
                "via_table_id": ids.get(r.via_table) if r.via_table else None,
            }
        )
    role = {"id": "__model__", "domain_access": ["*"], "capabilities": []}
    for t in tables:
        for c in t["columns"]:
            c["visible_to"] = [role["id"]]
    si = SchemaInput(
        tables=tables,
        relationships=rels,
        column_types=col_types,
        naming_rules=[n.model_dump() for n in cfg.naming.rules],
        role=role,
        domains=domains,
        domain_prefix=cfg.naming.domain_prefix,
        source_types={
            s.id: s.type.value if hasattr(s.type, "value") else s.type for s in cfg.sources
        },
        functions=[f.model_dump() for f in cfg.functions],
        webhooks=[w.model_dump() for w in cfg.webhooks],
    )
    return si, skipped


def survey(path: Path) -> dict:
    _declare_env(path)
    from provisa.core.secrets_store import bound_to_values

    text = path.read_text()
    with bound_to_values("survey", {n: "survey" for n in _SECRET.findall(text)}):
        cfg = parse_config_dict(read_config_with_includes(path))
    naming.configure(gql=cfg.naming.convention, sql=cfg.naming.sql_convention)
    si, skipped = model_input(cfg)
    ctx = build_context(si)
    out = {"tables": len(si.tables), "skipped": skipped, "clashes": [], "warnings": []}
    try:
        table = build_model_names(ctx, si, "refuse")
        out["warnings"] = table.warnings
    except NamesClashRefused as refused:
        out["clashes"] = refused.clashes
        try:
            out["warnings"] = build_model_names(ctx, si, "disambiguate").warnings
        except NamesClashRefused as again:
            out["warnings"] = []
            out["unfreezable"] = len(again.clashes)
    return out


def main(argv: list[str]) -> int:
    paths = [Path(a) for a in argv] or default_configs()
    bad = 0
    for p in paths:
        label = str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p)
        try:
            r = survey(p)
        except Exception as exc:  # survey reports every model's failure, then exits nonzero
            bad += 1
            print(f"{label}\tLOAD-FAIL\t{type(exc).__name__}: {str(exc)[:200]}")
            continue
        kinds = defaultdict(int)
        for w in r["warnings"]:
            kinds[w.code] += 1
        head = f"{label}\t{'REFUSED' if r['clashes'] else 'PASS'}\ttables={r['tables']}"
        print(
            f"{head} undeclared_tables={r['skipped']} warnings={len(r['warnings'])} {dict(kinds)}"
        )
        for c in r["clashes"]:
            bad += 1
            print(f"    CLASH {c.surface}: {c.name!r}  {c.newcomer}  vs  {c.holder}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
