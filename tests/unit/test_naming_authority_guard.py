# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2b9e41-3a6c-4f80-b1d5-c8e04a9f2b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A check refuses new name shaping outside the naming authority (REQ-2361, REQ-194).

Every product source file (``provisa/**/*.py`` and ``provisa-ui/src/**/*.ts(x)``, tests excluded)
is scanned for the patterns client-facing names were shaped with outside the authority
(``provisa/compiler/naming.py``, ``provisa-ui/src/naming.ts``): ``str.capitalize()``/``.title()``,
local case and inflection helpers, identifier-sanitising and camel/snake regexes, hand-built Cypher
relationship types and ``__``-composed names. A line that shapes an internal (non-client) name says
so on the line or the line above with ``naming-ok: <reason>``.

The sites already in the tree when the check landed are counted per file in ``_BASELINE``. A file
with more sites than its count, or a new file with any, fails: the new site calls the authority, or
the rule is added to the authority first. A file with fewer sites fails too, so the count follows
the tree down and a removed site cannot be silently replaced.
"""

# Requirements: REQ-2361, REQ-194

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]

_PY_AUTHORITY = "provisa/compiler/naming.py"
_TS_AUTHORITY = "provisa-ui/src/naming.ts"

_PRAGMA = re.compile(r"naming-ok:\s*\S")
# .capitalize() inside an f-string of prose (a message with spaces) shapes a sentence, not a name.
_PROSE_CAPITALIZE = re.compile(r"f[\"'][^\"']*\{[^}]*\.capitalize\(\)\}[^\"']* [^\"']*[\"']")

_PY_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\.(capitalize|title)\(\)"), "str.capitalize()/.title() shapes a name"),
    (
        re.compile(
            r"^\s*def\s+_?(to_)?(snake|camel|pascal|kebab)(_case)?\b"
            r"|^\s*def\s+_?\w*(camel|pascal|snake)_to_(snake|camel|pascal)\w*"
            r"|^\s*def\s+_?(singularize|pluralize|upper_snake|slugify_name)\b"
        ),
        "local case/inflection helper",
    ),
    (re.compile(r"^\s*(import inflect|from inflect import)"), "inflection outside the authority"),
    (
        re.compile(r"re\.sub\(\s*r?[\"']\[\^(a-zA-Z0-9_|A-Za-z0-9_|a-z0-9_|A-Za-z_0-9|\\w)\]"),
        "identifier-sanitising regex",
    ),
    (
        re.compile(
            r"r[\"'][^\"']*\(\[A-Z\]\+\)\(\[A-Z\]\[a-z\]\)|r[\"'][^\"']*\(\[a-z0-9\]\)\(\[A-Z\]\)"
        ),
        "camelCase/snake_case conversion regex",
    ),
    (re.compile(r"[\"'](HAS|IS)_[\"']\s*\+|f[\"'](HAS|IS)_\{"), "Cypher relationship type by hand"),
    (re.compile(r"f[\"'][^\"']*\{[^}]+\}__\{[^}]+\}"), "name composed with '__'"),
]

_TS_RULES: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"\.charAt\(0\)\.toUpperCase\(\)|\[0\]\.toUpperCase\(\)"),
        "manual capitalisation",
    ),
    (
        re.compile(
            r"\bfunction\s+_?(to)?(Snake|Camel|Pascal|Kebab)(Case)?\b"
            r"|\bconst\s+_?(to)?(snake|camel|pascal|Snake|Camel|Pascal)(Case)?\s*=\s*\("
            r"|\b(camel|pascal|snake)To(Snake|Camel|Pascal)\b\s*[=(]"
        ),
        "local case helper",
    ),
    (
        re.compile(
            r"/\(\[A-Z\]\+\)\(\[A-Z\]\[a-z\]\)/|/\(\[a-z0-9\]\)\(\[A-Z\]\)/|/\(\[a-z\]\)\(\[A-Z\]\)/"
        ),
        "camelCase/snake_case conversion regex",
    ),
    (re.compile(r"\.split\(\s*[\"']__[\"']\s*\)"), "domain prefix stripped by hand"),
    (re.compile(r"\.replace\(\s*/-/g\s*,\s*[\"']_[\"']\s*\)"), "id-to-identifier by hand"),
]

#: The name-shaping sites in each file when the check landed (REQ-2361). Only ever lowered.
_BASELINE: dict[str, int] = {
    "provisa-ui/src/components/SqlModelingModal.tsx": 1,
    "provisa-ui/src/components/relationships/relationshipQueries.ts": 2,
    "provisa-ui/src/pages/data-products/tableExamples.ts": 3,
    "provisa-ui/src/plugins/provisa-tools.tsx": 1,
    "provisa/api/admin/modeling_register.py": 1,
    "provisa/api/admin/schema_mutation_ops.py": 2,
    "provisa/api/airport/server.py": 1,
    "provisa/api/data/materialization.py": 1,
    "provisa/api/metadata_export/odps.py": 2,
    "provisa/api/region_ops_views.py": 1,
    "provisa/compiler/actions_schema.py": 3,
    "provisa/compiler/aggregate_gen.py": 3,
    "provisa/compiler/context.py": 2,
    "provisa/compiler/list_cap.py": 3,
    "provisa/compiler/schema_gen.py": 6,
    "provisa/compiler/sql_gen.py": 2,
    "provisa/compiler/sql_selection.py": 2,
    "provisa/core/glossary.py": 2,
    "provisa/core/relationship_names.py": 1,
    "provisa/cypher/sql_to_cypher.py": 1,
    "provisa/cypher/sql_to_cypher_agg.py": 1,
    "provisa/discovery/fk_introspect.py": 2,
    "provisa/ephemeral/indexes.py": 1,
    "provisa/ephemeral/read.py": 1,
    "provisa/ephemeral/store.py": 1,
    "provisa/executor/explain.py": 1,
    "provisa/federation/backend.py": 1,
    "provisa/federation/clickhouse_runtime.py": 1,
    "provisa/federation/duckdb_runtime.py": 3,
    "provisa/federation/pg_runtime.py": 3,
    "provisa/federation/replica_address.py": 3,
    "provisa/federation/trino_connectors.py": 1,
    "provisa/gql/label_map.py": 2,
    "provisa/gql/translator_helpers.py": 1,
    "provisa/gql/translator_rel.py": 1,
    "provisa/graphql_remote/mapper.py": 3,
    "provisa/grpc/proto_gen.py": 1,
    "provisa/grpc_remote/mapper.py": 1,
    "provisa/hasura_v2/mapper.py": 1,
    "provisa/mv/view_reinject.py": 1,
    "provisa/pgwire/catalog_constraints.py": 3,
    "provisa/security/association_suggester.py": 1,
    "provisa/subscriptions/pg_logical.py": 2,
    "provisa/tools/feature_schema.py": 2,
    "provisa/transpiler/function_macros.py": 1,
}


def _rules_for(rel: str) -> list[tuple[re.Pattern[str], str]] | None:
    if "/__tests__/" in rel or "/e2e/" in rel or rel.endswith((".test.ts", ".test.tsx")):
        return None
    if rel.endswith(".py") and rel.startswith("provisa/") and rel != _PY_AUTHORITY:
        return _PY_RULES
    if rel.endswith((".ts", ".tsx")) and rel.startswith("provisa-ui/src/") and rel != _TS_AUTHORITY:
        return _TS_RULES
    return None


def shaping_sites(rel: str, text: str) -> list[tuple[int, str]]:
    """Each line of ``text`` (the file at repo path ``rel``) that shapes a name outside the
    authority, with the rule it breaks: (line number, why)."""
    rules = _rules_for(rel)
    if rules is None:
        return []
    lines = text.splitlines()
    found: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith(("#", "//", "*")):
            continue
        prev = lines[i - 1] if i else ""
        if _PRAGMA.search(line) or _PRAGMA.search(prev) or _PROSE_CAPITALIZE.search(line):
            continue
        for pattern, why in rules:
            if pattern.search(line):
                found.append((i + 1, why))
                break
    return found


def _product_files() -> list[Path]:
    py = (_ROOT / "provisa").rglob("*.py")
    ui = _ROOT / "provisa-ui" / "src"
    ts = [p for ext in ("*.ts", "*.tsx") for p in ui.rglob(ext) if "node_modules" not in p.parts]
    return [*py, *ts]


def _tree_sites() -> dict[str, list[tuple[int, str]]]:
    sites: dict[str, list[tuple[int, str]]] = {}
    for path in _product_files():
        rel = path.relative_to(_ROOT).as_posix()
        found = shaping_sites(rel, path.read_text(encoding="utf-8"))
        if found:
            sites[rel] = found
    return sites


def test_no_new_name_shaping_outside_the_authority():
    over = {
        rel: found for rel, found in _tree_sites().items() if len(found) > _BASELINE.get(rel, 0)
    }
    assert not over, (
        "name shaping outside the naming authority (REQ-2361): call provisa/compiler/naming.py or "
        "provisa-ui/src/naming.ts (add the rule there first), or mark an internal name "
        "`naming-ok: <reason>`:\n"
        + "\n".join(f"  {rel}:{n}: {why}" for rel, found in over.items() for n, why in found)
    )


def test_the_baseline_follows_the_tree_down():
    tree = {rel: len(found) for rel, found in _tree_sites().items()}
    lower = {rel: (n, tree.get(rel, 0)) for rel, n in _BASELINE.items() if tree.get(rel, 0) < n}
    assert not lower, "lower these _BASELINE counts to the tree's: " + ", ".join(
        f"{rel} {was}->{now}" for rel, (was, now) in lower.items()
    )


def test_a_new_site_is_found_and_a_marked_one_is_not():
    rel = "provisa/compiler/new_module.py"
    assert shaping_sites(rel, "x = name.capitalize()\n") == [(1, _PY_RULES[0][1])]
    assert shaping_sites(rel, 'k = f"{domain}__{field}"\n') == [(1, "name composed with '__'")]
    assert shaping_sites(rel, "x = name.capitalize()  # naming-ok: a log label\n") == []
    assert shaping_sites(rel, "# naming-ok: a log label\nx = name.capitalize()\n") == []
    assert shaping_sites(_PY_AUTHORITY, "x = name.capitalize()\n") == []
    ts = "provisa-ui/src/pages/New.tsx"
    assert shaping_sites(ts, "const t = s[0].toUpperCase() + s.slice(1);\n") == [
        (1, "manual capitalisation")
    ]
    assert shaping_sites(ts, 'const bare = f.split("__")[1];\n') == [
        (1, "domain prefix stripped by hand")
    ]
    assert shaping_sites("provisa-ui/src/pages/__tests__/x.test.tsx", "s[0].toUpperCase()") == []
