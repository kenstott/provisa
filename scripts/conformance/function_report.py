# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1c7e42-a3b8-4f09-8e36-c2f1b07d9a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The catalog function report: a result per engine, no overall count (REQ-2171, EXACT OR
EQUIVALENT and RESULTS ARE BY ENGINE).

function_support.py measures; this says what a measurement is called and writes the Markdown that
fill_docs.py puts on docs/conformance/pgwire.md and overview.md and that the run's summary shows.

Every overload has one result on each engine:

  exact        every call PostgreSQL answered, the engine answered the same, and something was
               compared: an answer, a type and form, or a side effect
  equivalent   the same but for what ``RULES`` allows, each rule a footnote linked from its cells
  not checked  the engine answered every call with PostgreSQL's result type, and nothing was
               compared: no pass, a call that ran proves nothing, and it counts as failed
  by design    skipped by design under a decision of REQ-2167, the group naming it
  refused      Provisa refused a call, naming what it refuses (refused_by_name)
  differs      a wrong answer, or an engine error that is neither an answer nor a named refusal

``not_callable`` (PostgreSQL itself answers no call) and ``not_reached`` (the leg did not get there)
are no result: nothing was compared. ``RESULT_KEYS`` are the keys of each engine's counts in the
merged JSON, which are what a landing floor may read: they do not change without saying so.

Beside them, ``COLUMN_KEYS`` say which passes hold on a column too (``column_results``): an overload
that passes on literals and whose column call fails passes on literals only. No result, floor or
guaranteed list reads them."""

# Requirements: REQ-2171

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pgwire_function_probe as function_probe

#: Each engine's label on the page, in the page's order.
ENGINE_LABELS = {"pg": "Postgres", "duckdb": "DuckDB", "trino": "Trino", "clickhouse": "ClickHouse"}

EXACT = "exact"
EQUIVALENT = "equivalent"
REFUSED = "refused"
DIFFERS = "differs"
NOT_CHECKED = "not_checked"
BY_DESIGN = "by_design"
NOT_CALLABLE = "not_callable"
NOT_REACHED = "not_reached"

#: The keys of ``by_engine.<engine>`` in the merged JSON. ``differs`` is the sum of the two after
#: it; ``refused_by_name`` counts the result ``refused``.
RESULT_KEYS = (
    "passed",
    "skipped_by_design",
    "failed",
    "not_tested",
    "exact",
    "equivalent",
    "not_checked",
    "by_design",
    "refused_by_name",
    "differs",
    "differs_wrong_answer",
    "differs_engine_error",
    "not_callable",
    "not_reached",
    "total",
)

#: The keys of ``by_engine.<engine>`` beside ``RESULT_KEYS``: of an engine's passes, those that pass
#: on a column too and those that pass on literals only, each a count and its overloads, and those
#: with no column call to tell.
COLUMN_KEYS = (
    "pass_literal_and_column",
    "pass_literal_only",
    "pass_no_column_call",
    "pass_literal_and_column_overloads",
    "pass_literal_only_overloads",
)


@dataclass(frozen=True)
class Rule:
    """One way an answer may differ and still pass. ``key`` is the JSON's and the page's anchor."""

    key: str
    title: str
    text: str


JSON_SPACING = "json-spacing"
ENUM_LABEL = "enum-label-text"
ROW_ORDER = "row-order"
NUMBER_VALUE = "number-value"
MODEL_ANSWER = "model-answer"
FLOAT_CLOSE = "float-tolerance"
JSON_KEY_ORDER = "json-key-order"
ARRAY_ORDER = "array-order"
CLOCK_FIELD = "clock-field"
ZONE_PACKAGING = "tzdata-packaging"

RULES = (
    Rule(
        JSON_SPACING,
        "Whitespace in JSON",
        "JSON compares as parsed JSON, so spacing between its tokens is no difference. This covers "
        "a json or jsonb answer and a JSON value inside a record, such as the value `json_each` "
        "returns on DuckDB.",
    ),
    Rule(
        ENUM_LABEL,
        "Enum label sent as text",
        "Provisa holds an enum as text. The label is the same, and the column reports `text` where "
        "PostgreSQL reports the enum's own type oid.",
    ),
    Rule(
        ROW_ORDER,
        "Rows compare as a set",
        "These calls have no `ORDER BY`, so nothing fixes the order of their rows. The engine "
        "returned the same rows in another order.",
    ),
    Rule(
        NUMBER_VALUE,
        "Numbers equal by value",
        "`2.50` and `2.5` are the same number. A number compares by value, so its trailing zeros "
        "and scale are no difference.",
    ),
    Rule(
        MODEL_ANSWER,
        "Answers describe the model",
        "`database_to_xmlschema` and its kin describe the database they run on. Provisa answers "
        "for its model and PostgreSQL for its own catalog, so the contents differ. The result type "
        "matches and both answers are XML.",
    ),
    Rule(
        FLOAT_CLOSE,
        "Floats within one part in 10^14",
        "Math libraries differ in the last digits of a float, so `real` and `double precision` "
        "answers compare within one part in 10^14.",
    ),
    Rule(
        JSON_KEY_ORDER,
        "JSON key order",
        "A JSON object has no key order. The same keys and values in another order are the same "
        "answer.",
    ),
    Rule(
        ARRAY_ORDER,
        "Array built without ORDER BY",
        "Only an `ORDER BY` in the statement fixes the order of an array an aggregate builds. "
        "Without one, its elements compare in any order.",
    ),
    Rule(
        CLOCK_FIELD,
        "A clock's field compared by its form",
        "A field read from the clock, such as `pg_cursor`'s `creation_time`, differs on every "
        "call, so it compares by its type's form; the record's other fields compare by value.",
    ),
    Rule(
        ZONE_PACKAGING,
        "Time zones a distribution packages differently",
        "`pg_timezone_names` lists the pinned upstream IANA tzdata, as upstream PostgreSQL ships "
        "it, and compares by the names both lists hold. A name that differs only through a "
        "distribution's tzdata packaging is skipped by design (time zones follow upstream "
        "tzdata): Debian drops the backward links, a link Provisa "
        "lists whose zone both list, and adds `localtime` and `posixrules`.",
    ),
)

_NUMBERS = {rule.key: i + 1 for i, rule in enumerate(RULES)}


def result_of(outcome: str, rules: Iterable[str] = ()) -> str:
    """The result of one engine's ``outcome`` (function_support.OUTCOMES)."""
    if outcome == "pass":
        return EQUIVALENT if tuple(rules) else EXACT
    if outcome in (NOT_CHECKED, BY_DESIGN):
        return outcome
    if outcome == "refused":
        return REFUSED
    if outcome in ("wrong_answer", "error"):
        return DIFFERS
    if outcome in (NOT_CALLABLE, NOT_REACHED):
        return outcome
    raise ValueError(f"no result for outcome {outcome!r}")


PASSED = "passed"
SKIPPED_BY_DESIGN = "skipped_by_design"
FAILED = "failed"
NOT_TESTED = "not_tested"


def verdict(entry: dict[str, Any]) -> str:
    """An overload's one verdict on an engine: passed, skipped by design, failed or not tested
    (REQ-2167). A pass is passed. A decision's group is skipped by design, and so is an overload
    PostgreSQL itself cannot call from SQL (INTERNAL FUNCTIONS: an internal argument; amended
    2026-11-04, A call PostgreSQL cannot make is skipped by design: every call refused for the call
    itself, ``refused_for`` read from the refusals' SQLSTATEs). An answer the check did not
    compare, and one the leg did not reach, is not tested (amended 2026-11-03, Not tested is its
    own count), as is one PostgreSQL refused for the values the check gave it. Everything else failed:
    refused by name with no decision behind it, a wrong answer, an engine error, and one
    PostgreSQL refuses for this server's state."""
    outcome = entry["outcome"]
    if outcome == "pass":
        return PASSED
    if outcome in (NOT_CHECKED, NOT_REACHED):
        return NOT_TESTED
    if outcome == BY_DESIGN:
        return SKIPPED_BY_DESIGN
    if outcome == NOT_CALLABLE:
        internal = (
            entry["detail"] == function_probe.NO_CALL
            or entry.get("refused_for") == function_probe.THE_CALL
        )
        return SKIPPED_BY_DESIGN if internal else FAILED
    return FAILED


def result_counts(outcomes: Iterable[dict[str, Any]]) -> dict[str, int]:
    """``RESULT_KEYS`` over overloads' outcomes on one engine."""
    tally: Counter[str] = Counter()
    for entry in outcomes:
        tally[verdict(entry)] += 1
        result = result_of(entry["outcome"], entry.get("rules", ()))
        tally["refused_by_name" if result == REFUSED else result] += 1
        tally["differs_wrong_answer"] += entry["outcome"] == "wrong_answer"
        tally["differs_engine_error"] += entry["outcome"] == "error"
        tally["total"] += 1
    return {key: tally.get(key, 0) for key in RESULT_KEYS}


def _named(function: dict[str, Any]) -> str:
    return f"{function['function']}({function['overload']})"


def column_results(functions: list[dict[str, Any]], engine: str) -> dict[str, Any]:
    """``COLUMN_KEYS`` of ``engine``: its passes by what their column call did."""
    passes = [f for f in functions if f["outcomes"][engine]["outcome"] == "pass"]
    called = [f for f in passes if f["outcomes"][engine].get("column")]
    both = [f for f in called if f["outcomes"][engine]["column"]["outcome"] == "pass"]
    literal_only = [f for f in called if f["outcomes"][engine]["column"]["outcome"] != "pass"]
    return {
        "pass_literal_and_column": len(both),
        "pass_literal_only": len(literal_only),
        "pass_no_column_call": len(passes) - len(called),
        "pass_literal_and_column_overloads": sorted(map(_named, both)),
        "pass_literal_only_overloads": sorted(map(_named, literal_only)),
    }


def _served(function: dict[str, Any]) -> bool:
    return any(o["outcome"] == "pass" for o in function["outcomes"].values())


def _callable(function: dict[str, Any]) -> bool:
    return any(o["outcome"] != "not_callable" for o in function["outcomes"].values())


def no_engine(functions: list[dict[str, Any]]) -> dict[str, Any]:
    """What no engine serves: an overload PostgreSQL can call that no engine answers exactly or
    equivalently, and a function with no such overload served. By family, an overload's own and a
    function's first callable overload's."""
    by_name: dict[str, list[dict[str, Any]]] = {}
    for f in functions:
        if _callable(f):
            by_name.setdefault(f["function"], []).append(f)
    overloads: Counter[str] = Counter(
        f["family"] for group in by_name.values() for f in group if not _served(f)
    )
    named: Counter[str] = Counter(
        group[0]["family"] for group in by_name.values() if not any(map(_served, group))
    )
    return {
        "overloads": sum(overloads.values()),
        "functions": sum(named.values()),
        "by_family": {
            family: {"overloads": overloads[family], "functions": named[family]}
            for family in sorted(overloads)
        },
    }


def by_family(functions: list[dict[str, Any]], engines: list[str]) -> dict[str, dict[str, Any]]:
    """Each family's ``RESULT_KEYS`` per engine."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for f in functions:
        grouped.setdefault(f["family"], []).append(f)
    return {
        family: {e: result_counts(f["outcomes"][e] for f in members) for e in engines}
        for family, members in sorted(grouped.items())
    }


def rule_coverage(functions: list[dict[str, Any]], engines: list[str]) -> list[dict[str, Any]]:
    """Each rule, its footnote number and how many overloads it covers on each engine."""
    out = []
    for rule in RULES:
        covered = {
            e: sum(1 for f in functions if rule.key in f["outcomes"][e].get("rules", ()))
            for e in engines
        }
        out.append(
            {
                "key": rule.key,
                "footnote": _NUMBERS[rule.key],
                "title": rule.title,
                "text": rule.text,
                "overloads": covered,
            }
        )
    return out


# --- Markdown ---------------------------------------------------------------------------------


def _ordered(doc: dict[str, Any]) -> list[str]:
    known = [e for e in ENGINE_LABELS if e in doc["engines"]]
    return known + [e for e in doc["engines"] if e not in ENGINE_LABELS]


def _label(engine: str) -> str:
    return ENGINE_LABELS.get(engine, engine)


def _footnote(key: str) -> str:
    return f"[{_NUMBERS[key]}](#rule-{key})"


def cell(outcome: dict[str, Any]) -> str:
    """One overload on one engine. A pass says when its value could not be compared."""
    result = result_of(outcome["outcome"], outcome.get("rules", ()))
    check = {
        "answers": "",
        "type_and_form": " (type and form only)",
        "side_effect": " (side effect)",
    }[outcome.get("check", "answers")]
    if result == EQUIVALENT:
        return f"{EQUIVALENT}{check} " + " ".join(_footnote(key) for key in outcome["rules"])
    if result == EXACT:
        return EXACT + check
    if result == NOT_CHECKED:
        return "not checked"
    if result == BY_DESIGN:
        return f"skipped by design ({outcome['group']})"
    if result == DIFFERS:
        return "differs (error)" if outcome["outcome"] == "error" else DIFFERS
    if result == NOT_CALLABLE:
        return "n/a"
    return result.replace("_", " ")


def engine_table(doc: dict[str, Any]) -> list[str]:
    """One row per engine, in overloads. No row adds them up."""
    lines = [
        "| Engine | Passed | Skipped by design | Failed | Not tested |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for engine in _ordered(doc):
        c = doc["by_engine"][engine]
        lines.append(
            f"| {_label(engine)} | {c['passed']} | {c['skipped_by_design']} | {c['failed']} "
            f"| {c['not_tested']} |"
        )
    lines += [
        "",
        "In detail: passed is exact and equivalent; skipped by design is the decisions' groups "
        "and what PostgreSQL itself cannot call; not tested is what the check did not compare or "
        "did not reach; failed is the rest.",
        "",
        "| Engine | Exact | Equivalent | By a decision | PostgreSQL calls none | Not checked "
        "| Refused by name | Differs: wrong answer | Differs: engine error "
        "| PostgreSQL refuses here | Not reached |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for engine in _ordered(doc):
        c = doc["by_engine"][engine]
        internal = c["skipped_by_design"] - c["by_design"]
        lines.append(
            f"| {_label(engine)} | {c['exact']} | {c['equivalent']} | {c['by_design']} "
            f"| {internal} | {c['not_checked']} | {c['refused_by_name']} "
            f"| {c['differs_wrong_answer']} | {c['differs_engine_error']} "
            f"| {c['not_callable'] - internal} | {c['not_reached']} |"
        )
    return lines


def column_table(doc: dict[str, Any]) -> list[str]:
    """One row per engine: its passes, by whether they hold on a column."""
    lines = [
        "| Engine | Literals and columns | Literals only | No column call |",
        "| --- | ---: | ---: | ---: |",
    ]
    for engine in _ordered(doc):
        c = doc["by_engine"][engine]
        lines.append(
            f"| {_label(engine)} | {c['pass_literal_and_column']} | {c['pass_literal_only']} "
            f"| {c['pass_no_column_call']} |"
        )
    return lines


def _column_failure(column: dict[str, Any]) -> str:
    detail = column["detail"]
    said = " ".join((detail.get("error") or detail["difference"]).split())
    return said.replace("|", "\\|")[:200]


def literal_only_lists(doc: dict[str, Any]) -> list[str]:
    """Every overload that passes on literals only, by engine, with what its column call did."""
    lines: list[str] = []
    for engine in _ordered(doc):
        named = set(doc["by_engine"][engine]["pass_literal_only_overloads"])
        rows = [f for f in doc["functions"] if _named(f) in named]
        if not rows:
            continue
        lines += [
            f"<details><summary>{_label(engine)} ({len(rows)})</summary>",
            "",
            "| Overload | Column call |",
            "| --- | --- |",
        ]
        for f in rows:
            said = _column_failure(f["outcomes"][engine]["column"])
            lines.append(f"| {_signature(f)} | {said} |")
        lines += ["", "</details>", ""]
    return lines[:-1] or ["None."]


def _column_section(doc: dict[str, Any]) -> list[str]:
    return [
        "Of each engine's passes, these also pass reading the same value from a column of a "
        "registered table. An overload that passes on literals only keeps its result above. No "
        "column call means PostgreSQL answered none, or the overload takes no value.",
        "",
        *column_table(doc),
        "",
        *literal_only_lists(doc),
    ]


def family_table(doc: dict[str, Any]) -> list[str]:
    """Per family and engine: exact, equivalent, refused by name, differs."""
    engines = _ordered(doc)
    lines = [
        "| Family | " + " | ".join(_label(e) for e in engines) + " |",
        "| --- |" + " ---: |" * len(engines),
    ]
    for family, per_engine in doc["by_family"].items():
        cells = []
        for engine in engines:
            c = per_engine[engine]
            cells.append(
                f"{c['exact']} / {c['equivalent']} / {c['not_checked']} / {c['by_design']} "
                f"/ {c['refused_by_name']} / {c['differs']}"
            )
        lines.append(f"| {family} | " + " | ".join(cells) + " |")
    return lines


def rule_lines(doc: dict[str, Any]) -> list[str]:
    """The footnotes, each saying what it covers on each engine."""
    engines = _ordered(doc)
    lines = []
    for rule in doc["rules"]:
        covered = ", ".join(f"{_label(e)} {rule['overloads'][e]}" for e in engines)
        lines += [
            f'<a id="rule-{rule["key"]}"></a>**{rule["footnote"]}. {rule["title"]}.** '
            f"{rule['text']} Overloads covered: {covered}.",
            "",
        ]
    return lines[:-1]


def _signature(function: dict[str, Any]) -> str:
    return f"`{function['function']}({function['overload']})`".replace("|", "\\|")


def overload_tables(doc: dict[str, Any]) -> list[str]:
    """Every overload PostgreSQL can call, its result per engine, a collapsed table per family."""
    engines = _ordered(doc)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for f in doc["functions"]:
        if _callable(f):
            grouped.setdefault(f["family"], []).append(f)
    lines: list[str] = []
    for family, members in sorted(grouped.items()):
        lines += [
            f"<details><summary>{family} ({len(members)})</summary>",
            "",
            "| Overload | " + " | ".join(_label(e) for e in engines) + " |",
            "| --- |" + " --- |" * len(engines),
        ]
        for f in members:
            cells = " | ".join(cell(f["outcomes"][e]) for e in engines)
            lines.append(f"| {_signature(f)} | {cells} |")
        lines += ["", "</details>", ""]
    return lines[:-1]


def wrong_answers(doc: dict[str, Any]) -> list[str]:
    """Every wrong answer, by engine: each is a defect and stays in view."""
    lines: list[str] = []
    for engine in _ordered(doc):
        rows = [
            (f, f["outcomes"][engine]["detail"])
            for f in doc["functions"]
            if f["outcomes"][engine]["outcome"] == "wrong_answer"
        ]
        if not rows:
            continue
        lines += [
            f"<details><summary>{_label(engine)} ({len(rows)})</summary>",
            "",
            "| Overload | Difference |",
            "| --- | --- |",
        ]
        for f, detail in rows:
            said = " ".join(detail["difference"].split()).replace("|", "\\|")[:200]
            lines.append(f"| {_signature(f)} | {said} |")
        lines += ["", "</details>", ""]
    return lines[:-1] or ["None."]


def not_checked_lines(doc: dict[str, Any]) -> list[str]:
    """Why each engine's overloads not checked were not."""
    reasons = doc["not_checked_reasons"]
    engines = [e for e in _ordered(doc) if reasons.get(e)]
    if not engines:
        return ["Every overload PostgreSQL answers had something compared."]
    every = sorted({why for e in engines for why in reasons[e]})
    return [
        "Why an overload was not checked, by engine:",
        "",
        "| Not checked because | " + " | ".join(_label(e) for e in engines) + " |",
        "| --- |" + " ---: |" * len(engines),
        *(
            f"| {why.replace('|', chr(92) + '|')} | "
            + " | ".join(str(reasons[e].get(why, 0)) for e in engines)
            + " |"
            for why in every
        ),
    ]


def by_design_lines(doc: dict[str, Any]) -> list[str]:
    """The overloads skipped by design, by group and engine."""
    groups = doc["by_design_groups"]
    engines = [e for e in _ordered(doc) if groups.get(e)]
    if not engines:
        return ["No overload is skipped by design."]
    every = sorted({group for e in engines for group in groups[e]})
    return [
        "Skipped by design, by group and engine:",
        "",
        "| Group | " + " | ".join(_label(e) for e in engines) + " |",
        "| --- |" + " ---: |" * len(engines),
        *(
            f"| {group} | " + " | ".join(str(len(groups[e].get(group, []))) for e in engines) + " |"
            for group in every
        ),
    ]


def no_engine_lines(doc: dict[str, Any]) -> list[str]:
    gap = doc["no_engine"]
    lines = [
        f"{gap['functions']} functions have no overload that any engine answers exactly or "
        f"equivalently, nor do {gap['overloads']} overloads.",
    ]
    if gap["by_family"]:
        lines += [
            "",
            "| Family | Functions | Overloads |",
            "| --- | ---: | ---: |",
            *(
                f"| {family} | {c['functions']} | {c['overloads']} |"
                for family, c in gap["by_family"].items()
            ),
        ]
    return lines


def _incomplete(doc: dict[str, Any]) -> list[str]:
    if not doc["engines_incomplete"]:
        return []
    names = ", ".join(_label(e) for e in doc["engines_incomplete"])
    return [f"**Incomplete engines:** {names}. Their overloads not reached have no result.", ""]


def overview_block(doc: dict[str, Any]) -> str:
    """The overview page's table."""
    return "\n".join(
        [
            *_incomplete(doc),
            *engine_table(doc),
            "",
            "Counts are `pg_catalog` overloads against PostgreSQL "
            f"{doc['postgres_version']}, one row per engine. Each result is that engine's own, "
            "and the rows do not add up to a product result. The footnotes for each equivalent "
            "result are on the [pgwire page](pgwire.md#function-results-by-engine).",
            "",
            *_column_section(doc),
        ]
    )


def pgwire_block(doc: dict[str, Any]) -> str:
    """The pgwire page's section: the counts, the footnotes, then every overload."""
    return "\n".join(
        [
            "### Function results by engine",
            "",
            f"Measured against PostgreSQL {doc['postgres_version']}. Each overload has a result "
            "on each engine. Each result is that engine's own, and the rows do not add up to a "
            "product result.",
            "",
            *_incomplete(doc),
            *engine_table(doc),
            "",
            "| Result | Meaning |",
            "| --- | --- |",
            "| Exact | The engine answered every call as PostgreSQL did, and an answer, a type "
            "and form, or a side effect was compared. |",
            "| Equivalent | The answer passes under a numbered rule below, linked from its row. |",
            "| Not checked | The engine answered with PostgreSQL's result type, but nothing was "
            "compared. A call that ran proves nothing, so this is no pass. |",
            "| Skipped by design | A recorded decision skips it: a side-effect function off "
            "the PostgreSQL engine, one whose side effect no client can see, or a refusal by name "
            "a decision makes. |",
            "| Refused by name | Provisa refused the call and said what it refuses. |",
            "| Differs: wrong answer | The answer is wrong. This is a defect. |",
            "| Differs: engine error | The engine returned an error and no named refusal. This "
            "is a defect. |",
            "",
            "A result marked *type and form only* could not be compared by value, so its result "
            "type and, for a string, its canonical form were compared with PostgreSQL's own "
            "output. One marked *side effect* returns void or acts: what it did, seen through a "
            "client, was compared with what PostgreSQL did. `n/a` means PostgreSQL itself refuses "
            "every call.",
            "",
            *by_design_lines(doc),
            "",
            *not_checked_lines(doc),
            "",
            "### Literals and columns",
            "",
            *_column_section(doc),
            "",
            "### Equivalence rules",
            "",
            *rule_lines(doc),
            "",
            "### No engine serves",
            "",
            *no_engine_lines(doc),
            "",
            "### Results by family",
            "",
            "Each cell reads exact / equivalent / not checked / skipped by design / refused by "
            "name / differs.",
            "",
            *family_table(doc),
            "",
            "### Wrong answers",
            "",
            *wrong_answers(doc),
            "",
            "### Every overload",
            "",
            *overload_tables(doc),
        ]
    )


def summary_block(doc: dict[str, Any]) -> str:
    """The run's summary: what the pages show, without the overload tables."""
    return "\n".join(
        [
            *_incomplete(doc),
            *engine_table(doc),
            "",
            "### Literals and columns",
            "",
            *_column_section(doc),
            "",
            "### Equivalence rules",
            "",
            *(
                f"{rule['footnote']}. **{rule['title']}** "
                + ", ".join(f"{_label(e)} {rule['overloads'][e]}" for e in _ordered(doc))
                for rule in doc["rules"]
            ),
            "",
            "### No engine serves",
            "",
            *no_engine_lines(doc),
        ]
    )
