# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1f0e7b-8d2a-4b59-9e64-71a5c2d8f0b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What one discovery call sends (REQ-413, amended 2026-11-18): its tables listed once, each with
its context and its comparable columns' contexts, then its numbered table pairs, and the
instructions around them: the call is a batch of separate table pairs, each answered on its own
under its number, with column pairs and scores or "none".

Tables and columns are named as people see them (:attr:`provisa.discovery.columns.TableFacts.shown`,
:attr:`provisa.discovery.columns.ColumnFacts.shown`): the alias where one is set, never a physical
name beside it. A table's context is its name, description, tags, role of fact or dimension, the
names of all its columns, those not compared included, and its comparable columns (those in a
compared type group). A compared column's context is its name, description, the glossary term
bound to it, classes, tags and sample values (a few of its distinct values), never its type; a
column with a sensitive class sends its class and no sample values. The prompt states that a shared pii tag alone is weak evidence.

A table that requires a parameter is never read (REQ-1986): its context names its required
parameters with their types and says its values were not read, and its columns send the class
``not_read`` and no sample values.

The instructions say what a confidence value states (:func:`rubric`), by the evidence a request
shows, and the run's threshold (REQ-413, amended 2026-11-18: the tick threshold of the person who
started it), so the bar a run keeps suggestions at is one the model was told of.

A sample value is sent in at most :data:`SAMPLE_VALUE_CHARS` characters as the request writes it
(:func:`sample_text`), so what a column can send has a known greatest length
(:func:`widest`): the request count before Run is computed from it, without reading a value.
"""

# Requirements: REQ-413, REQ-1986

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from provisa.discovery.columns import NOT_READ, UNKNOWN, ColumnFacts, TableFacts

#: The characters one sample value takes in a request, as written there (escapes counted, its
#: quotes not): a longer value is cut to fit. A uuid and a timestamp fit whole.
SAMPLE_VALUE_CHARS = 64
#: The distinct sample values sent of a column.
SAMPLE_VALUES = 5


def table_context(table: TableFacts, label: str) -> dict[str, Any]:
    """A table's context, named ``label`` (its name as people see it, :attr:`TableFacts.shown`,
    made unique among the run's tables): its description, tags, role, every column by the name
    people see, and its comparable columns' contexts (those in a compared type group)."""
    out: dict[str, Any] = {
        "name": label,
        "description": table.description,
        "tags": list(table.tags),
        "role": table.role,
        "columns": [c.shown for c in table.columns],
        "comparable_columns": [column_context(c) for c in table.columns if c.groups],
    }
    if table.reduced:
        out["required_parameters"] = [
            {"column": c.shown, "parameter": c.required_parameter, "type": c.data_type}
            for c in table.columns
            if c.required_parameter is not None
        ]
        out["values"] = NOT_READ
    return out


def column_context(column: ColumnFacts) -> dict[str, Any]:
    """A compared column's context, by the name people see (REQ-413, amended 2026-11-18): a
    sensitive column's class stands in for its values."""
    out: dict[str, Any] = {
        "name": column.shown,
        "description": column.description,
        "classes": list(column.classes),
        "tags": list(column.tags),
    }
    if column.term is not None:
        out["term"] = column.term
    if not column.sensitive and NOT_READ not in column.classes:
        out["sample_values"] = list(column.samples)
    return out


def _dump(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def sample_text(value: Any) -> str:
    """A sample value as it is sent: its text, cut so that the request writes it in at most
    :data:`SAMPLE_VALUE_CHARS` characters."""
    text = str(value)[:SAMPLE_VALUE_CHARS]
    while len(_dump(text)) > SAMPLE_VALUE_CHARS + 2:
        text = text[:-1]
    return text


def widest(column: ColumnFacts) -> ColumnFacts:
    """``column`` with the longest classes and sample values a run could send for it, whatever
    its table holds: every kind of the classifier's catalogue and no values (one of them is
    sensitive), or the kinds that are not sensitive and :data:`SAMPLE_VALUES` values of
    :data:`SAMPLE_VALUE_CHARS` characters, whichever is longer. A column of a table that is never
    read sends what it has: nothing is read to add to it."""
    if NOT_READ in column.classes:
        return column
    from provisa.profiler.classify import catalogue

    kinds = catalogue()
    every = replace(column, classes=tuple(sorted(k.id for k in kinds)), sensitive=True, samples=())
    plain = tuple(sorted(k.id for k in kinds if not k.sensitive)) or (UNKNOWN,)
    valued = replace(
        column,
        classes=plain,
        sensitive=False,
        samples=tuple("x" * SAMPLE_VALUE_CHARS for _ in range(SAMPLE_VALUES)),
    )
    return max(every, valued, key=lambda c: len(_dump(column_context(c))))


def table_weight(table: TableFacts, label: str) -> int:
    """A table's weight in a call: the length in characters of what is sent for it."""
    return len(_dump(table_context(table, label)))


def pair_entry(number: int, source: str, target: str) -> dict[str, Any]:
    """One numbered table pair as a call lists it, its tables by their labels."""
    return {"pair": number, "source": source, "target": target}


def pair_weight(number: int, source: str, target: str) -> int:
    return len(_dump(pair_entry(number, source, target)))


def list_weight(weights: Sequence[int]) -> int:
    """The length of a rendered JSON list holding items of these weights: brackets and commas."""
    return sum(weights) + 2 + max(0, len(weights) - 1)


#: The headers a call puts before its tables and its table pairs.
TABLES_HEADER = "## Tables\n"
PAIRS_HEADER = "## Table pairs\n"
HEADERS = len(TABLES_HEADER) + len(PAIRS_HEADER) + 2


_COLUMN_PAIR = (
    "{"
    '"source_table":"<table name>","source_column":"<column name>",'
    '"target_table":"<table name>","target_column":"<column name>",'
    '"cardinality":"many-to-one"|"one-to-many",'
    '"confidence":<0.0-1.0>,"reasoning":"<brief>","suggested_name":"<kebab-case business concept>"'
    "}"
)
_SHAPE = '{"pair":<number>,"column_pairs":[' + _COLUMN_PAIR + ', ...] or "none"}'


_NOT_READ = (
    "A table with required_parameters is read only when each is given a value, so none of "
    "its values was read: its columns have the class 'not_read' and no sample values, and "
    "are judged on their names, descriptions and tags alone. A required parameter is a "
    "value a caller passes to read the table, and often names another table's key.\n"
)


#: The decimal places a threshold is written with (:func:`written`), so its text has a known
#: greatest length, :data:`WIDEST_THRESHOLD`: calls are sized for it.
THRESHOLD_DECIMALS = 4
WIDEST_THRESHOLD = "0." + "9" * THRESHOLD_DECIMALS


def written(threshold: float) -> str:
    """A run's threshold as the instructions state it: at most :data:`THRESHOLD_DECIMALS`
    decimal places, trailing zeros dropped (0.7, 0.55, 1)."""
    return f"{threshold:.{THRESHOLD_DECIMALS}f}".rstrip("0").rstrip(".")


def rubric(threshold: str) -> str:
    """What a confidence value states, and ``threshold`` (as :func:`written` writes it), the run's
    bar a relationship is suggested at."""
    return _RUBRIC + (
        f"A relationship is suggested at {threshold} or more; one below is kept for review as "
        "left out, so give each the confidence its evidence has.\n"
    )


#: What a confidence value states, by the evidence a request shows of the two columns. A request
#: shows a few sample values of each column, so two columns of one key seldom show the same
#: values: values of one form count as agreeing.
_RUBRIC = (
    "confidence states the evidence shown here for the relationship, on this scale:\n"
    "- 0.9 to 1.0: the target column identifies one row of its table (its key: named as the "
    "table's id or code, its sample values all different), the source column's name says it "
    "refers to it (the same name, or the target's table and key named, as customer_id names "
    "customers.id), and the two columns' sample values overlap or are of one form (the same "
    "prefix, pattern or range).\n"
    "- 0.7 to 0.89: the names say so and the target is a key, and the sample values neither "
    "confirm nor contradict it: none are shown for one of the columns, or too few to tell.\n"
    "- 0.4 to 0.69: one kind of evidence alone: the same name with sample values of different "
    "forms; sample values that overlap under unrelated names; or the same name on a column "
    "that is a key of neither table (a status, a category, a place name, a date).\n"
    "- below 0.4: a guess.\n"
)


#: The measured score (REQ-413), the rule a proposed pair is scored by once its values are measured
#: (:mod:`provisa.discovery.measure`); the model's confidence is kept beside it.
#: ``contained`` is the share of the source column's distinct values found in the target column,
#: ``back`` the reverse.
#: - The target's values are distinct (a key): a source contained in it (CONTAINED_AT or more of
#:   its values found) is a key relationship and scores KEY_SCORE, whatever the model rated it;
#:   one less contained scores KEY_SCORE times ``contained``, each value with no row to refer to
#:   taking its share off.
#: - Neither side is a key: the columns share values and no relationship starts at one and ends
#:   at the other's key; the pair scores SHARED_CEILING times the larger of the two shares, so it
#:   never reaches the default threshold (:data:`provisa.discovery.threshold.DEFAULT`).
#: So a pair whose values barely meet scores low however alike its names are. A pair whose values
#: could not be compared (a side not read, or none read of the source) keeps the model's
#: confidence, and says why.
CONTAINED_AT = 0.95
KEY_SCORE = 0.95
SHARED_CEILING = 0.6


def measured_score(contained: float, back: float, target_distinct: bool) -> float:
    """A measured pair's score, by the rule above."""
    if target_distinct:
        return KEY_SCORE if contained >= CONTAINED_AT else round(KEY_SCORE * contained, 4)
    return round(SHARED_CEILING * max(contained, back), 4)


def instructions(
    pairs: int, *, threshold: float, either_direction: bool, not_read: bool = False
) -> str:
    """The text before a call's tables and table pairs. ``pairs``: how many table pairs the call
    lists. ``threshold``: the run's, the bar a relationship is suggested at. ``either_direction``:
    source and target are one set of tables, so a relationship may start in either table of a
    pair. ``not_read``: a table of the call requires a parameter and was never read, and the text
    says how to judge it."""
    return _instructions(
        pairs, written(threshold), either_direction=either_direction, not_read=not_read
    )


def instructions_chars(pairs: int, *, either_direction: bool, not_read: bool = False) -> int:
    """The most characters :func:`instructions` holds, whatever the run's threshold: it is
    written at its widest (:data:`WIDEST_THRESHOLD`)."""
    return len(
        _instructions(pairs, WIDEST_THRESHOLD, either_direction=either_direction, not_read=not_read)
    )


def _instructions(pairs: int, threshold: str, *, either_direction: bool, not_read: bool) -> str:
    if either_direction:
        direction = (
            "Source and target are drawn from one set of tables: in a pair, a relationship may "
            "start in either table and end in the other, and a pair of one table with itself "
            "compares two different columns of it."
        )
    else:
        direction = (
            "In each pair a relationship starts at a column of the source table (the referring "
            "column) and ends at a column of the target table."
        )
    return (
        "You are a database relationship analyst. Find columns that refer to other columns, as "
        "a foreign key refers to the key it names: many-to-one, the referring column the many "
        "side.\n"
        f"This is a batch of {pairs} separate table pairs. The tables are listed once under "
        "'## Tables', then the numbered pairs under '## Table pairs'. Judge each pair on its own, "
        "comparing only the two tables it names. For each pair name the column pairs of its two "
        "tables likely to hold the same or similar values, each with a confidence; a column pair "
        "you do not name scores zero. Answer every pair under its number, with its column pairs "
        'or "none".\n'
        "Each table lists the names of all its columns; only its comparable_columns may be "
        "named, and a column is compared only with one of the same type group (whole number, "
        "decimal, text, uuid). A column's classes say what its values were recognised as; "
        "'unknown' is a column none was recognised for. A column with a sensitive class shows "
        "its class and no sample values. Sample values are a few of a column's distinct values: "
        "they show what kind of values it holds.\n"
        f"{_NOT_READ if not_read else ''}"
        "A shared pii tag alone is weak evidence of a relationship.\n"
        f"{direction}\n"
        f"{rubric(threshold)}"
        "Name tables and columns exactly as listed. Return ONLY a JSON array, no other text, "
        "one element per pair:\n"
        f"{_SHAPE}\n"
        "suggested_name names the business concept the join means, not its tables or columns.\n"
    )


def prompt(
    tables: Sequence[dict[str, Any]],
    pairs: Sequence[dict[str, Any]],
    *,
    threshold: float,
    either_direction: bool,
) -> str:
    """One call: the instructions, its tables (each :func:`table_context`) listed once, and its
    numbered table pairs (each :func:`pair_entry`). ``threshold``: the run's.
    ``either_direction``: source and target are one set of tables."""
    not_read = any(t.get("values") == NOT_READ for t in tables)
    return "\n".join(
        [
            instructions(
                len(pairs),
                threshold=threshold,
                either_direction=either_direction,
                not_read=not_read,
            ),
            TABLES_HEADER + _dump(list(tables)),
            PAIRS_HEADER + _dump(list(pairs)),
        ]
    )
