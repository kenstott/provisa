# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4d1e73-2c58-4a06-8f17-6e3a0c9d5b82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a scheduled SQL trigger may run (REQ-1003, REQ-1004).

A trigger's statement inserts, updates or deletes rows of registered tables — nothing else — and
runs through the one write admission as the role the trigger names. Creating an object (CREATE
TABLE … AS SELECT, or any other definition) is refused when the trigger is saved and again when it
runs, naming the trigger. Its date tokens supply values only (scheduler/templating.py).
"""

from __future__ import annotations

from datetime import datetime

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.scheduler.templating import substitute_date_tokens

_ROW_WRITES = (exp.Insert, exp.Update, exp.Delete)


class TriggerSqlRefused(ValueError):
    """A scheduled statement the trigger may not run, said naming the trigger."""


def checked_trigger_sql(sql: str, trigger_id: str, run_at: datetime) -> str:
    """``sql`` with its date tokens rendered for ``run_at``, once it is shown to be one row write.
    Raises :class:`TriggerSqlRefused` naming ``trigger_id`` otherwise."""
    try:
        rendered = substitute_date_tokens(sql, run_at)
    except ValueError as exc:
        raise TriggerSqlRefused(f"trigger {trigger_id!r}: {exc}") from exc
    try:
        statements = [s for s in sqlglot.parse(rendered, read="postgres") if s is not None]
    except SqlglotError as exc:
        raise TriggerSqlRefused(f"trigger {trigger_id!r}: its SQL does not parse: {exc}") from exc
    if len(statements) != 1:
        raise TriggerSqlRefused(
            f"trigger {trigger_id!r}: runs one statement; this SQL holds {len(statements)}"
        )
    statement = statements[0]
    if isinstance(statement, exp.Create):
        raise TriggerSqlRefused(
            f"trigger {trigger_id!r}: creates an object ({statement.key.upper()} "
            f"{statement.args.get('kind') or ''}); a scheduled statement only inserts, updates "
            "or deletes rows of registered tables"
        )
    if not isinstance(statement, _ROW_WRITES):
        raise TriggerSqlRefused(
            f"trigger {trigger_id!r}: a scheduled statement inserts, updates or deletes rows of "
            f"registered tables; this one is {statement.key.upper()}"
        )
    return rendered
