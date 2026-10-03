# Copyright (c) 2026 Kenneth Stott
# Canary: 3b69f102-4554-4ed6-9668-c9a74c1067cc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An admin mutation's commit is named by its operation (REQ-1524).

The commit itself is made by the request's change scope (:mod:`provisa.core.model_change`); this
extension only gives the change the operation's name, for a commit no writer named.
"""

# Requirements: REQ-1524

from __future__ import annotations

from strawberry.extensions import SchemaExtension

from provisa.core import model_change


class ModelChangeLabel(SchemaExtension):
    """Label the request's model change with the mutation's operation name."""

    async def on_operation(self):
        yield
        # After the operation: its document is parsed only once it runs. The request's change is
        # committed later, when its response starts, so the label is in place for it.
        execution_context = self.execution_context
        if execution_context.result is None or execution_context.pre_execution_errors:
            return
        if execution_context.operation_type.value.lower() == "mutation":
            model_change.label(execution_context.operation_name or "mutation")
