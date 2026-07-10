# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin GraphQL schema — assembles the Query + Mutation types into one schema.

The read-side resolvers live in ``schema_query`` and the write-side resolvers in
``schema_mutation`` (whose heaviest bodies live in ``schema_mutation_ops``). This
module wires them together and re-exports the handful of helpers that external
callers import via ``provisa.api.admin.schema``.
"""

# Requirements: REQ-012, REQ-013, REQ-016, REQ-019, REQ-020, REQ-021, REQ-041, REQ-042, REQ-063, REQ-133, REQ-155, REQ-156, REQ-158, REQ-215, REQ-252, REQ-253, REQ-276, REQ-304, REQ-305, REQ-306, REQ-366, REQ-393, REQ-399, REQ-400, REQ-402, REQ-413, REQ-416, REQ-432, REQ-433, REQ-434

from __future__ import annotations

import strawberry

from provisa.api.admin.schema_query import Query
from provisa.api.admin.schema_mutation import Mutation

# Re-exports for external importers of provisa.api.admin.schema (catalog_cache,
# table_search_router, tests + steps). Keep these paths stable.
from provisa.compiler.naming import source_to_catalog  # noqa: F401
from provisa.api.admin.schema_helpers import _get_pool, _rebuild_schemas  # noqa: F401
from provisa.api.admin._table_ops import (  # noqa: F401
    _build_column_models,
    _ensure_view_column_types,
)
from provisa.api.admin.schema_common import (  # noqa: F401
    _rebuild_relationship_input,
    _rebuild_table_input,
)

admin_schema = strawberry.Schema(query=Query, mutation=Mutation)
