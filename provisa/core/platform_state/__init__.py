# Copyright (c) 2026 Kenneth Stott
# Canary: 1852c7d2-0977-41f4-9950-58dfbb76a017
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PLATFORM STATE STORE: per-deployment operating state, in the platform database.

The deployment has four stores, each reached through its own handle
(``provisa/core/store_sides.py``, which keeps them apart). The MODEL STORE (``model_db``) is per
org, shared across its regions. The STATE STORE (``tenant_db``) and the RECORD (``record_db``) are
per org and region. The PLATFORM STATE STORE, here, holds operating state that belongs to the
deployment as a whole: one handle, ``state.platform_state_db``, in the platform database; its
tables are declared with the platform tables (``core/schema_admin``), and it is never part of an
org's model — never projected, never in an environment copy or export.

One module per member:

- ``nodes``: the cluster's nodes, each with its mode and region (REQ-1916).
"""

from provisa.core.platform_state import nodes

#: Every platform-state table, across the members.
TABLES: frozenset[str] = frozenset(nodes.TABLES)
