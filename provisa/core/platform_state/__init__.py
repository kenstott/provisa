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

The deployment has three stores. The MODEL STORE is per org, shared across its regions (the
model's tables, ``core/repositories``). The STATE STORE is per org and region, with the request
record (replica builds, events, freshness — ``core/env_classes.NEVER_RUNTIME``). The PLATFORM
STATE STORE, here, holds operating state that belongs to the deployment as a whole: it is reached
through the platform database (``state.admin_db``), its tables are declared with the platform
tables (``core/schema_admin``), and it is never part of an org's model — never projected, never
in an environment copy or export.

One module per member:

- ``nodes``: the cluster's nodes, each with its mode and region (REQ-1916).
"""
