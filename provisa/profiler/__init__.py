# Copyright (c) 2026 Kenneth Stott
# Canary: 7f4d2b86-1e93-4c5a-b8d0-6a3e9c1f2d74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The built-in Data Profiler source type (REQ-1934).

A profiler source is a name, a cron schedule and a run default (the whole table, or a sample of a
stated size). Tables join it from the table editor; each run profiles a member table with one
aggregate statement read as the org admin through the one governed pipeline, and appends the
profile to that table's result relations (``schema``). Registering a result relation on the
profiler source exposes it like any governed table.
"""
