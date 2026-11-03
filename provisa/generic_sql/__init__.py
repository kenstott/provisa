# Copyright (c) 2026 Kenneth Stott
# Canary: 18849715-5e42-4625-a45a-0f390440ff87
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generic SQL sources (REQ-2191): a database with no named source type, reached by its connection
URL through the SQLAlchemy dialect the URL's scheme names, and written to in the SQL dialect its
admin chose. What it can do is what it has been shown to do: each capability is a setting, off to
start (``settings``)."""

# Requirements: REQ-2191

SOURCE_TYPE = "generic_sql"
