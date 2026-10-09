# Copyright (c) 2026 Kenneth Stott
# Canary: cf3e7a17-db52-4734-93e2-c3ff17c2c8c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Microsoft 365 source: a person's mail, calendar and tasks read through Microsoft Graph
into the canonical tables both mail sources produce (``provisa.core.canonical_mail``)."""

#: The source type's name, as a source row records it.
SOURCE_TYPE = "microsoft_365"
