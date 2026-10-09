# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Microsoft 365 source: a person's mail, calendar and tasks read through Microsoft Graph
into the canonical tables both mail sources produce (``provisa.core.canonical_mail``)."""

from provisa.core import mail_platforms

#: The source type's name, as a source row records it.
SOURCE_TYPE = "microsoft_365"

# REQ-1923: an organisation enters its Microsoft client once -- the application, its secret and
# the directory (tenant) it is registered in; every source of it signs in with that client.
mail_platforms.declare(mail_platforms.Platform(SOURCE_TYPE, ("tenant",)))
