# Copyright (c) 2026 Kenneth Stott
# Canary: 60063fa4-2258-4ad6-a523-87f0de921ba4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Google Workspace source: the mail, calendar and tasks of Google accounts (REQ-1923)."""

from provisa.core.declared_sensitive import declare_canonical_mail

#: The source type's name, as a source row records it.
SOURCE_TYPE = "google_workspace"

# REQ-1943: its mail tables are the canonical ones, hidden and tagged as those are declared.
declare_canonical_mail(SOURCE_TYPE)
