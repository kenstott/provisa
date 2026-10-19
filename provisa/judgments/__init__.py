# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Judgments under review (REQ-2094, docs/arch/judgment-review-design.md).

Every governance judgment -- a tag on an object, a column's fake and synthetic rule, a mask
entry, a row rule, a write grant -- carries an optional ``why``: the reason, purpose, cited bases
and review-by date its decider gave, and who decided it, when and on what profile evidence, set by
the server. The review checks compare that record with the model and profiles now and put what
they find in one queue, ``judgment_findings``.
"""
