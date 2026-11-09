# Copyright (c) 2026 Kenneth Stott
# Canary: 07ca1475-31ad-4895-af3c-394ab8fe7c85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Entitlements (REQ-2219): who may reach which objects, within a role.

* :mod:`~provisa.entitlements.lists` -- the lists: the identity provider's and Provisa's, and which wins.
* :mod:`~provisa.entitlements.index` -- the objects a role's lists can select, and the two checks.
* :mod:`~provisa.entitlements.mask` -- the bitmask over those objects and its cache.
* :mod:`~provisa.entitlements.store` -- the Provisa-held rows.
* :mod:`~provisa.entitlements.runtime` -- a request's mask: what the surfaces call.

The rule that enforces the mask is ``provisa.compiler.entitlement_rule``: it is a governance
rule, and the compiler may not import what builds the mask.
"""

# Requirements: REQ-2219
