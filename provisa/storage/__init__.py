# Copyright (c) 2026 Kenneth Stott
# Canary: 7c41d0a8-53be-4e2f-9a6d-2f8b41c7e095
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Platform-side storage: what an org's bytes cost the operator, and who bears it.

Two concerns, deliberately separate modules:

* :mod:`provisa.storage.quota` — measure an org's footprint in the platform's own store and
  reject the operation that would push it past its tier ceiling (REQ-1046/1047/1049).
* :mod:`provisa.storage.byo` — the org that supplies its own store instead, whose bytes land on
  its bill and are never measured or capped here (REQ-1048).
"""
