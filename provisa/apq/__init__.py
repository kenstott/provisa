# Copyright (c) 2026 Kenneth Stott
# Canary: 2b3c4d5e-6f7a-8901-bcde-f12345678902
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Automatic Persisted Queries (APQ) — Phase AN.

Apollo-compatible APQ: clients first send a SHA-256 hash; on a miss Provisa
returns ``PersistedQueryNotFound`` and the client retries with the full query.
Subsequent requests use the hash alone.
"""

from provisa.apq.cache import APQCache, NoopAPQCache

__all__ = ["APQCache", "NoopAPQCache"]
