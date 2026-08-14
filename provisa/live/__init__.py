# Copyright (c) 2026 Kenneth Stott
# Canary: 5888030c-802a-4438-9550-fbc50596211a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live Query Engine (Phase AM) — watermark-based polling with SSE fanout and Kafka sink."""

from provisa.live.engine import LiveEngine

__all__ = ["LiveEngine"]
