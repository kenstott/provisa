# Copyright (c) 2026 Kenneth Stott
# Canary: 10926cca-c492-48cf-889a-d55181674ef3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# The semantic SQL keeps its null-on-failure casts at every rewrite (REQ-778): registered before
# any statement is written.
from provisa.transpiler import semantic_dialect as _semantic_dialect  # noqa: E402, F401
