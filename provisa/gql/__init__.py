# Copyright (c) 2026 Kenneth Stott
# Canary: e41af8d3-5c7e-4a7f-96e2-29a2df045182
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The graph query IR and its back end, which lowers it to semantic SQL (REQ-2135). Each graph
language has a front end that parses into the IR: Cypher's is provisa.cypher."""

# The semantic SQL keeps its null-on-failure casts at every rewrite (REQ-778): registered by each
# package that writes or rewrites it -- never by the package root, which the pg engine's fake
# functions import in a Python holding no sqlglot (REQ-1494).
from provisa.transpiler import semantic_dialect as _semantic_dialect  # noqa: E402, F401
