# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2c8e17-3b6a-4f09-a1c4-7e9b0d3f2a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The id prefixes of Provisa's built-in relationships: the one definition.

The startup seed (``provisa.api._meta_seed``) declares the built-in relationships under these
prefixes and removes the ones it no longer declares by recognising them; the relationship
repository refuses a saved relationship whose id carries one.
"""

BUILTIN_RELATIONSHIP_ID_PREFIXES: tuple[str, ...] = ("meta:", "ontology:")
