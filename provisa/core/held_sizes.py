# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How many entries each process-wide map holds that an org's deletion does not clear (REQ-2149):
what a soak reads through ``/debug/leak-probe`` beside the org-kept maps (``org_forget.sizes``).
A module registers its map at its own import (:func:`held`)."""

# Requirements: REQ-2149

from __future__ import annotations

from collections.abc import Callable, Sized

_sizes: dict[str, Callable[[], int]] = {}


def held(module: str, name: str, mapping: Sized) -> None:
    """Report ``len(mapping)`` as ``<module>.<name>``."""
    _sizes[f"{module}.{name}"] = lambda: len(mapping)


def sizes() -> dict[str, int]:
    """Each registered map's entry count, by its name."""
    return {name: size() for name, size in _sizes.items()}
