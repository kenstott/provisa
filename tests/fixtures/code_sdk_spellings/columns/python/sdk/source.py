# Copyright (c) 2026 Kenneth Stott
# Canary: c2727e16-6a69-483d-b802-73c40697a820
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import Batch, Results, columns


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Upper:
    upper: str


@columns
def shout(names: Batch[str]) -> Results[Upper]:
    """Each name in capitals."""
    return [Upper(upper=n.upper()) for n in names]
