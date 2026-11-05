# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1f7e2a-9b4d-4e85-a6d0-58f2b1c94e17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A schema build records the model stamp it is loaded at only after its own model writes
(REQ-1914, REQ-351). The build stores every relationship's effective names; each row it changes
advances the model stamp. Read before those writes, the stamp the runtime serves is one the model
has already left, so no audit row can name the commit the environment stands at (REQ-1524): the
boot baseline is recorded at the stamp after the writes."""

from __future__ import annotations

import inspect

from provisa.api import app as app_module


def test_the_build_stores_effective_names_before_it_reads_the_model_stamp():
    source = inspect.getsource(app_module._rebuild_schemas_impl)
    stored = source.index("store_effective_names(")
    stamped = source.index("_config_stamp.read(")
    assert stored < stamped
