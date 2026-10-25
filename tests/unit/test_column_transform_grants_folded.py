# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155, REQ-2131: a column transform (a ``code_columns`` command) is admitted like every
command -- its ``visible_to`` holds for a role it names or a role that inherits one -- so the build
folds its grants over each role's ancestors as it folds every other command's. The insider demo's
deal_signals names data_steward; org_admin, which inherits it, was refused it (demo bundles run
40229110917, "Unknown command: 'deal_signals'")."""

from __future__ import annotations

import inspect

from provisa.api import app as app_module


def test_the_build_folds_the_column_transforms_grants_with_the_commands():
    source = inspect.getsource(app_module._rebuild_schemas_impl)
    assert "expand_grants(tracked_functions, role_chains_by_id)" in source
    assert "expand_grants(list(state.column_transforms.values()), role_chains_by_id)" in source
