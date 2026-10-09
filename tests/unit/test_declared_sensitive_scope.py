# Copyright (c) 2026 Kenneth Stott
# Canary: 1edaf711-681c-4bb1-b1ae-fd907e6b453c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A declared sensitive column is not opened by changing its scope (REQ-1943, REQ-1959).

DEPENDS ON the hiding guard treating a column's ``scope`` as a hiding field, which it does not
yet: until that change is in, a table editor sets a restricted column's scope back to
``domain`` and it is served to every role in reach, without holding ``sensitive_data``. This
file holds that one case so it can be ordered after the guard's fix; the rest of the rule is
in ``test_declared_sensitive.py``."""

# Requirements: REQ-1943, REQ-1959
from __future__ import annotations

import pytest

from tests.unit.test_declared_sensitive import (  # noqa: F401  (fixtures)
    _a_kind_that_declares_mail,
    _as_stored,
    _messages,
    _register,
    _stored,
    db,
)

pytestmark = pytest.mark.asyncio


async def test_a_registrar_cannot_open_a_declared_column_by_changing_its_scope(db):  # noqa: F811
    # Depends on the hiding guard treating a column's scope as a hiding field (REQ-1959): a
    # restricted column with no grant, set back to scope domain, is served to every role.
    await _register(db, _messages(), holds_rights=False)
    again = await _register(db, _as_stored(subject={"visible_to": [], "scope": "domain"}))
    assert again.success is False and again.code == "schema.hiding_right_required"
    _, columns, _ = await _stored(db)
    assert columns["subject"]["scope"] == "restricted"
