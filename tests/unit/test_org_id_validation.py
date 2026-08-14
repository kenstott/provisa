# Copyright (c) 2026 Kenneth Stott
# Canary: 33b9bcce-74b2-4607-857f-df55a3bd3f2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1309: the org id is chosen once and can never change, so creation is the only chance to
reject one that would break something later.

The id is interpolated unquoted into per-org DDL (``org_<id>`` schema, ``role_<id>``,
``org_<id>_mv_cache``, ``org_<id>__<catalog>``) and is also the subdomain. A value that is legal
JSON but illegal there fails deep inside background provisioning, which is exactly the failure the
validator exists to move forward to the request that chose it.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from provisa.api.admin.orgs_router import _MAX_ORGS_PER_USER, _validate_new_org_id


@pytest.mark.parametrize("org_id", ["ac", "acme", "acme2", "a1", "a" * 40])
def test_accepts_legal_ids(org_id):
    _validate_new_org_id(org_id)  # does not raise


@pytest.mark.parametrize(
    "org_id",
    [
        "",  # empty
        "a",  # single character
        "1acme",  # leads with a digit — not a legal bare schema name
        "Acme",  # uppercase would fold unpredictably in an unquoted identifier
        "ac me",  # whitespace
        "acme-corp",  # hyphen: legal in a subdomain, a syntax error in unquoted org_<id> DDL
        "acme_corp",  # underscore would collide with the org_<id>_mv_cache suffix space
        "acme;drop",  # the reason this is a whitelist and not an escape
        "a" * 41,  # past the length ceiling
        "pg_toast",  # every pg_-prefixed schema name carries an underscore the pattern rejects
        "information_schema",
    ],
)
def test_rejects_illegal_ids(org_id):
    with pytest.raises(HTTPException) as exc:
        _validate_new_org_id(org_id)
    assert exc.value.status_code == 400


@pytest.mark.parametrize("org_id", ["root", "default", "public", "admin", "api", "www"])
def test_rejects_reserved_ids(org_id):
    with pytest.raises(HTTPException) as exc:
        _validate_new_org_id(org_id)
    assert exc.value.status_code == 400
    assert "reserved" in exc.value.detail


def test_cap_is_a_backstop_not_a_tier():
    # REQ-1311. Asserted so the number cannot drift into a product limit without a deliberate edit.
    assert _MAX_ORGS_PER_USER == 100
