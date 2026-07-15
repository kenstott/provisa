# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""pgwire scalar interception for pg_is_in_recovery() and txid_current().

A JDBC/DataGrip status probe combining both functions must resolve to a bigint
without reaching the engine (Calcite rejects PG_IS_IN_RECOVERY()).
"""

from unittest.mock import MagicMock

from provisa.pgwire.catalog import answer, classify

_CURRENT_TXID_PROBE = (
    "SELECT CASE WHEN pg_is_in_recovery() THEN NULL ELSE "
    "CAST(CAST((MOD(pg_catalog.txid_current(),4294967296)) AS VARCHAR) AS BIGINT) END "
    "AS current_txid"
)


def _state():
    st = MagicMock()
    st.contexts = {}
    return st


def test_pg_is_in_recovery_intercepted_false():
    assert classify("SELECT pg_is_in_recovery()") == "INTERCEPT"
    result = answer("SELECT pg_is_in_recovery()", "alice", _state())
    assert result.column_names == ["pg_is_in_recovery"]
    assert result.rows == [(False,)]


def test_txid_current_intercepted_bigint():
    assert classify("SELECT txid_current()") == "INTERCEPT"
    result = answer("SELECT txid_current()", "alice", _state())
    assert result.column_names == ["txid_current"]
    (val,) = result.rows[0]
    assert isinstance(val, int)


def test_current_txid_case_probe_resolves_to_bigint():
    assert classify(_CURRENT_TXID_PROBE) == "INTERCEPT"
    result = answer(_CURRENT_TXID_PROBE, "alice", _state())
    assert result.column_names == ["current_txid"]
    assert result.column_types == ["BIGINT"]
    (val,) = result.rows[0]
    assert isinstance(val, int)


def test_txid_current_is_monotonic():
    r1 = answer("SELECT txid_current()", "alice", _state())
    r2 = answer("SELECT txid_current()", "alice", _state())
    assert r2.rows[0][0] > r1.rows[0][0]
