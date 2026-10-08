# Copyright (c) 2026 Kenneth Stott
# Canary: 9d382b0a-889a-4686-a2b8-353743181722
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement the pgwire surface refuses is not an error of the server: it is logged as the
refusal it is, without a traceback. An unexpected failure keeps its traceback."""

from __future__ import annotations

import logging

from provisa.api.errors import ApiError
from provisa.pgwire.server import _log_statement_failure


def test_a_refusal_is_one_info_line_without_a_traceback(caplog):
    refusal = ApiError(403, "data.write_not_admitted", "Mutation 'relabel' not permitted")
    with caplog.at_level(logging.DEBUG, logger="provisa.pgwire.server"):
        _log_statement_failure("statement", "CALL relabel(1)", refusal)
    (record,) = caplog.records
    assert record.levelno == logging.INFO and record.exc_info is None
    assert "refused" in record.getMessage() and "not permitted" in record.getMessage()


def test_a_server_error_answer_keeps_its_traceback(caplog):
    failure = ApiError(500, "data.no_direct_route", "no direct connection")
    with caplog.at_level(logging.DEBUG, logger="provisa.pgwire.server"):
        _log_statement_failure("statement", "SELECT 1", failure)
    (record,) = caplog.records
    assert record.levelno == logging.WARNING and record.exc_info is not None


def test_an_unexpected_failure_keeps_its_traceback(caplog):
    with caplog.at_level(logging.DEBUG, logger="provisa.pgwire.server"):
        _log_statement_failure("DESCRIBE", "SELECT 1", ValueError("boom"))
    (record,) = caplog.records
    assert record.levelno == logging.WARNING and record.exc_info is not None
    assert "DESCRIBE EXCEPTION" in record.getMessage()
