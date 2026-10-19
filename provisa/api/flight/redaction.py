# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every Flight error message passes central redaction (REQ-2087): each RPC method of the
Flight and airport servers raises its error with the text redacted, keeping the error's kind."""

# Requirements: REQ-2087

from __future__ import annotations

import functools
from typing import Any

#: The Flight RPCs a server implements.
RPCS = (
    "list_flights",
    "get_flight_info",
    "get_schema",
    "do_get",
    "do_put",
    "do_exchange",
    "do_action",
    "list_actions",
    "do_handshake",
)


def _redacted(method: Any) -> Any:
    import pyarrow.flight as flight

    from provisa.security.redaction import redact

    @functools.wraps(method)
    def call(*args: Any, **kwargs: Any) -> Any:
        try:
            return method(*args, **kwargs)
        except flight.FlightError as exc:  # pyright: ignore[reportPrivateImportUsage]
            said = redact(str(exc))
            if said == str(exc):
                raise
            raise type(exc)(said, exc.extra_info) from None
        except Exception as exc:  # noqa: BLE001  # allow-blind-except: pyarrow sends any other exception's text to the client as a server error; only its text is redacted, and the error stays a server error
            said = redact(str(exc))
            if said == str(exc):
                raise
            raise flight.FlightServerError(said) from None  # pyright: ignore[reportPrivateImportUsage]

    return call


def redact_errors(cls: type) -> type:
    """``cls`` with each Flight RPC it defines raising redacted errors."""
    for name in RPCS:
        if name in vars(cls):
            setattr(cls, name, _redacted(vars(cls)[name]))
    return cls
