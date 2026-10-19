# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Central redaction where telemetry leaves the process (REQ-2087): every span every channel
exports, and the recent-spans view; every log record the OTel log pipeline sends; and every
record a logging handler writes, stdout and container logs included. One rule list
(:mod:`provisa.security.redaction`), applied by every org's rules together."""

# Requirements: REQ-2087

from __future__ import annotations

import logging
from typing import Any

from provisa.security import redaction


def _attributes(attrs: Any) -> dict[str, Any]:
    return {k: redaction.redact_value(v) for k, v in dict(attrs or {}).items()}


def redacted_span(span: Any) -> Any:
    """A copy of the ended ``span`` with its name, attributes, events (exception events
    included) and status description redacted. An ended span cannot be changed in place, so each
    channel is handed this copy."""
    from opentelemetry.sdk.trace import Event, ReadableSpan
    from opentelemetry.trace import Status

    events = [
        Event(
            name=redaction.redact(event.name),
            attributes=_attributes(event.attributes),
            timestamp=event.timestamp,
        )
        for event in span.events or ()
    ]
    status = span.status  # an ended span always has one (UNSET when nothing set it)
    if status.description:
        status = Status(status.status_code, redaction.redact(status.description))
    return ReadableSpan(
        name=redaction.redact(span.name),
        context=span.context,
        parent=span.parent,
        resource=span.resource,
        attributes=_attributes(span.attributes),
        events=events,
        links=span.links,
        kind=span.kind,
        instrumentation_scope=span.instrumentation_scope,
        status=status,
        start_time=span.start_time,
        end_time=span.end_time,
    )


def log_record_processor() -> Any:
    """A LogRecordProcessor that redacts each record's body and attributes before the
    processors after it export it. Installed first on the log provider."""
    from opentelemetry.sdk._logs import LogRecordProcessor

    class _Redacting(LogRecordProcessor):
        def on_emit(self, log_record: Any) -> None:
            record = log_record.log_record  # the SDK's ReadWriteLogRecord holds the API record
            if isinstance(record.body, str):
                record.body = redaction.redact(record.body)
            elif record.body is not None:
                record.body = redaction.redact_value(record.body)
            if record.attributes:
                record.attributes = _attributes(record.attributes)

        def shutdown(self) -> None:
            pass

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            return True

    return _Redacting()


class RedactingFilter(logging.Filter):
    """Redacts a log record's message and exception text before any handler formats it: on
    every handler (stdout, files, the OTel bridge), so container logs carry what the OTel
    pipeline carries."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = redaction.redact(message)
        if redacted != message:
            record.msg, record.args = redacted, None
        if record.exc_info and record.exc_text is None:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = redaction.redact(record.exc_text)
        if record.stack_info:
            record.stack_info = redaction.redact(record.stack_info)
        return True


_FILTER = RedactingFilter()


def install_on_every_handler() -> None:
    """Put the redacting filter on every handler of every logger that has one now, the root's
    and uvicorn's included. Called after logging is configured and again after the OTel bridge
    attaches its handler."""
    loggers = [logging.getLogger(), *logging.Logger.manager.loggerDict.values()]
    for logger in loggers:
        for handler in getattr(logger, "handlers", ()) or ():
            if _FILTER not in handler.filters:
                handler.addFilter(_FILTER)
