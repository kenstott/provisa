# Copyright (c) 2026 Kenneth Stott
# Canary: 0e7d3b56-8a1f-4c29-b4e0-7f2a9c5d1e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Warnings and errors from ``provisa.*`` modules, printed on the server's own log.

The server process is started by uvicorn, whose logging configuration attaches a stream handler to
the ``uvicorn`` loggers only. Every module here logs on ``logging.getLogger(__name__)``, a
``provisa.*`` logger with no handler of its own; its records propagate to the root logger, where
the app's own handlers (the OTLP log export, the ``/debug/logs`` buffer) take them — and, because
the root logger then HAS handlers, Python's last-resort stderr handler never prints them. A
``log.error`` in a provisa module was exported and buffered but never appeared in the process log.

:func:`send_module_logs_to_server_log` closes that: a handler on the ``provisa`` logger hands
every WARNING-and-above record to the handlers the server's own log lines go through — the ones a
``logging.getLogger("uvicorn.error")`` line reaches — so it is printed on the same stream, in the
same format, once. The root handlers are not involved: they still receive the record by ordinary
propagation, exactly as before."""

from __future__ import annotations

import logging

_SERVER_LOGGER = "uvicorn.error"


class _ServerLogHandler(logging.Handler):
    """Hands a record to the handlers of the server's log, never to the root logger's."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)

    def emit(self, record: logging.LogRecord) -> None:
        # The chain a record logged on the server logger would walk, stopping before the root:
        # the root's handlers already receive this record through the provisa logger's own
        # propagation, and a process uvicorn did not configure has no server stream to print on.
        logger: logging.Logger | None = logging.getLogger(_SERVER_LOGGER)
        while logger is not None and logger is not logging.root:
            for handler in logger.handlers:
                if record.levelno >= handler.level:
                    handler.handle(record)
            if not logger.propagate:
                return
            logger = logger.parent


def send_module_logs_to_server_log() -> None:
    """Attach the handler to the ``provisa`` logger. Once per process: a second call (the app's
    lifespan starting again in the same process) finds it there and adds nothing."""
    package_log = logging.getLogger("provisa")
    if not any(isinstance(h, _ServerLogHandler) for h in package_log.handlers):
        package_log.addHandler(_ServerLogHandler())
