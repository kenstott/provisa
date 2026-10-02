# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1e9d04-2f7a-4b83-a5c6-0d8f3e7b1a92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A warning or error from any ``provisa.*`` module reaches the server's own log.

The server process is started by uvicorn, whose logging configuration gives only the ``uvicorn``
loggers a stream handler. The app adds its own handlers to the root logger (the OTLP log export,
the ``/debug/logs`` buffer), so Python's last-resort stderr handler never fires either: a
``log.error`` in a ``provisa`` module was exported, buffered, and never printed. Each case below
runs in a fresh interpreter configured the way a started server is, and reads its real stderr."""

from __future__ import annotations

import subprocess
import sys
import textwrap

_SERVER = """
import logging, logging.config
import uvicorn.config

logging.config.dictConfig(uvicorn.config.LOGGING_CONFIG)  # what `uvicorn main:app` does first

class _RootSink(logging.Handler):  # stands in for the OTLP export and the debug buffer
    seen = []
    def emit(self, record):
        self.seen.append(record.getMessage())

logging.getLogger().addHandler(_RootSink())
from provisa.core.server_log import send_module_logs_to_server_log
"""


def _stderr(body: str, *, server: str = _SERVER) -> list[str]:
    script = textwrap.dedent(server) + textwrap.dedent(body)
    done = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120, check=True
    )
    return [line for line in done.stderr.splitlines() if "DeprecationWarning" not in line]


def test_a_module_error_is_printed_once_in_uvicorns_format():
    lines = _stderr(
        """
        send_module_logs_to_server_log()
        logging.getLogger("provisa.federation.replica_hot").error("promotion failed for %s", "orders")
        logging.getLogger("uvicorn.error").error("a server line")
        """
    )
    assert lines == ["ERROR:    promotion failed for orders", "ERROR:    a server line"]


def test_without_it_the_same_error_never_reaches_stderr():
    """The defect: this is what a started server did with every provisa module's errors."""
    lines = _stderr(
        """
        logging.getLogger("provisa.federation.replica_hot").error("promotion failed")
        assert _RootSink.seen == ["promotion failed"]
        """
    )
    assert lines == []


def test_warnings_are_printed_and_lower_levels_are_not():
    lines = _stderr(
        """
        send_module_logs_to_server_log()
        log = logging.getLogger("provisa.federation.native_backend")
        log.setLevel(logging.DEBUG)
        log.debug("d"); log.info("i"); log.warning("attach failed"); log.critical("gone")
        """
    )
    assert lines == ["WARNING:  attach failed", "CRITICAL: gone"]


def test_a_traceback_is_printed_with_its_error():
    lines = _stderr(
        """
        send_module_logs_to_server_log()
        try:
            raise ValueError("bad row")
        except ValueError:
            logging.getLogger("provisa.api.rest.cypher_router").exception("execution failed")
        """
    )
    assert lines[0] == "ERROR:    execution failed"
    assert lines[1].startswith("Traceback") and lines[-1] == "ValueError: bad row"


def test_installing_twice_still_prints_once_and_the_root_handlers_see_it_once():
    lines = _stderr(
        """
        send_module_logs_to_server_log()
        send_module_logs_to_server_log()  # the lifespan runs again in the same process
        logging.getLogger("provisa.core.catalog").error("registration failed")
        assert _RootSink.seen == ["registration failed"], _RootSink.seen
        """
    )
    assert lines == ["ERROR:    registration failed"]


def test_other_libraries_and_the_servers_own_lines_are_untouched():
    lines = _stderr(
        """
        send_module_logs_to_server_log()
        logging.getLogger("sqlalchemy.engine").error("not ours")
        logging.getLogger("uvicorn.error").warning("startup phase ready")
        assert _RootSink.seen == ["not ours"], _RootSink.seen
        """
    )
    assert lines == ["WARNING:  startup phase ready"]


def test_a_process_uvicorn_did_not_configure_gets_no_second_copy_on_the_root_handlers():
    """Not started by uvicorn (an embedded or test process): the server log has no stream of its
    own there, and a module record must not be handed to the root handlers a second time."""
    lines = _stderr(
        """
        send_module_logs_to_server_log()
        logging.getLogger("provisa.core.catalog").error("registration failed")
        assert _RootSink.seen == ["registration failed"], _RootSink.seen
        """,
        server=_SERVER.replace("logging.config.dictConfig(uvicorn.config.LOGGING_CONFIG)", "pass"),
    )
    assert lines == []
