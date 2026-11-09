#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0c1b7e-8a34-4d62-9c1e-3b7d2e90a614
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Run uvicorn so that a backend which cannot start ends the launch.

``uvicorn --reload`` never looks at its child: when the app fails to import or its lifespan
fails, the child exits with uvicorn's startup-failure code and the reloader goes on watching
files with nothing running. ``--workers`` already stops on that code.

The first server only: once a reload has happened, a failed start is a bad edit, and the reloader
keeps waiting for the next one.

Usage: serve_backend.py uvicorn main:app [uvicorn options]. The word ``uvicorn`` is required so
the command line still matches ``pgrep -f "uvicorn main:app"``, which is how the launch scripts
find a backend to stop.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import uvicorn.main
from uvicorn.config import STARTUP_FAILURE
from uvicorn.supervisors import ChangeReload

logger = logging.getLogger("uvicorn.error")


class EndOnFailedFirstStart(ChangeReload):
    """The reloader, stopping when the first server exits with the startup-failure code."""

    reloaded = False
    first_start_failed = False

    def restart(self) -> None:
        self.reloaded = True
        super().restart()

    def should_restart(self) -> list[Path] | None:
        if not self.reloaded and self.process.exitcode == STARTUP_FAILURE:
            logger.error("The backend failed to start; stopping the reloader.")
            self.first_start_failed = True
            raise StopIteration
        return super().should_restart()

    def shutdown(self) -> None:
        super().shutdown()
        if self.first_start_failed:
            sys.exit(STARTUP_FAILURE)


def main() -> None:
    if sys.argv[1:2] != ["uvicorn"]:
        raise SystemExit("usage: serve_backend.py uvicorn main:app [uvicorn options]")
    uvicorn.main.ChangeReload = EndOnFailedFirstStart  # pyright: ignore[reportAttributeAccessIssue]
    uvicorn.main.main(args=sys.argv[2:])


if __name__ == "__main__":
    main()
