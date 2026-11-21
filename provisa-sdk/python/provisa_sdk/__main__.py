# Copyright (c) 2026 Kenneth Stott
# Canary: c4d27e60-93b1-4a58-8f0d-6e15b7a2c394
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``python -m provisa_sdk check [args]``: runs ``uvx provisa==<this SDK's version> code check [args]``.

No logic of its own: the check is the server's own reader at the matching version (REQ-1984).
"""

# Requirements: REQ-1984
from __future__ import annotations

import os
import sys
from importlib.metadata import version


def command(args: list[str]) -> list[str]:
    if not args or args[0] != "check":
        raise SystemExit("usage: python -m provisa_sdk check [DIR] [--json] [--python PATH] ...")
    return ["uvx", f"provisa=={version('provisa-sdk')}", "code", "check", *args[1:]]


def main(argv: list[str]) -> None:
    argv_ = command(argv)
    os.execvp(argv_[0], argv_)


if __name__ == "__main__":
    main(sys.argv[1:])
