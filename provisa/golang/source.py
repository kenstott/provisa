# Copyright (c) 2026 Kenneth Stott
# Canary: 51f0b8c3-9a26-4d7e-8e41-c3b7d2a96f08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go source's code, as Provisa runs it (REQ-2200).

- Its exported functions are read by the reader (:mod:`provisa.golang.reader`) and judged by the
  shared rules (:mod:`provisa.codesource.manifest`).
- Its host is compiled with the code linked in (:mod:`provisa.golang.build`), and its worker runs
  that one binary inside the operating system's sandbox, speaking every code source's protocol
  (:mod:`provisa.codesource.worker`). The binary is static and needs no runtime: it reads nothing
  but itself (and, on Linux, the system's certificates the sandbox always gives), and reaches its
  socket and the egress proxy only.
- On macOS Go verifies TLS with the system's trust service, which the sandbox does not reach, so
  the host is given the system's certificate bundle (which the sandbox lets every worker read)
  as the roots it verifies with instead.
"""

# Requirements: REQ-2200
from __future__ import annotations

import platform
from collections.abc import Sequence
from pathlib import Path

from provisa.codesource.sandbox import sandboxed
from provisa.codesource.worker import HostCommand, private_environment

#: The system's certificate bundle on macOS (macos_sandbox's /private/etc/ssl, read by every
#: worker).
MACOS_CERTIFICATES = Path("/private/etc/ssl/cert.pem")


def host_command(*, binary: Path, egress: Sequence[str]) -> HostCommand:
    """How the compiled host runs, sandboxed: the binary is its own runtime, and the directory
    holding it the only one it reads."""
    env = private_environment()
    if platform.system() == "Darwin":
        env.update(PROVISA_CERTIFICATES=str(MACOS_CERTIFICATES), GODEBUG="x509usefallbackroots=1")
    return sandboxed(
        lambda address: [str(binary), address],
        executable=binary,
        read=[],
        env=env,
        cwd=binary.parent,
        egress=egress,
    )
