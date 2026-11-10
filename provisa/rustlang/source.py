# Copyright (c) 2026 Kenneth Stott
# Canary: ef50ac1f-42ba-4f4b-b14c-b82c948f6c3e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Rust source's code, as Provisa runs it (REQ-2227).

- Its public functions are read by the reader (:mod:`provisa.rustlang.reader`) and judged by the
  shared rules (:mod:`provisa.codesource.manifest`).
- Its host is compiled with the code linked in (:mod:`provisa.rustlang.build`), and its worker
  runs that one binary inside the operating system's sandbox, speaking every code source's
  protocol (:mod:`provisa.codesource.worker`). The binary needs no runtime: on Linux it is static
  and reads nothing but itself; it reaches its socket and the egress proxy only. This is the
  same sandbox a Go source's host runs in.
- On macOS a TLS library that asks the system's trust service is out of the sandbox's reach, so
  the host is pointed at the system's certificate bundle (which the sandbox lets every worker
  read), where the Rust TLS libraries look for one (``SSL_CERT_FILE``).
"""

# Requirements: REQ-2227
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
        env.update(SSL_CERT_FILE=str(MACOS_CERTIFICATES))
    return sandboxed(
        lambda address: [str(binary), address],
        executable=binary,
        read=[],
        env=env,
        cwd=binary.parent,
        egress=egress,
    )
