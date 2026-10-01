# Copyright (c) 2026 Kenneth Stott
# Canary: e94d1b27-6c05-4a3f-8b72-5f0a9c3e7d16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where licensing keeps its state (REQ-1135, REQ-1136).

The user's ``~/.provisa`` plus the OS-native per-user data directory — two locations outside the
install dir, so no single deletion resets first use. NOTHING relocates them: ``$PROVISA_HOME``
moves the ops store and the certificates, not the trial, or setting it would start a fresh one.

THE SANDBOX (``$PROVISA_LICENSING_SANDBOX_DIR``) exists for one case: a process that must not
WRITE the user's installation — a server started by the test suite, on the machine of someone who
also runs the product. Under it licensing still READS the real anchors and the real high-water
mark, so the trial clock is the real one; every write (healing an anchor, advancing the mark,
installing a license) goes to the sandbox directory instead. It cannot be used to reset a trial:
the first-use date is the earliest of the real and the sandbox anchors, and elapsed time is
measured against the later of the real and the sandbox high-water marks, so a sandbox — however
new — is never further from expiry than the installation beside it. The one thing it does differ
in is the license file, which is the sandbox's own: a sandboxed process is unlicensed unless a
license is installed into the sandbox.
"""

# Requirements: REQ-1135, REQ-1136

from __future__ import annotations

import os
from pathlib import Path


def user_home() -> Path:
    """The user's Provisa home, ``~/.provisa``."""
    return Path.home() / ".provisa"


def secondary_store() -> Path:
    """The second, independent anchor location: the OS per-user data directory."""
    import platformdirs

    return Path(platformdirs.user_data_dir("provisa", "provisa"))


def sandbox_dir() -> Path | None:
    """The directory licensing writes to instead of the user's installation, or ``None``."""
    sandbox = os.environ.get("PROVISA_LICENSING_SANDBOX_DIR")
    return Path(sandbox) if sandbox else None
