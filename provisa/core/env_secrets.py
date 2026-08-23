# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f4e91-6c3d-48a5-9f10-84ce2d76b135
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A credential typed into a carried field is refused at the write (REQ-1525).

WHY AT THE WRITE AND NOT AT THE COMMIT. REQ-1524 forbids a failed commit from failing the change it
observes, so a refusal at commit time would accept the secret into the control plane, decline to
project it, and mark the environment drifted -- leaving the credential in the database, the
repository permanently behind, and a rebuild that would either write the secret or never converge.
Refusing the write leaves both stores agreeing and the secret in neither.

WHY GIT MAKES THIS DIFFERENT FROM AN ORDINARY VALIDATION. History is immutable by design, and a
rewrite does not reach clones or forks already taken. Once a literal is pushed the only remedy is
rotating the credential, so the requirement is to keep it from being written rather than to remove
it afterwards.

WHAT IS AND IS NOT SCANNED. Only :data:`provisa.core.env_classes.CARRIED` tables. A credential on an
IDENTITY_ONLY table is a BINDING, which is exactly where a credential belongs (REQ-1491) and which
REQ-1489 already keeps out of every copy and every commit -- scanning those would refuse the
supported way to give an environment a credential.

WHY NOT ENTROPY. A generated identifier and a generated key are the same string to an entropy
detector, and a false refusal against a legitimate expression teaches authors to route around the
check. Detection is by high-confidence provider token shapes and by a URI carrying a password in
its userinfo, both of which a legitimate value does not accidentally look like. What this misses
remains the author's care, which the requirement reduces rather than eliminates.
"""

# Requirements: REQ-1489, REQ-1491, REQ-1524, REQ-1525

from __future__ import annotations

import re
from typing import Any

from provisa.core.env_classes import CARRIED

#: The reference form an author is shown. It resolves at USE time through
#: :mod:`provisa.core.secrets`, so what is carried is the NAME of a secret -- meaningless without
#: the environment holding it -- and the value stays where REQ-1489 already keeps values.
REFERENCE_FORM = "${env:VAR_NAME}"

#: Provider token shapes, each specific enough that a legitimate value does not resemble one. The
#: name is the reason shown to the author, so it names the provider rather than the pattern.
_TOKEN_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("an AWS access key id", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    (
        "a GitHub token",
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})"),
    ),
    ("a Slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}")),
    ("a Stripe secret key", re.compile(r"\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}")),
    ("an OpenAI API key", re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}")),
    ("an Anthropic API key", re.compile(r"\bsk-ant-[A-Za-z0-9_-]{24,}")),
    ("a Google API key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("a private key", re.compile(r"-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----")),
)

#: ``scheme://user:password@host``. The password group is what makes this a credential rather than a
#: URL -- ``https://host/path`` and ``https://user@host`` both fail to match.
_URI_USERINFO = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/:@]+:([^\s/@]+)@")

#: A value that IS a reference. Matched against the credential candidate rather than the whole
#: string, so ``postgres://svc:${env:PGPASSWORD}@host`` passes while the literal form does not.
_REFERENCE = re.compile(r"\$\{\w+:[^}]+\}")


class CredentialLiteralError(ValueError):
    """A carried field was written with a credential in it, naming the field and the form to use."""

    def __init__(self, table: str, column: str, reason: str) -> None:
        self.table = table
        self.column = column
        self.reason = reason
        super().__init__(
            f"{table}.{column} looks like it contains {reason}. A carried field is committed to the "
            f"environment's git history (REQ-1524), where a credential cannot be taken back -- write "
            f"a reference instead: {REFERENCE_FORM}"
        )


def find_credential(value: Any) -> str | None:
    """What credential ``value`` appears to contain, or ``None``.

    A non-string is not scanned: a credential is text somebody typed, and coercing a number or a
    JSON bag to str to search it would report matches inside values no author wrote.
    """
    if not isinstance(value, str) or not value:
        return None
    for reason, pattern in _TOKEN_PATTERNS:
        match = pattern.search(value)
        if match and not _REFERENCE.search(match.group(0)):
            return reason
    userinfo = _URI_USERINFO.search(value)
    if userinfo and not _REFERENCE.search(userinfo.group(1)):
        return "a password in a URL"
    return None


def check_row(table: str, values: dict[str, Any]) -> None:
    """Refuse a write to a carried table that carries a credential literal.

    A table outside :data:`CARRIED` is not scanned at all -- see the module docstring for why a
    binding is the one place a credential is supposed to be.
    """
    if table not in CARRIED:
        return
    for column, value in values.items():
        reason = find_credential(value)
        if reason is not None:
            raise CredentialLiteralError(table, column, reason)


def guard_statement(stmt: Any) -> Any:
    """The seam: every Core INSERT/UPDATE is checked before it executes, and returned unchanged.

    Modelled on REQ-828's meta-RLS guard and placed beside it for the same reason -- a rule that
    holds only where somebody remembered to call it is a rule with holes. Returns the statement so
    the caller reads as a pipeline; it never rewrites one.
    """
    from sqlalchemy.sql.dml import Insert, Update

    if not isinstance(stmt, (Insert, Update)):
        return stmt
    table = getattr(stmt, "table", None)
    name = getattr(table, "name", None)
    if name is None or name not in CARRIED:
        return stmt
    parameters = getattr(stmt, "_values", None) or {}
    for column, value in parameters.items():
        column_name = getattr(column, "name", column)
        literal = getattr(value, "value", None)
        reason = find_credential(literal)
        if reason is not None:
            raise CredentialLiteralError(name, str(column_name), reason)
    return stmt
