# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f0a72-4d3b-4e88-9a51-2f7c8b0d15ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Login throttling and lockout for every credential-validating surface (REQ-1393).

Password guessing is a protocol-independent attack: the same account can be hammered over HTTP,
pgwire and Bolt, so a counter that lives on one surface is a counter an attacker moves off. This
module holds the counter at the credential-validation layer instead — :func:`throttled` wraps the
validator that ``validator_for_scheme`` hands back, so every surface counts into the same store and
a lockout earned on one is enforced on all of them.

The key is the principal the protocol carries (the HTTP Basic / pgwire startup / Bolt username).
A bearer-only surface carries no principal, so the key is a digest of the credential itself: a
signed token is not guessable a character at a time, and what a digest key does stop is one bad
token being replayed without limit.

The store is per process. A deployment running several API workers therefore allows up to
``max_attempts`` per worker before locking; the counter is a brake on guessing, not a distributed
quota, and making it shared would put a database write on every failed authentication.
"""

from __future__ import annotations

import hashlib
import math
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable

import jwt

# Requirements: REQ-1393

# Defaults, applied when the deployment configures no ``auth.login_throttle`` block. They are the
# design's choice rather than a guess to be patched around: five attempts is above any plausible
# typo count, and a fifteen-minute lockout costs a locked-out human one coffee break while costing
# an online guesser three orders of magnitude in throughput. REQ-1393 requires the throttle to be
# on by default — an opt-in brake protects only the deployments that did not need protecting.
_DEFAULT_MAX_ATTEMPTS = 5
_DEFAULT_WINDOW_SECONDS = 300
_DEFAULT_LOCKOUT_SECONDS = 900


class LockedOut(PermissionError):  # REQ-1393
    """Too many recent failures for this subject; the credential was not even examined.

    A ``PermissionError`` rather than a ``ValueError`` on purpose: every surface catches
    ``ValueError`` to mean "bad credential, answer 401", and a lockout answered that way is
    indistinguishable from a wrong password — the client keeps trying. Being a distinct type
    forces each surface to say what it is.
    """

    def __init__(self, subject: str, retry_after: int) -> None:
        self.subject = subject
        self.retry_after = retry_after
        super().__init__(f"too many failed authentication attempts; retry in {retry_after} seconds")


class LoginThrottle:  # REQ-1393
    """Failure counter with lockout, keyed by subject.

    Reached from pgwire's socketserver worker threads and from the API event loop at the same
    time, so every mutation is under one lock.
    """

    def __init__(
        self,
        *,
        max_attempts: int,
        window_seconds: int,
        lockout_seconds: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.lockout_seconds = lockout_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._failures: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}

    @property
    def settings(self) -> tuple[int, int, int]:
        return (self.max_attempts, self.window_seconds, self.lockout_seconds)

    def check(self, subject: str) -> None:
        """Raise :class:`LockedOut` if this subject is locked; return otherwise."""
        now = self._clock()
        with self._lock:
            until = self._locked_until.get(subject)
            if until is None:
                return
            if until <= now:
                # The lockout expired: clear it together with the failures that caused it, so the
                # subject starts the next window with a full allowance rather than one attempt.
                del self._locked_until[subject]
                self._failures.pop(subject, None)
                return
            # Rounded up, so a client that waits exactly this long finds the lockout expired rather
            # than a fraction of a second short of it. Never below 1: a Retry-After of 0 invites an
            # immediate retry that is certain to be refused again.
            retry_after = max(1, math.ceil(until - now))
        raise LockedOut(subject, retry_after)

    def record_failure(self, subject: str) -> None:
        """Count one rejected credential, locking the subject out on the nth within the window."""
        now = self._clock()
        with self._lock:
            recent = [t for t in self._failures.get(subject, []) if now - t < self.window_seconds]
            recent.append(now)
            self._failures[subject] = recent
            if len(recent) >= self.max_attempts:
                self._locked_until[subject] = now + self.lockout_seconds

    def record_success(self, subject: str) -> None:
        """A valid credential clears the subject's history — the account is demonstrably in use."""
        with self._lock:
            self._failures.pop(subject, None)
            self._locked_until.pop(subject, None)


_throttle: LoginThrottle | None = None
_throttle_lock = threading.Lock()


def _settings_from(auth_config: dict | None) -> tuple[int, int, int]:
    block = (auth_config or {}).get("login_throttle") or {}
    return (
        int(block.get("max_attempts", _DEFAULT_MAX_ATTEMPTS)),
        int(block.get("window_seconds", _DEFAULT_WINDOW_SECONDS)),
        int(block.get("lockout_seconds", _DEFAULT_LOCKOUT_SECONDS)),
    )


def configure_login_throttle(auth_config: dict | None) -> LoginThrottle:
    """Install the throttle described by ``auth.login_throttle``, keeping the counts.

    Called from ``build_auth_provider``, which runs per connection on the wire surfaces, so
    identical settings must return the existing instance — rebuilding would reset every counter on
    the next connection attempt and hand an attacker an unlimited allowance.
    """
    global _throttle
    settings = _settings_from(auth_config)
    with _throttle_lock:
        if _throttle is None or _throttle.settings != settings:
            max_attempts, window_seconds, lockout_seconds = settings
            _throttle = LoginThrottle(
                max_attempts=max_attempts,
                window_seconds=window_seconds,
                lockout_seconds=lockout_seconds,
            )
        return _throttle


def login_throttle() -> LoginThrottle:
    """The process-wide throttle, built with the REQ-1393 defaults if nothing configured one."""
    global _throttle
    with _throttle_lock:
        if _throttle is None:
            _throttle = LoginThrottle(
                max_attempts=_DEFAULT_MAX_ATTEMPTS,
                window_seconds=_DEFAULT_WINDOW_SECONDS,
                lockout_seconds=_DEFAULT_LOCKOUT_SECONDS,
            )
        return _throttle


def reset_login_throttle() -> None:
    """Drop the installed throttle. For tests, which must not inherit another test's counts."""
    global _throttle
    with _throttle_lock:
        _throttle = None


def subject_key(principal: str | None, credential: str) -> str:
    """The lockout key for one authentication attempt.

    ``principal`` is what the protocol names — the HTTP Basic user, the pgwire startup user, the
    Bolt principal. A bearer-only surface has none, and keying on the credential's digest is then
    the tightest thing available: it bounds replay of a single rejected token without letting one
    caller's bad token lock out an unrelated one.
    """
    if principal:
        return f"user:{principal}"
    return "token:" + hashlib.sha256(credential.encode("utf-8")).hexdigest()


@contextmanager
def login_attempt(principal: str, credential: str):  # REQ-1393
    """Count one ``/auth/login`` exchange under the throttle.

    The login routes sit on the middleware skip list, so the wrapper the middleware applies never
    sees them; they enter the same store through here instead, keyed by the account being tried.
    Raises :class:`LockedOut` before the body runs when that account is locked.
    """
    throttle = login_throttle()
    key = subject_key(principal, credential)
    throttle.check(key)
    try:
        yield
    except (ValueError, jwt.PyJWTError):
        throttle.record_failure(key)
        raise
    throttle.record_success(key)


async def throttled(
    validator: Callable[[str], Any], credential: str, *, principal: str | None
) -> Any:  # REQ-1393
    """Run one credential validation under the throttle.

    Raises :class:`LockedOut` before the validator is reached when the subject is locked; counts a
    failure when the validator refuses the credential; clears the history when it accepts.
    Anything other than a credential rejection — a JWKS fetch failure, a database outage — is
    neither counted nor swallowed: an infrastructure fault must not lock users out.
    """
    throttle = login_throttle()
    key = subject_key(principal, credential)
    throttle.check(key)
    try:
        identity = await validator(credential)
    except (ValueError, jwt.PyJWTError):
        throttle.record_failure(key)
        raise
    throttle.record_success(key)
    return identity
