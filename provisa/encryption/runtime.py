# Copyright (c) 2026 Kenneth Stott
# Canary: 9cfb0cee-666a-4b1c-8128-fbf731a12c8f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Process-configured EncryptionService accessor (REQ-684, REQ-686).

The encryption provider is process-global config (``encryption.provider`` /
``encryption.key_id`` in provisa.yaml, REQ-684), so cross-cutting call sites —
column encryption in repositories, cache payloads — read the one configured
service through ``encryption_service()`` instead of threading it through every
call chain. ``configure_encryption`` is called once at startup; before that (and
in tests) the accessor returns the passthrough NullEncryption, so the encrypted
code path is exercised identically whether or not a provider is configured.

REQ-1574 makes the accessor ORG-AWARE. An org that has set a key of its own is served that
org's ring service; an org that has not is served the deployment's, because unset is not
unconfigured. The org is read off the ``current_org`` ContextVar through an injected getter --
``provisa.encryption`` cannot import ``provisa.api``, the same constraint ``core.domain_policy``
solves the same way. Selection FAILS CLOSED against a ROSTER: startup records which orgs hold a
ring at all (one query over ``org_encryption_keys``), and a bound org on that roster whose ring has
not been loaded raises rather than being served the deployment key -- which would wrap its next
payload under a key its owner did not choose. An org not on the roster holds no key of its own and
is served the deployment service, because unset is not unconfigured. With no roster and no selector
(CLI, tests, single-org startup) the accessor is exactly what it was: the process-wide service.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from collections.abc import Iterable

from provisa.encryption.service import EncryptionService, NullEncryption

_service: EncryptionService | None = None

# REQ-1574: org_id -> the org's ring service, or None for "this org holds no key of its own".
# A key ABSENT from this map is not the same statement: see ``encryption_service``.
_org_services: dict[str, EncryptionService | None] = {}
# The ContextVar getter, injected by the API layer (see ``bind_org_selector``). ``None`` means no
# org is ever bound -- CLI, tests, single-org startup.
_org_selector: Callable[[], str | None] | None = None
# REQ-1574: the orgs KNOWN to hold a ring. Membership here without a loaded service is the one
# state that must raise -- the org has a key and this process has not got it.
_org_rings: set[str] = set()


def configure_encryption(
    provider: str | None, *, key_id: str | None = None, config: dict | None = None
) -> EncryptionService:
    """Build and install the process-wide EncryptionService from config. Idempotent.

    ``config`` is the per-provider block (``encryption.<provider>``) carrying e.g.
    the KMS key ARN / Vault address for the selected provider.
    """
    global _service
    from provisa.encryption.factory import build_encryption_service  # noqa: PLC0415

    _service = build_encryption_service(provider, key_id=key_id, config=config)
    return _service


def deployment_encryption_service() -> EncryptionService:
    """The DEPLOYMENT's service, ignoring whichever org is bound (REQ-1574).

    Two call sites need this and no others: wrapping an org's own key for storage, and unwrapping
    it to build that org's ring. Reaching for :func:`encryption_service` there would ask the org's
    key to encrypt itself.
    """
    return _service if _service is not None else NullEncryption()


def bind_org_selector(getter: Callable[[], str | None] | None) -> None:
    """Tell the accessor how to find the bound org (REQ-1574). Called once by the API layer."""
    global _org_selector
    _org_selector = getter


def set_org_encryption(org_id: str, service: EncryptionService | None) -> None:
    """Install (or clear) one org's ring service, and keep the roster in step.

    ``None`` records that the org holds no key of its own, which also takes it OFF the roster: the
    two statements are the same one, and letting them disagree is what would make a load raise
    forever after a key was removed.
    """
    _org_services[org_id] = service
    if service is None:
        _org_rings.discard(org_id)
    else:
        _org_rings.add(org_id)


def note_org_rings(org_ids: "Iterable[str]") -> None:
    """Record which orgs hold a ring, without loading any of them (REQ-1574).

    Startup reads this off ``org_encryption_keys`` in one query. It is the whole of the fail-closed
    guarantee: an org named here is served its own key or nothing at all.
    """
    _org_rings.update(org_ids)


def org_encryption_loaded(org_id: str) -> bool:
    """Whether this process has already resolved ``org_id``'s ring (loaded, or known to be absent)."""
    return org_id in _org_services


def encryption_service() -> EncryptionService:
    """The service for the BOUND org, else the deployment's (REQ-684, REQ-1574).

    An org with a loaded ring gets it. An org off the roster holds no key of its own and gets the
    deployment service, which is what its data was already written under. An org ON the roster with
    no loaded ring raises: the roster is the design guarantee that this process knows the org holds
    a key, so a gap is a load-ordering defect and not a licence to pick a different key.
    """
    if _org_selector is not None:
        org_id = _org_selector()
        if org_id is not None:
            ring = _org_services.get(org_id)
            if ring is not None:
                return ring
            if org_id in _org_rings:
                raise RuntimeError(
                    f"org {org_id!r} holds an encryption key this process has not loaded; its "
                    "runtime must load the ring before any encrypted path runs (REQ-1574)"
                )
    return _service if _service is not None else NullEncryption()


def reset_encryption() -> None:
    """Clear the configured service and every per-org ring (test isolation)."""
    global _service, _org_selector
    _service = None
    _org_services.clear()
    _org_rings.clear()
    _org_selector = None
