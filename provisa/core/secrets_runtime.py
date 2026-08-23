# Copyright (c) 2026 Kenneth Stott
# Canary: 0b2193be-5d0c-488e-ba43-a27b62b452d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one secrets service this process is wired to (REQ-1557).

``secrets.provider`` / ``secrets.<provider>`` in provisa.yaml selects it, exactly as
``encryption.provider`` selects the encryption service, and ``configure_secrets`` installs it once
at startup. Unconfigured is NOT a broken state: it means Provisa's own encrypted per-org store,
which is the answer for the open-source install, the demo, and every hosted org that does not own
the server's process environment.

The backend is built LAZILY on first use rather than at import, so a deployment that never resolves
a ``${secret:...}`` never constructs one -- and so the built-in store does not go looking for an
encryption master key at import time.
"""

# Requirements: REQ-557, REQ-1557

from __future__ import annotations

from provisa.core.secrets import SecretsProvider

_selected: tuple[str | None, dict] = (None, {})
_backend: SecretsProvider | None = None


def configure_secrets(provider: str | None, *, config: dict | None = None) -> None:
    """Select the secrets backend. Idempotent; the build itself is deferred to first use."""
    global _selected, _backend
    _selected = (provider, config or {})
    _backend = None


def secrets_backend() -> SecretsProvider:
    """The configured backend, building it on first use. Fail-closed on an unknown one."""
    global _backend
    if _backend is not None:
        return _backend
    from provisa.core.secrets_registry import get_secrets_provider_spec

    name, config = _selected
    spec = get_secrets_provider_spec(name)
    if spec is None:
        raise ValueError(
            f"Unknown secrets provider {name!r}. Register it via "
            "provisa.core.secrets_registry.register_secrets_provider "
            "(or PROVISA_SECRETS_PROVIDER_MODULES)."
        )
    if not spec.available():
        raise ValueError(
            f"Secrets provider {spec.key!r} is registered but not available "
            "(its SDK/runtime is not installed)."
        )
    _backend = spec.build(config)
    return _backend


def secrets_backend_spec():
    """The spec of the selected backend, without building it. Used by the Secrets page."""
    from provisa.core.secrets_registry import get_secrets_provider_spec

    return get_secrets_provider_spec(_selected[0])


def reset_secrets() -> None:
    """Clear the selection (test isolation)."""
    global _selected, _backend
    _selected, _backend = (None, {}), None
