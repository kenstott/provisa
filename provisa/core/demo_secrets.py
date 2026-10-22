# Copyright (c) 2026 Kenneth Stott
# Canary: 3a413ddd-ce8a-4f66-b046-f4ad10a5d616
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The secrets a demo declares (REQ-2095).

A demo's ``secrets.yaml``, beside its ``config.yaml`` (and so in its bundle, which carries the
demo's directory), names each secret and its value::

    secrets:
      SHOP_DB_PASSWORD: provisa_reader

Every org built from the demo's configuration -- its template org, the deployment org launched on
it, each sandbox cloned from the template -- has them written to its own vault (the org vault,
``ORG_OWNER``) before the configuration is loaded, so the configuration names them
``${secret:NAME}`` instead of holding the value. A rewrite is a rotation (``secrets_store.put``).
A parse of the configuration where that vault is not bound (a template's seed, the deployment's
file at launch, a bundle build) resolves them from the same file (:func:`resolving`). Names are
reported; values are never logged, printed or put in an error.
"""

# Requirements: REQ-2095

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from provisa.core.database import Database

FILE = "secrets.yaml"
_DESCRIPTION = f"Declared by the demo ({FILE})"


def declared(directory: Path) -> dict[str, str]:
    """The secrets the demo in ``directory`` declares, by name; none without a secrets.yaml. A
    file of another shape is refused naming the file and the entry, never a value."""
    from provisa.core.secrets_store import validate_name

    path = directory / FILE
    if not path.is_file():
        return {}
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict) or set(raw) != {"secrets"} or not isinstance(raw["secrets"], dict):
        raise ValueError(f"{path}: expected one key, secrets, mapping each name to its value")
    found: dict[str, str] = {}
    for name, value in raw["secrets"].items():
        if not isinstance(name, str):
            raise ValueError(f"{path}: a secret's name must be text")
        validate_name(name)
        if not isinstance(value, str) or value == "":
            raise ValueError(f"{path}: secret {name} needs a value, as text")
        found[name] = value
    return found


def launched_directory(config_path: Path) -> Path:
    """The directory whose secrets.yaml the deployment org's vault is given: the named demo the
    deployment was started as (``start-ui-install.sh --demo`` names it, and its config is then a
    wrapper written elsewhere), or, when none was, the directory of the file it was launched with
    (a demo's config.yaml launched as it is, as the bundle build and the demos' tests do)."""
    from provisa.core.demo import started_demo_directory

    return started_demo_directory() or config_path.parent


async def seed(admin_db: Database, org_id: str, directory: Path) -> list[str]:
    """Write the secrets the demo in ``directory`` declares to org ``org_id``'s vault. Returns
    their names."""
    from provisa.core.secrets_store import ORG_OWNER, put

    secrets = declared(directory)
    for name, value in secrets.items():
        await put(admin_db, org_id, name, value, owner_id=ORG_OWNER, description=_DESCRIPTION)
    return sorted(secrets)


@contextmanager
def resolving(directory: Path, org_id: str):
    """Within the block, a ``${secret:NAME}`` the demo in ``directory`` declares resolves to its
    value, as org ``org_id``'s: for a parse of the demo's configuration where the org's vault is
    not bound. Nothing is bound when the demo declares no secrets."""
    from provisa.core.secrets_store import bound_to_values

    secrets = declared(directory)
    if not secrets:
        yield
        return
    with bound_to_values(org_id, secrets):
        yield
