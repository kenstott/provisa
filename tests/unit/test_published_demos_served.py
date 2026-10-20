# Copyright (c) 2026 Kenneth Stott
# Canary: 7cf7a4f1-63f3-496b-bf9b-9153acebd43a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every published named demo is in a tier's bundle list, so cloud.provisa.dev serves it.

worldcup, cloudpolicy and churnprod were published (their cards on the site's live demos page)
but never added to sandbox-tier-bundles, and the sandbox tier answered "No published demo named".
"""

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_LISTS = _ROOT / "packaging" / "linux" / "demo-stores"


def _listed() -> set[str]:
    names: set[str] = set()
    for name in ("sandbox-tier-bundles", "regions-tier-bundles"):
        names |= {d for d in (_LISTS / name).read_text().strip().replace(" ", "").split(",") if d}
    return names


def _published() -> set[str]:
    out = set()
    for spec in (_ROOT / "demo" / "named").glob("*/demo.yaml"):
        if yaml.safe_load(spec.read_text()).get("published") is True:
            out.add(spec.parent.name)
    return out


def test_every_published_demo_is_served_by_a_tier():
    assert _published() - _listed() == set()
