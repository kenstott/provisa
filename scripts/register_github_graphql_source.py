# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7d83d9-5784-4b42-8f12-b5eaca4046ea
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register GitHub's public GraphQL API as a graphql_remote source (REQ-307).

No new connector: graphql_remote's generic introspect/execute path (REQ-307/309)
already handles GitHub's bearer-token auth. This just POSTs a registration to the
running admin API.

Requires GITHUB_TOKEN (a GitHub PAT with at least `read:user` scope) in the
environment — raises if unset, never falls back silently.

Usage:
    GITHUB_TOKEN=$(gh auth token) python3 scripts/register_github_graphql_source.py
"""

import os
import sys

import httpx

ADMIN_URL = (
    f"http://localhost:{os.environ.get('PROVISA_API_PORT', '8001')}/admin/sources/graphql-remote"
)


def main() -> None:
    token = os.environ["GITHUB_TOKEN"]
    payload = {
        "source_id": "github-graphql",
        "url": "https://api.github.com/graphql",
        "namespace": "gh",
        "auth": {"type": "bearer", "token": token},
        "description": "GitHub public GraphQL API",
    }
    resp = httpx.post(ADMIN_URL, json=payload, timeout=60.0)
    resp.raise_for_status()
    print(resp.json())


if __name__ == "__main__":
    sys.exit(main())
