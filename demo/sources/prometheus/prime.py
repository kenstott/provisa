# Copyright (c) 2026 Kenneth Stott
# Canary: 0f085169-a425-4cdf-8550-f0686dd2d12c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Prometheus demo source: wait until the server has scraped itself at least
once, so the `up` metric has a sample. Nothing is written — Prometheus is pull-only.
"""

from __future__ import annotations

import os
import sys
import time

import httpx

PORT = int(os.environ.get("PROVISA_DEMO_PROMETHEUS_PORT", "29090"))
BASE = f"http://localhost:{PORT}"


def main() -> int:
    deadline = time.monotonic() + 90
    with httpx.Client(timeout=10) as client:
        while True:
            try:
                resp = client.get(f"{BASE}/api/v1/query", params={"query": "up"})
                if resp.status_code == 200 and resp.json().get("data", {}).get("result"):
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                print(f"prometheus at {BASE} never scraped itself", file=sys.stderr)
                return 1
            time.sleep(2)
    print(f"prometheus demo source primed: 'up' has samples at {BASE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
