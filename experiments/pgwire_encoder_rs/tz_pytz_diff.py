# Copyright (c) 2026 Kenneth Stott
# Canary: 8d41b6e2-2f7a-4c09-b3d5-6e0a91c47f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How far the product's own path (pyarrow's to_pylist, which takes a named zone from pytz when
pytz is installed) is from zoneinfo, over the differential's instants of every zone: the count of
text cells that differ, by year range. Run like test_compare.py, with the same PYTHONPATH:
    .venv/bin/python experiments/pgwire_encoder_rs/tz_pytz_diff.py
"""

from __future__ import annotations

from collections import Counter

import pyarrow as pa
from python_ref import python_encode
from test_compare import ZONES, _batch, _data_rows, _python, _zone_instants

TZ = "TIMESTAMP WITH TIME ZONE"


def main() -> None:
    by_era: Counter[str] = Counter()
    zones_differing = 0
    cells = 0
    for zone in ZONES:
        micros, _ = _zone_instants(zone)
        batch = _batch([pa.array(micros, pa.timestamp("us", tz=zone))])
        product, _ = python_encode(batch.schema, [batch], [TZ], [0])
        reference, _ = _python(batch, [TZ], [0])
        differ = 0
        for v, a, b in zip(micros, _data_rows(product), _data_rows(reference), strict=True):
            cells += 1
            if a != b:
                differ += 1
                year = int(b[0][:4]) if b[0] and b[0][:1].isdigit() else 0
                era = (
                    "before 1901" if year < 1901 else "1901-2037" if year <= 2037 else "after 2037"
                )
                by_era[era] += 1
                assert v is not None
        zones_differing += differ > 0
    print(f"{cells} cells over {len(ZONES)} zones; {zones_differing} zones differ")
    for era, n in sorted(by_era.items()):
        print(f"  {era}: {n} cells")


if __name__ == "__main__":
    main()
