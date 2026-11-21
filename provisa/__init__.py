# Copyright (c) 2026 Kenneth Stott
# Canary: 10926cca-c492-48cf-889a-d55181674ef3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa. Importing the package fixes where zoneinfo finds its zones, before any module reads one.

Every surface answers a named zone from one IANA release: the ``tzdata`` package pinned in
pyproject.toml, which is also the release the Rust encoders' tables are built from
(rust/pgwire/third_party/chrono-tz/tz) and pytz's (pyarrow's zoned timestamps). zoneinfo would
otherwise read the host's /usr/share/zoneinfo first, whatever release the host carries. An empty
search path sends it to the package; PYTHONTZPATH carries the same to every Python process
Provisa starts."""

import os
import zoneinfo

os.environ["PYTHONTZPATH"] = ""
zoneinfo.reset_tzpath(to=())
zoneinfo.ZoneInfo.clear_cache()
