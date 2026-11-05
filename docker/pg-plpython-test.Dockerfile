# Copyright (c) 2026 Kenneth Stott
#
# A PostgreSQL 16 with PL/Python and provisa's runtime dependencies (REQ-1494): the test image for
# the pg engine's fake functions (tests/integration/test_fake_reads_pg_e2e.py). Production runs
# PL/Python on the interpreter that holds provisa and all it depends on
# (provisa/pg_extensions/staging.py), so this image installs the same dependencies, at uv.lock's
# versions, into PL/Python's interpreter. requirements.txt is uv.lock's export, written beside this
# file by the test; pgserver (the embedded control plane, which PL/Python never imports) is left
# out, since it has no wheel for this image's Python. The functions import provisa from the
# checkout the test mounts at /provisa (PYTHONPATH).
FROM postgres:16
COPY requirements.txt /tmp/requirements.txt
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-plpython3-16 python3-pip \
 && pip3 install --no-cache-dir --break-system-packages --ignore-installed -r /tmp/requirements.txt \
 && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt
