# Copyright (c) 2026 Kenneth Stott
#
# A PostgreSQL 16 with PL/Python and the fake library the Provisa engines use (REQ-1494): the test
# image for the pg engine's fake functions (tests/integration/test_fake_reads_pg_e2e.py). The
# functions import provisa from the checkout the test mounts at /provisa (PYTHONPATH).
FROM postgres:16
ARG FAKER_VERSION=40.41.0
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-plpython3-16 python3-pip \
 && pip3 install --no-cache-dir --break-system-packages "faker==${FAKER_VERSION}" \
 && rm -rf /var/lib/apt/lists/*
