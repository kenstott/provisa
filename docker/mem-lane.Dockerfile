# Base image for the linux-mem lane (scripts/run-linux-mem-lane.sh).
#
# The RLIMIT_AS memory-bounded tests in tests/integration/test_streaming_memory_bounded_e2e.py
# only exercise real behavior on Linux — macOS ignores RLIMIT_AS for the large pyarrow/libpq
# arena maps, so the tests @linux_only-skip there. This image lets the lane run them for real
# on a macOS host by executing an arm64-native Linux container with a real PostgreSQL.
#
# pgserver has NO linux/aarch64 wheel and its bundled initdb SIGSEGVs under amd64 QEMU on
# Apple Silicon, so we install a native apt PostgreSQL and point the test at it via
# PROVISA_MEM_TEST_DSN instead of using embedded pgserver.
FROM python:3.12-slim

RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
      postgresql postgresql-client libpq-dev gcc g++ >/dev/null && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /repo
