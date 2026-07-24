# Base image for the linux-mem lane (scripts/run-linux-mem-lane.sh).
#
# The RLIMIT_AS memory-bounded tests in tests/integration/test_streaming_memory_bounded_e2e.py
# only exercise real behavior on Linux — macOS ignores RLIMIT_AS for the large pyarrow/libpq
# arena maps, so the tests @linux_only-skip there. This image lets the lane run them for real
# on a macOS host by executing an arm64-native Linux container with a real PostgreSQL.
#
# pgserver has NO linux/aarch64 wheel, so the test can't use embedded pgserver here. It talks
# to the harness postgres (docker-compose.core.yml) over host.docker.internal via
# PROVISA_MEM_TEST_DSN instead — so this image needs only the libpq client + a toolchain for
# any wheels that build from source, not a PostgreSQL server.
FROM python:3.12-slim

RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
      libpq-dev gcc g++ >/dev/null && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /repo
