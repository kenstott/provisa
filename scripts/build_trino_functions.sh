#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# Build Provisa's Trino function plugin (trino-functions/, REQ-1494) into
# trino/plugins/provisa-functions/. The build runs in a Maven container on the toolchain the
# engine's other plugins use (Java 25), so the host needs only Docker. The Maven repository is
# cached in ~/.m2.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/trino/plugins/provisa-functions"

docker run --rm \
  -v "$root/trino-functions:/src" \
  -v "$HOME/.m2:/root/.m2" \
  -w /src \
  maven:3.9-eclipse-temurin-25 \
  mvn -B -q package

mkdir -p "$out"
rm -f "$out"/*.jar
cp "$root/trino-functions/target/provisa-functions-1.jar" "$out/"
echo "built $out/provisa-functions-1.jar"
