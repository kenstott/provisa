#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# Build Provisa's Trino function plugin (trino-functions/, REQ-1494) into
# trino/plugins/provisa-functions/. The build runs in a Maven container on the toolchain the
# engine's other plugins use (Java 25), so the host needs only Docker. The Maven repository is
# cached in ~/.m2.
#
# The container runs as the CALLING user. Run as the image's root it left root-owned directories
# in ~/.m2 (and in trino-functions/target), and a Maven run on the host afterwards could not
# write its own repository: the JDBC driver's integration test failed in CI with
# "AccessDeniedException: /home/runner/.m2/repository/..." (run 37824835227, core 4/6). The
# official image's way to run as another user: MAVEN_CONFIG and user.home name a home that user
# can write. ~/.m2 is made first, or Docker creates the mount point itself, as root.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/trino/plugins/provisa-functions"

mkdir -p "$HOME/.m2"
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e MAVEN_CONFIG=/var/maven/.m2 \
  -v "$root/trino-functions:/src" \
  -v "$root/provisa/fakes/portable:/provisa/fakes/portable:ro" \
  -v "$HOME/.m2:/var/maven/.m2" \
  -w /src \
  maven:3.9-eclipse-temurin-25 \
  mvn -B -q -Duser.home=/var/maven package

mkdir -p "$out"
rm -f "$out"/*.jar
cp "$root/trino-functions/target/provisa-functions-1.jar" "$out/"
echo "built $out/provisa-functions-1.jar"
