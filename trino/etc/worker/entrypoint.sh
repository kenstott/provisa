#!/bin/bash
# Generate unique node.id from container hostname before starting Trino.
# The trinodb image has no `hostname` binary, so read /etc/hostname (always the
# container id) — a bare $(hostname) yields an empty, invalid node.id.
set -euo pipefail

cat > /etc/trino/node.properties <<EOF
node.environment=docker
node.data-dir=/data/trino
node.id=$(cat /etc/hostname)
EOF

exec /usr/lib/trino/bin/run-trino "$@"
