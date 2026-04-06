#!/bin/bash
# Generate unique node.id from container hostname before starting Trino.
set -euo pipefail

cat > /etc/trino/node.properties <<EOF
node.environment=docker
node.data-dir=/data/trino
node.id=$(hostname)
EOF

exec /usr/lib/trino/bin/run-trino "$@"
