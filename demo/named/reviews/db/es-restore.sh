#!/bin/bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# The run-time Elasticsearch store (docker-compose.run.yml): Elasticsearch starts as its image
# starts it, and the first time, the indices the bundle build snapshotted (scripts/demo_bundle.py:
# dump/elasticsearch/) are restored from the read-only repository mounted at /snapshots and closed
# to writes. The repository, once registered, marks the store restored.
set -euo pipefail
/usr/local/bin/docker-entrypoint.sh eswrapper &
es=$!
es_api() { curl -sf -H 'Content-Type: application/json' "$@"; }
until es_api "localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null; do
  kill -0 "$es" || exit 1
  sleep 2
done
# The marker, in the data volume, is written when the restore has run to its end. A volume with
# the repository registered and no marker is a restore cut off half-way: it is left unmarked, so
# the store stays unhealthy and the node's sync (scripts/demo_stores_sync.sh) recreates its volume.
marker=/usr/share/elasticsearch/data/.provisa-init-complete
if [ ! -f "$marker" ] && ! es_api "localhost:9200/_snapshot/bundle" >/dev/null 2>&1; then
  es_api -XPUT localhost:9200/_snapshot/bundle \
    -d '{"type": "fs", "settings": {"location": "/snapshots", "readonly": true}}' >/dev/null
  es_api -XPOST "localhost:9200/_snapshot/bundle/data/_restore?wait_for_completion=true" \
    -d '{"include_global_state": false}' >/dev/null
  es_api -XPUT "localhost:9200/_all/_settings" -d '{"index.blocks.write": true}' >/dev/null
  touch "$marker"
fi
wait "$es"
