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
# The run-time Neo4j store (docker-compose.run.yml): the first time, the database the bundle build
# dumped (scripts/demo_bundle.py: dump/neo4j/neo4j.dump) is loaded, then Neo4j starts as its image
# starts it, every database read only (NEO4J_dbms_databases_default__to__read__only).
set -euo pipefail
# Loaded until a load has run to its end: the marker, in the data volume, is written after it, and
# a load cut off half-way (a database directory and no marker) is loaded again over itself.
if [ ! -f /data/.provisa-init-complete ]; then
  neo4j-admin database load neo4j --from-path=/dumps --overwrite-destination=true
  touch /data/.provisa-init-complete
fi
exec /startup/docker-entrypoint.sh neo4j
