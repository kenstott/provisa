# The store's first start ran every init script before this one to its end (docker-entrypoint
# runs them in name order, and only on an empty volume): its healthcheck requires this marker, so
# a start cut off half-way is never taken for a loaded store, and the node's sync
# (scripts/demo_stores_sync.sh) recreates its volume. PGDATA for Postgres; /data/db for MongoDB.
touch "${PGDATA:-/data/db}/.provisa-init-complete"
