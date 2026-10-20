# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# A demo bundle's Postgres store run as the demo's own container (start-ui-install.sh --demo):
# the postgres image's entrypoint runs this once, on an empty volume. Each dump the bundle build
# wrote (/dumps/<database>.sql, its dump/<service>/ mounted) loads into a database of its name;
# the image's POSTGRES_DB is created already. The sandbox tier's node loads the same dumps into
# its shared instance instead (scripts/demo_stores.py). No interpreter line, as
# db/init-complete.sh: the entrypoint runs it, or sources it, either way in bash.
# Sourced into the entrypoint, it sets nothing the entrypoint's own code then runs under.
loaded=0
for dump in /dumps/*.sql; do
  [ -f "$dump" ] || continue
  db="$(basename "$dump" .sql)"
  if [ "$db" != "$POSTGRES_DB" ]; then
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE \"$db\"" || exit 1
  fi
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$db" -f "$dump" || exit 1
  loaded=$((loaded + 1))
done
if [ "$loaded" -eq 0 ]; then
  echo "no /dumps/<database>.sql to load" >&2
  exit 1
fi
