# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# The catalog as the bundle build dumped it (scripts/demo_bundle.py), restored at the first start.
set -euo pipefail
mongorestore --quiet --gzip --archive=/dump/mongodb.archive.gz \
  --nsExclude='admin.*' --nsExclude='config.*' --nsExclude='local.*' \
  --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" \
  --authenticationDatabase admin
