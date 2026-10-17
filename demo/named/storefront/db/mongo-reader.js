// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// The reader Provisa reads the storefront's catalog as: it writes nothing.
db.getSiblingDB("admin").createUser({
  user: "storefront_reader",
  pwd: "storefront_reader",
  roles: [{ role: "read", db: "catalog" }],
});
