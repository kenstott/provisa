// Copyright (c) 2026 Kenneth Stott
// Canary: 1b6e4d2f-9a37-4c05-b8e1-3f7a2c9d6e10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useDomains, useRoles } from "../../hooks/useAdminQueries";
import { LocalUsersTab } from "./LocalUsersTab";

// REQ-1327: Team's Local users section, with the org's roles and domains a user is assigned to.
export function TeamLocalUsers() {
  const { roles } = useRoles();
  const { domains } = useDomains();
  return (
    <LocalUsersTab
      allRoles={roles.map((r) => r.id)}
      allDomains={domains.filter((d) => d.id !== "").map((d) => d.id)}
    />
  );
}
