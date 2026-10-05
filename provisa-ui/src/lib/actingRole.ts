// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7e4b91-5c3a-4f68-a0e2-9b1f6c8d3a57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import type { Role } from "../types/auth";

/**
 * The X-Provisa-Role value for the active roles (REQ-1620): one role, or under "Role: All" the
 * comma-separated set, which the server serves as their meta-role -- its schema, its tables. A
 * page that sent only the first role's id was served that one role's schema. Null with no role.
 */
export function actingRoleHeader(selectedRoles: Role[]): string | null {
  return selectedRoles.length > 0 ? selectedRoles.map((r) => r.id).join(",") : null;
}
