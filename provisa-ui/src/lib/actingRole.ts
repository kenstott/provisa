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

/** The header the server reads the acting roles from (REQ-273, REQ-1620). */
export const ROLE_HEADER = "X-Provisa-Role";

// The roles every request acts as: the role picker's selection, as its header value. Rights follow
// the roles a request names, not every role the user holds, so an admin request carries the
// selection too -- the same one the UI's capability gates read. Set by the auth context.
let acting: string | null = null;

export function setActingRoles(header: string | null): void {
  acting = header;
}

/** The selected roles' header value, or null before any role is known. */
export function currentActingRoles(): string | null {
  return acting;
}

