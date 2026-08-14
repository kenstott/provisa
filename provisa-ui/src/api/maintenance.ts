// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7a4d18-6c3b-4f5a-9d21-70b8c1e4a9f3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

/**
 * REQ-1466: the deployment-wide scheduled-maintenance notice.
 *
 * The banner's wording is composed by the server, not here, so one deployment says one thing on
 * every surface. `message` is therefore always populated when `active` is true.
 */
export interface MaintenanceNotice {
  active: boolean;
  message: string | null;
  // ISO-8601, or null when no estimate is being offered.
  ends_at: string | null;
  started_at: string | null;
}

export async function fetchMaintenanceNotice(): Promise<MaintenanceNotice> {
  const res = await fetch("/admin/platform/maintenance");
  if (!res.ok) throw new Error(requestFailed("Maintenance notice", res.status));
  return res.json();
}

export async function setMaintenanceNotice(body: {
  active: boolean;
  message: string | null;
  ends_at: string | null;
}): Promise<MaintenanceNotice> {
  const res = await fetch("/admin/platform/maintenance", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(serverMessage(await res.json().catch(() => null), "Maintenance notice update"));
  }
  return res.json();
}
