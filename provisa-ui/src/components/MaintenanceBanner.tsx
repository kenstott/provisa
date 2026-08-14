// Copyright (c) 2026 Kenneth Stott
// Canary: 9c1e5b73-2a84-4d10-bf6e-53a7d09c4e28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Alert } from "@mantine/core";
import { Wrench } from "lucide-react";
import { fetchMaintenanceNotice, type MaintenanceNotice } from "../api/maintenance";

/**
 * REQ-1466: the deployment-wide scheduled-downtime message.
 *
 * Planned work that replaces the engine cluster — switching `var.engine_cluster_mode` between the
 * Autopilot and Standard topologies is the case that forces it (REQ-1465) — takes the data plane
 * down for minutes. Without this the user sees queries failing and has no way to tell scheduled
 * work from an outage, so the banner sits above the whole app, on every route, for the duration.
 *
 * Not dismissible: the point is that it is still true, and a query issued after dismissing it fails
 * exactly the same way.
 *
 * Polled rather than pushed, because the window ends when the platform admin says it ends and the
 * client should clear the banner without a reload. A poll that fails leaves the banner in whatever
 * state it was last known to be — a transport error during a maintenance window is not evidence the
 * window is over.
 */
const POLL_MS = 60_000;

export function MaintenanceBanner() {
  const [notice, setNotice] = useState<MaintenanceNotice | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = () => {
      fetchMaintenanceNotice()
        .then((next) => {
          if (!cancelled) setNotice(next);
        })
        .catch(() => {
          /* keep the last known notice; see the poll note above */
        });
    };
    load();
    const timer = window.setInterval(load, POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  if (!notice?.active) return null;

  const endsAt = notice.ends_at ? new Date(notice.ends_at) : null;
  const expected = endsAt
    ? `Expected back by ${endsAt.toLocaleString()}.`
    : "No end time has been given yet.";

  return (
    <Alert
      variant="filled"
      color="orange"
      radius={0}
      icon={<Wrench size={18} />}
      title="Scheduled maintenance"
      data-testid="maintenance-banner"
    >
      {notice.message} {expected}
    </Alert>
  );
}
