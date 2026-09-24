// Copyright (c) 2026 Kenneth Stott
// Canary: 7e2b4a91-3d6c-4f18-9a05-8c1e6b7d2f40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Badge, Tooltip } from "@mantine/core";
import { BadgeInfo } from "lucide-react";
import { fetchLicenseStatus } from "../api/license";

/**
 * A persistent, always-visible "Unregistered" indicator for an unlicensed deployment (companion to
 * REQ-1137's post-trial nag, not a replacement for it).
 *
 * REQ-1137's nag only fires after the 30-day trial expires, and only through server logs and each
 * protocol's out-of-band notice channel — never the UI. Distinct decision from this session: show
 * this footer badge from day one whenever unlicensed, so registration is discoverable without
 * waiting on the trial or reading server logs. It never gates functionality and disappears the
 * moment a valid license is applied.
 *
 * Polled rather than pushed, matching MaintenanceBanner: a poll failure keeps the last known state
 * rather than flickering the badge on a transient network error.
 */
const POLL_MS = 300_000;

export function LicenseBadge() {
  const [licensed, setLicensed] = useState<boolean | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = () => {
      fetchLicenseStatus()
        .then((next) => {
          if (!cancelled) setLicensed(next.licensed);
        })
        .catch(() => {
          /* keep the last known state; see the poll note above */
        });
    };
    load();
    const timer = window.setInterval(load, POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  if (licensed !== false) return null;

  return (
    <Tooltip
      label="Provisa is free to register. Registered users' bug reports get priority over unregistered users (paid customers first), plus emails on major releases, use-case walkthroughs, and feature deep dives. Visit provisa.dev/register, or run `provisa license status` for your machine ID."
      multiline
      w={300}
      withArrow
    >
      <Badge
        component="a"
        href="https://provisa.dev/register"
        target="_blank"
        rel="noreferrer"
        variant="light"
        color="gray"
        leftSection={<BadgeInfo size={12} />}
        data-testid="license-badge"
        style={{
          position: "fixed",
          bottom: 8,
          left: 8,
          zIndex: 100,
          cursor: "pointer",
        }}
      >
        Unregistered
      </Badge>
    </Tooltip>
  );
}
