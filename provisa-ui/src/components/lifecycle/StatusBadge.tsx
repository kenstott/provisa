// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7f4a91-5c3e-4b18-9a06-e1b8c4f72d35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The lifecycle every governed object shares -- a glossary term (REQ-2144), a data product: draft,
// live, deprecated, retired -- and its one badge. The object's page names each status in its own
// words; the colours are the lifecycle's.

import { Badge } from "@mantine/core";
import { Tip } from "../Tip";

export type LifecycleStatus = "draft" | "live" | "deprecated" | "retired";

const COLOUR: Record<LifecycleStatus, string> = {
  draft: "yellow",
  live: "green",
  deprecated: "gray",
  retired: "orange",
};

/** A status, named by ``label``; ``help``, where given, behind it as a tip. */
export function StatusBadge({
  status,
  label,
  help,
  testId,
}: {
  status: LifecycleStatus;
  label: string;
  help?: string;
  testId?: string;
}) {
  const badge = (
    <Badge
      size="xs"
      variant="light"
      color={COLOUR[status]}
      data-status={status}
      data-testid={testId}
    >
      {label}
    </Badge>
  );
  return help ? <Tip text={help}>{badge}</Tip> : badge;
}
