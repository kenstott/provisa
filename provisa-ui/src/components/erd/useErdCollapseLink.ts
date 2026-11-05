// Copyright (c) 2026 Kenneth Stott
// Canary: e1a7c340-95d2-4b06-b3f8-2c6d7f01a9b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";

/** The ERD's collapsed domains and its link: `?collapsed=` is taken when it changes, and the
 * reader's clicks stand after that; what is collapsed is reported to the page. */
export function useErdCollapseLink(
  collapsed: string | null,
  onCollapsed: ((collapsed: string[] | "all") => void) | undefined,
  visibleDomainIds: string[],
  collapsedDomains: Set<string>,
  setCollapsedDomains: (domains: Set<string>) => void,
): void {
  const [seeded, setSeeded] = useState<string | null>(null);
  if (collapsed !== seeded) {
    setSeeded(collapsed);
    if (collapsed !== null) {
      setCollapsedDomains(
        new Set(collapsed === "all" ? visibleDomainIds : collapsed.split(",").filter(Boolean)),
      );
    }
  }
  const every =
    visibleDomainIds.length > 0 && visibleDomainIds.every((id) => collapsedDomains.has(id));
  const key = every ? "all" : [...collapsedDomains].sort().join(",");
  useEffect(() => {
    onCollapsed?.(key === "all" ? "all" : key.split(",").filter(Boolean));
  }, [key, onCollapsed]);
}
