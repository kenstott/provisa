// Copyright (c) 2026 Kenneth Stott
// Canary: c71b5e92-4a08-4d36-9f27-e0a6b3d8152c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: the page toolbar's control swapping the ERD with the list, filled while the view is on,
// as the Ontology view's is.

import { ActionIcon } from "@mantine/core";
import { Network } from "lucide-react";
import { useErdView } from "./useErdView";

/** The ERD view's on/off icon. What it draws is the page header's command description
 *  (PageCommand), so the icon carries no card of its own. */
export function ErdToggle({ page, label }: { page: "tables" | "relationships"; label: string }) {
  const { on, setOn } = useErdView();
  return (
    <ActionIcon
      data-tour={`${page === "tables" ? "tables" : "rels"}-erd`}
      data-testid={`${page}-erd-toggle`}
      variant={on ? "light" : "subtle"}
      aria-label={label}
      aria-pressed={on}
      onClick={() => setOn(!on)}
    >
      <Network size={14} />
    </ActionIcon>
  );
}
