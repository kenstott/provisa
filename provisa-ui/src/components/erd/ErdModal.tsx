// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Modal, useComputedColorScheme } from "@mantine/core";
import { ErdPanel } from "./ErdPanel";
import { getErdPalette } from "./sections/erd-palette";
import type { ErdModalProps } from "./sections/erd-types";

// Modal chrome around ErdPanel — used by the Relationships page. The Schema
// page embeds ErdPanel directly (no overlay) as its "ERD" sub-tab.
export function ErdModal({ tables, relationships, domains, checkedDomains, onClose }: ErdModalProps) {
  const colorScheme = useComputedColorScheme("dark");
  const palette = getErdPalette(colorScheme === "dark");
  return (
    <Modal
      opened
      onClose={onClose}
      withCloseButton={false}
      centered
      size="92vw"
      data-testid="erd-modal"
      styles={{
        content: {
          height: "88vh",
          maxHeight: "88vh",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          background: palette.bg,
        },
        body: {
          padding: 0,
          display: "flex",
          flexDirection: "column",
          flex: 1,
          overflow: "hidden",
        },
      }}
    >
      <ErdPanel
        tables={tables}
        relationships={relationships}
        domains={domains}
        checkedDomains={checkedDomains}
        onClose={onClose}
      />
    </Modal>
  );
}

export type { ErdNodeDomain, ErdNodeTable } from "./ErdPanel";
