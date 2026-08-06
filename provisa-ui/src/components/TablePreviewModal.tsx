// Copyright (c) 2026 Kenneth Stott
// Canary: b3369936-efb2-4360-9f55-b35811c4c45e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Table data preview modal — thin shell around the shared GovernedTableViewer.

import { Modal, Text } from "@mantine/core";
import type { RegisteredTable } from "../types/admin";
import { GovernedTableViewer } from "./GovernedTableViewer";

interface TablePreviewModalProps {
  table: RegisteredTable | null;
  onClose: () => void;
}

export function TablePreviewModal({ table, onClose }: TablePreviewModalProps) {
  return (
    <Modal
      opened={table != null}
      onClose={onClose}
      size="90%"
      title={
        <Text fw={600} ff="monospace">
          {table ? (table.alias || table.tableName) : ""}
        </Text>
      }
      data-testid="table-preview-modal"
    >
      {table && (
        <div style={{ height: "70vh", display: "flex", flexDirection: "column" }}>
          <GovernedTableViewer key={table.id} table={table} />
        </div>
      )}
    </Modal>
  );
}
