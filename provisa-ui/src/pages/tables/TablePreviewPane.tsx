// Copyright (c) 2026 Kenneth Stott
// Canary: b7a3a92c-702a-4da2-9f62-f7ed7e2ff909
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table's rows, looked at inside its detail panel: the way back to the detail, the line that
// says a draft is not yet shown to readers, and the one governed viewer every preview uses. A
// working surface is never a dialog over the page.

import { Button, Text } from "@mantine/core";
import { ArrowLeft } from "lucide-react";
import { useTranslation } from "react-i18next";
import { GovernedTableViewer } from "../../components/GovernedTableViewer";
import type { ListedTable } from "../../types/admin";

export function TablePreviewPane({
  table,
  role = null,
  onBack,
}: {
  /** A registered table, or a list row of one: the viewer reads what a list row holds. */
  table: ListedTable;
  /** The role the rows are read as; null reads as the caller. */
  role?: string | null;
  onBack: () => void;
}) {
  const { t } = useTranslation();
  return (
    <>
      <div className="split-pane__bar">
        <Button
          variant="subtle"
          size="compact-sm"
          leftSection={<ArrowLeft size={14} />}
          onClick={onBack}
          data-testid="table-preview-back"
        >
          {t("tablesPage.backToDetail")}
        </Button>
      </div>
      {/* REQ-1921: a draft's preview is the same viewer; it reads through the admin preview, as
          the caller, and says whom the table is not yet shown to. */}
      {table.draft && (
        <Text size="sm" c="dimmed" data-testid="table-preview-draft-note">
          {t("tableReadView.draftPreviewNote")}
        </Text>
      )}
      <GovernedTableViewer key={table.id} table={table} role={role} />
    </>
  );
}
