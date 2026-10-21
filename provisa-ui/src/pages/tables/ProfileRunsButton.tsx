// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123 (amended 2026-10-21): View Profile Runs in the table's read-only panel too, beside the
// profiler it belongs to; it opens the same modal the edit panel's button does.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Box, Button } from "@mantine/core";
import type { RegisteredTable } from "../../types/admin";
import { ProfileRunsModal } from "./ProfileRunsModal";

export function ProfileRunsButton({ table }: { table: RegisteredTable }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  return (
    <Box px="0.75rem" py="0.5rem">
      <Button
        size="xs"
        variant="light"
        data-testid="table-read-view-profile-runs"
        onClick={() => setOpen(true)}
      >
        {t("profilerPanel.viewRuns")}
      </Button>
      {open && (
        <ProfileRunsModal
          tableId={table.id}
          tableName={table.tableName}
          onClose={() => setOpen(false)}
        />
      )}
    </Box>
  );
}
