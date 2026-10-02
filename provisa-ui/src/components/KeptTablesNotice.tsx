// Copyright (c) 2026 Kenneth Stott
// Canary: 9dd12821-e456-41b4-96ab-c09ff4c678da
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Alert, List } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { referrersOf } from "../lib/keptTables";
import type { KeptTable } from "../lib/keptTables";

/** REQ-1918: after a remote source is registered again, the tables it KEPT although the remote
 *  no longer has them (or no longer has some of their columns), because something still refers
 *  to them. The registration went through; this says what it left in place and why, so the
 *  person can remove what refers to each and register again. Renders nothing when none. */
export function KeptTablesNotice({ kept, onClose }: { kept: KeptTable[]; onClose: () => void }) {
  const { t } = useTranslation();
  if (kept.length === 0) return null;
  return (
    <Alert
      color="yellow"
      mb="md"
      withCloseButton
      onClose={onClose}
      title={t("keptTables.title", { count: kept.length })}
      data-testid="kept-tables"
    >
      {t("keptTables.intro")}
      <List size="sm" mt="xs">
        {kept.map((table) => (
          <List.Item key={`${table.id}:${table.name}`}>
            {t("keptTables.item", { table: table.name, referrers: referrersOf(table).join(", ") })}
          </List.Item>
        ))}
      </List>
    </Alert>
  );
}
