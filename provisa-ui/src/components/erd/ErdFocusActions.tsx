// Copyright (c) 2026 Kenneth Stott
// Canary: 4d8a0f37-b6e2-4c19-8e54-3f7c1a9d62e0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: a table panel's head while the ERD is the view: Focus, and while a focus is on Add to
// focus. Outside the ERD view they are not there.

import { useTranslation } from "react-i18next";
import { ActionIcon } from "@mantine/core";
import { Crosshair, ListPlus } from "lucide-react";
import { Tip } from "../Tip";
import { PanelActions } from "../list/SplitPane";
import { catalogTableName } from "../../naming";
import type { ListedTable } from "../../types/admin";
import { addTableToFocus, focusIsOn, focusOnTable } from "./erd-focus";
import { useErdView } from "./useErdView";

export function ErdFocusActions({ table }: { table: Pick<ListedTable, "domainId" | "tableName"> }) {
  const { t } = useTranslation();
  const { on, focus, setFocus } = useErdView();
  if (!on) return null;
  return (
    <PanelActions>
      <Tip text={t("erdModal.focus")} named>
        <ActionIcon
          aria-label={t("erdModal.focus")}
          onClick={() => setFocus(focusOnTable(table))}
          data-testid="erd-focus-table"
        >
          <Crosshair size={16} />
        </ActionIcon>
      </Tip>
      {focusIsOn(focus) && (
        <Tip text={t("erdModal.addToFocus")} named>
          <ActionIcon
            aria-label={t("erdModal.addToFocus")}
            disabled={focus.objects.includes(catalogTableName(table))}
            onClick={() => setFocus(addTableToFocus(focus, table))}
            data-testid="erd-add-to-focus"
          >
            <ListPlus size={16} />
          </ActionIcon>
        </Tip>
      )}
    </PanelActions>
  );
}
