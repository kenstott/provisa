// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2c9a14-3b58-4f07-91d6-a4e8c0b5d273
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: the table editor's catalogs: the panel (links, provenance, Refresh, the lock), the pick
// list and review, and what an import wrote carried into the form so its next Save keeps it.

import { useState } from "react";
import { Alert } from "@mantine/core";
import type { RegisteredTable } from "../../types/admin";
import { lockTableToCatalog, type Imported } from "../../api/catalogLinks";
import { FormField } from "../../components/form/FormLayout";
import { CatalogPickModal } from "./CatalogPickModal";
import { TableCatalogPanel } from "./TableCatalogPanel";
import type { TableCatalogs } from "./useTableCatalogs";

/** The pick list's state: closed (undefined), From catalog (null), or Refresh from a catalog. */
export type CatalogPick = string | null | undefined;

export function TableCatalogSection({
  tableId,
  catalogs,
  pick,
  onPick,
  setEditingTable,
}: {
  tableId: number;
  catalogs: TableCatalogs;
  pick: CatalogPick;
  onPick: (pick: CatalogPick) => void;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
}) {
  const [locking, setLocking] = useState(false);
  const takeImported = (imported: Imported) => {
    setEditingTable((prev) =>
      prev
        ? {
            ...prev,
            description:
              imported.description !== undefined ? imported.description || null : prev.description,
            columns: prev.columns.map((c) =>
              imported.columns && c.columnName in imported.columns
                ? { ...c, description: imported.columns[c.columnName] || null }
                : c,
            ),
          }
        : prev,
    );
    void catalogs.reload();
  };
  return (
    <>
      {catalogs.problem && (
        <FormField size="full">
          <Alert color="red" data-testid="table-catalogs-problem">
            {catalogs.problem}
          </Alert>
        </FormField>
      )}
      {catalogs.catalogs.length > 0 && (
        <FormField size="full">
          <TableCatalogPanel
            catalogs={catalogs.catalogs}
            editUrl={catalogs.locked?.link?.url ?? null}
            locking={locking}
            onRefresh={(catalog) => onPick(catalog)}
            onLock={async (catalog) => {
              setLocking(true);
              try {
                await lockTableToCatalog(tableId, catalog);
                await catalogs.reload();
              } catch (e) {
                catalogs.report(String(e));
              } finally {
                setLocking(false);
              }
            }}
          />
          <CatalogPickModal
            tableId={tableId}
            opened={pick !== undefined}
            refresh={pick ?? null}
            onClose={() => onPick(undefined)}
            onApplied={(_catalog, imported) => takeImported(imported)}
          />
        </FormField>
      )}
    </>
  );
}
