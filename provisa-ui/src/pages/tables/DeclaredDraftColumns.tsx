// Copyright (c) 2026 Kenneth Stott
// Canary: 8e1c4a7f-3b92-4d6e-9f05-7a2d6c8b1e34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1924, REQ-2234: an OpenAPI table whose operation declares no response schema is registered
// on its own, opened unsaved in its detail panel; its columns are declared here, in the Columns
// section, on the table as it will be registered. Save registers it with them.

import { useEffect, useState } from "react";
import { fetchIrTypes } from "../../api/admin";
import { FormLayout } from "../../components/form/FormLayout";
import { IR_TYPES_FALLBACK } from "../../irTypes";
import type { RegisteredTable, TableColumn } from "../../types/admin";
import { DeclaredColumns } from "./DeclaredColumns";
import { draftColumn } from "./draftTable";
import type { ColumnForm } from "./types";

/** A draft's column as the declared-columns editor holds it. */
function formOf(c: TableColumn): ColumnForm {
  return {
    name: c.columnName,
    visibleTo: c.visibleTo ?? [],
    masks: c.masks ?? [],
    alias: c.alias ?? "",
    description: c.description ?? "",
    selected: true,
    nativeFilterType: c.nativeFilterType,
    nativeFilterRequired: c.nativeFilterRequired,
    requiredBySteward: c.requiredBySteward,
    dataType: c.dataType ?? "",
    subtype: c.subtype,
    isPrimaryKey: c.isPrimaryKey,
    scope: c.scope,
    path: c.path,
  };
}

/** The editor's column back on the draft, as the create step would have registered it. */
function columnOf(c: ColumnForm, i: number): TableColumn {
  return draftColumn(
    {
      name: c.name,
      dataType: c.dataType || undefined,
      subtype: c.subtype ?? undefined,
      visibleTo: c.visibleTo,
      masks: c.masks,
      alias: c.alias || undefined,
      description: c.description || undefined,
      nativeFilterType: c.nativeFilterType ?? undefined,
      nativeFilterRequired: c.nativeFilterRequired ?? undefined,
      requiredBySteward: c.requiredBySteward,
      isPrimaryKey: c.isPrimaryKey,
      scope: c.scope,
      path: c.path ?? undefined,
    },
    i,
  );
}

export function DeclaredDraftColumns({
  editingTable,
  setEditingTable,
  visibleTo,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  /** The roles a declared column is visible to when it is added. */
  visibleTo: string[];
}) {
  const [irTypes, setIrTypes] = useState<string[]>(IR_TYPES_FALLBACK);
  useEffect(() => {
    fetchIrTypes()
      .then((types) => {
        if (types.length > 0) setIrTypes(types);
      })
      .catch(() => setIrTypes(IR_TYPES_FALLBACK));
  }, []);
  return (
    <FormLayout style={{ marginBottom: "0.75rem" }}>
      <DeclaredColumns
        sourceId={editingTable.sourceId}
        tableName={editingTable.tableName}
        columns={editingTable.columns.map(formOf)}
        setColumns={(columns) =>
          setEditingTable((current) =>
            current === null ? current : { ...current, columns: columns.map(columnOf) },
          )
        }
        irTypes={irTypes}
        visibleTo={visibleTo}
      />
    </FormLayout>
  );
}
