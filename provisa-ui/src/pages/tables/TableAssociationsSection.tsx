// Copyright (c) 2026 Kenneth Stott
// Canary: a675220c-44d8-4264-9409-448bbf74ba87
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import type { DataProduct, RegisteredTable, Relationship } from "../../types/admin";
import { TableAssociations } from "./TableAssociations";
import { dataProductField } from "./tableSectionFields";

/** The table's associations. Its data products are read-only here: a product's members are set on
 * the product, in the Products page (REQ-1634). */
export function TableAssociationsSection({
  editingTable,
  dataProducts,
  relationships,
}: {
  editingTable: RegisteredTable;
  dataProducts: DataProduct[];
  relationships: Relationship[];
}) {
  const { t } = useTranslation();
  return (
    <TableAssociations
      table={editingTable}
      relationships={relationships}
      leading={[dataProductField(t, editingTable, dataProducts)]}
    />
  );
}
