// Copyright (c) 2026 Kenneth Stott
// Canary: a675220c-44d8-4264-9409-448bbf74ba87
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Box, Select } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { DataProduct, RegisteredTable, Relationship } from "../../types/admin";
import { FormField, FormLayout } from "../../components/form/FormLayout";
import { TableAssociations } from "./TableAssociations";

/** The Associations section of a table's form: its data product and its relationships. */
export function TableAssociationsSection({
  editingTable,
  setEditingTable,
  dataProducts,
  relationships,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  dataProducts: DataProduct[];
  relationships: Relationship[];
}) {
  const { t } = useTranslation();
  return (
    <>
      <FormLayout className="form-card" data-testid="table-section-associations-panel">
        {/* REQ-1443 PRODUCT ASSIGNMENT: a checker table is assigned as any member is. */}
        <FormField size="medium" hint={t("tableEditForm.dataProductDesc")}>
          <Select
            label={t("tableEditForm.dataProductLabel")}
            placeholder={t("tableEditForm.dataProductNone")}
            clearable
            // REQ-1634: a table may only join a data product owned by its own domain.
            data={dataProducts
              .filter((p) => p.domainId === editingTable.domainId)
              .map((p) => ({ value: p.id, label: p.name }))}
            value={editingTable.productId}
            onChange={(value) =>
              setEditingTable({
                ...editingTable,
                productId: value,
              })
            }
            data-testid="table-edit-product-id"
          />
        </FormField>
      </FormLayout>
      <Box mt="md">
        <TableAssociations table={editingTable} relationships={relationships} />
      </Box>
    </>
  );
}
