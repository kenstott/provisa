// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Box, Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { DataProduct, RegisteredTable, Relationship } from "../../types/admin";
import { FormField, FormLayout } from "../../components/form/FormLayout";
import { TableAssociations } from "./TableAssociations";

/** The Associations section of a table's form: its data product and its relationships. */
export function TableAssociationsSection({
  editingTable,
  setEditingTable,
  isCheckerTable,
  dataProducts,
  relationships,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  isCheckerTable: boolean;
  dataProducts: DataProduct[];
  relationships: Relationship[];
}) {
  const { t } = useTranslation();
  return (
    <>
      <FormLayout className="form-card" data-testid="table-section-associations-panel">
        {isCheckerTable ? (
          // REQ-1443 clause 10: a checker table's membership is inherited from the table its
          // contract scans — the server derives it, so it is shown, never chosen.
          <FormField size="medium" hint={t("tableEditForm.dataProductInherited")}>
            <TextInput
              label={t("tableEditForm.dataProductLabel")}
              value={
                editingTable.productId == null
                  ? t("tableEditForm.dataProductNone")
                  : (dataProducts.find((p) => p.id === editingTable.productId)?.name ??
                    editingTable.productId)
              }
              readOnly
              disabled
              data-testid="table-edit-product-id"
            />
          </FormField>
        ) : (
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
        )}
      </FormLayout>
      <Box mt="md">
        <TableAssociations table={editingTable} relationships={relationships} />
      </Box>
    </>
  );
}
