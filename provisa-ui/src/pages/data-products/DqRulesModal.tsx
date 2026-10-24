// Copyright (c) 2026 Kenneth Stott
// Canary: 4b8e1c27-9d3a-4f60-b2c5-7e1a9f0d3c58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1443 clause 10: the product detail's Data Quality panel opens a checker table's contract as
// the rules it enforces — read-only, parsed by the same server-side parser the table editor uses,
// so soda and GX contracts read the same way here without a client-side YAML/JSON reimplementation.
import { useTranslation } from "react-i18next";
import { Modal, Table, Text } from "@mantine/core";
import type { CoreDqRule } from "./productCore";

interface DqRulesModalProps {
  opened: boolean;
  onClose: () => void;
  /** The contract's checks as meta's dq_rules serves them, parsed at registration (REQ-1443). */
  rules: CoreDqRule[];
  /** The table the contract scans. */
  scans: { domain_id: string; table_name: string } | null;
  tableName: string;
}

export function DqRulesModal({ opened, onClose, rules, scans, tableName }: DqRulesModalProps) {
  const { t } = useTranslation();
  return (
    <Modal
      opened={opened}
      onClose={onClose}
      size="90%"
      title={t("dataProductsTab.detail.dqRulesTitle", { table: tableName })}
      styles={{
        content: { height: "90vh" },
        body: { height: "calc(90vh - 3.5rem)", overflow: "auto" },
      }}
    >
      {scans !== null && (
        <Text size="xs" c="dimmed" data-testid="dq-rules-dataset">
          {t("dataQualityPanel.dataset", { dataset: `${scans.domain_id}.${scans.table_name}` })}
        </Text>
      )}
      {rules.length === 0 && (
        <Text size="sm" c="var(--text-muted)">
          {t("dataProductsTab.detail.dqRulesEmpty")}
        </Text>
      )}
      {rules.length > 0 && (
        <Table className="data-table" data-testid="dq-rules-rows">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("dataQualityPanel.columnHeader")}</Table.Th>
              <Table.Th>{t("dataQualityPanel.checkHeader")}</Table.Th>
              <Table.Th>{t("dataQualityPanel.definitionHeader")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rules.map((c) => (
              <Table.Tr key={c.position}>
                <Table.Td>{c.column_name || t("dataQualityPanel.datasetScope")}</Table.Td>
                <Table.Td>{c.check_type}</Table.Td>
                <Table.Td>
                  <Text
                    component="pre"
                    size="xs"
                    ff="monospace"
                    style={{ margin: 0, whiteSpace: "pre-wrap" }}
                  >
                    {c.definition}
                  </Text>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Modal>
  );
}
