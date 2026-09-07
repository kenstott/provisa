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
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Modal, Table, Text } from "@mantine/core";
import { useDqContract } from "../../hooks/useAdminQueries";
import type { DqCheck } from "../../types/admin";

interface DqRulesModalProps {
  opened: boolean;
  onClose: () => void;
  /** The checker source type the scan runs through — soda | great_expectations. */
  checker: string;
  contractText: string;
  tableName: string;
}

export function DqRulesModal({
  opened,
  onClose,
  checker,
  contractText,
  tableName,
}: DqRulesModalProps) {
  const { t } = useTranslation();
  const { parseContract } = useDqContract();
  const [dataset, setDataset] = useState<string | null>(null);
  const [checks, setChecks] = useState<DqCheck[] | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);

  useEffect(() => {
    if (!opened) return;
    let live = true;
    void parseContract({ checker, contractText }).then((parsed) => {
      if (!live || parsed === null) return;
      setDataset(parsed.dataset);
      setChecks(parsed.checks);
      setParseError(parsed.error);
    });
    return () => {
      live = false;
    };
  }, [opened, checker, contractText, parseContract]);

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
      {dataset !== null && (
        <Text size="xs" c="dimmed" data-testid="dq-rules-dataset">
          {t("dataQualityPanel.dataset", { dataset })}
        </Text>
      )}
      {parseError !== null && (
        <Alert color="red" data-testid="dq-rules-parse-error">
          {parseError}
        </Alert>
      )}
      {checks !== null && checks.length === 0 && parseError === null && (
        <Text size="sm" c="var(--text-muted)">
          {t("dataProductsTab.detail.dqRulesEmpty")}
        </Text>
      )}
      {checks !== null && checks.length > 0 && (
        <Table className="data-table" data-testid="dq-rules-rows">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("dataQualityPanel.columnHeader")}</Table.Th>
              <Table.Th>{t("dataQualityPanel.checkHeader")}</Table.Th>
              <Table.Th>{t("dataQualityPanel.definitionHeader")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {checks.map((c, i) => (
              <Table.Tr key={`${c.columnName}:${c.checkType}:${i}`}>
                <Table.Td>{c.columnName || t("dataQualityPanel.datasetScope")}</Table.Td>
                <Table.Td>{c.checkType}</Table.Td>
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
