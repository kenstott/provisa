// Copyright (c) 2026 Kenneth Stott
// Canary: 67fbc635-e808-4465-bb25-7ee60c3dbc53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Button, Group, Modal, Stack, Switch, Text } from "@mantine/core";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { checkColumnFake, proposeFakes, type FakeCatalog } from "../../../api/fakes";
import type { RegisteredTable, TableColumn } from "../../../types/admin";
import { FakePicker } from "./FakePicker";

const CHECK_DELAY_MS = 400;

// REQ-1494: one column's test data -- its fake (shown by faked reads), whether it is stable, and
// its synthetic rule (used by synthetic generation only) -- each checked by the server as typed,
// so a declaration the save would refuse is refused here, by name.
export function ColumnFakeDialog({
  table,
  column,
  catalog,
  onClose,
  onSave,
}: {
  table: RegisteredTable;
  column: TableColumn;
  catalog: FakeCatalog;
  onClose: () => void;
  onSave: (fake: string, rule: string, stable: boolean) => void;
}) {
  const { t } = useTranslation();
  const [fake, setFake] = useState(column.fake ?? "");
  const [rule, setRule] = useState(column.syntheticRule ?? "");
  const [stable, setStable] = useState(!!column.fakeStable);
  const [refusal, setRefusal] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);
  // REQ-1494: this column's proposal from the table's latest profile run.
  const fill = () => {
    proposeFakes(table.id)
      .then((p) => {
        const proposal = p.columns[column.columnName];
        if (proposal?.fake) setFake(proposal.fake);
        if (proposal?.syntheticRule) setRule(proposal.syntheticRule);
        setNote(
          proposal
            ? null
            : p.unmatchedPii.includes(column.columnName)
              ? t("testData.noConfidentMatch")
              : t("testData.noProposal"),
        );
      })
      .catch((e: Error) => setNote(e.message));
  };
  useEffect(() => {
    const timer = setTimeout(() => {
      checkColumnFake({
        tableId: table.id,
        tableName: table.tableName,
        column: column.columnName,
        dataType: column.dataType ?? "",
        fake: fake.trim() || null,
        syntheticRule: rule.trim() || null,
        stable,
      })
        .then(() => setRefusal(null))
        .catch((e: Error) => setRefusal(e.message));
    }, CHECK_DELAY_MS);
    return () => clearTimeout(timer);
  }, [fake, rule, stable, table.id, table.tableName, column.columnName, column.dataType]);
  return (
    <Modal
      opened
      onClose={onClose}
      size="lg"
      title={t("testData.dialogTitle", { column: column.columnName })}
    >
      <Stack data-testid="testdata-dialog">
        <FakePicker
          catalog={catalog}
          label={t("testData.fake")}
          value={fake}
          onChange={setFake}
          rule={false}
          testId="testdata-dialog-fake"
        />
        <Switch
          label={t("testData.stable")}
          description={t("testData.stableHint")}
          checked={stable}
          onChange={(e) => setStable(e.currentTarget.checked)}
          data-testid="testdata-dialog-stable"
        />
        <FakePicker
          catalog={catalog}
          label={t("testData.syntheticRule")}
          value={rule}
          onChange={setRule}
          rule
          testId="testdata-dialog-rule"
        />
        {note && (
          <Text size="sm" c="dimmed" data-testid="testdata-dialog-note">
            {note}
          </Text>
        )}
        {refusal && (
          <Text c="red" size="sm" data-testid="testdata-dialog-refusal">
            {refusal}
          </Text>
        )}
        <Group justify="flex-end">
          <Button variant="light" mr="auto" onClick={fill} data-testid="testdata-dialog-fill">
            {t("testData.fillFromProfile")}
          </Button>
          <Button variant="default" onClick={onClose}>
            {t("testData.cancel")}
          </Button>
          <Button
            disabled={refusal !== null}
            onClick={() => onSave(fake.trim(), rule.trim(), stable)}
            data-testid="testdata-dialog-save"
          >
            {t("testData.apply")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
