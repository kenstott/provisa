// Copyright (c) 2026 Kenneth Stott
// Canary: 6c8e6d5d-73e7-487b-b889-8dc251d0fe8b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { ActionIcon, Badge, Button, Checkbox, Group, Table, Text, TextInput } from "@mantine/core";
import { SlidersHorizontal, Sparkles } from "lucide-react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { fetchFakeCatalog, proposeFakes, type FakeCatalog } from "../../../api/fakes";
import type { RegisteredTable } from "../../../types/admin";
import { ColumnFakeDialog } from "./ColumnFakeDialog";

type Update = (i: number, key: string, value: string | string[] | boolean) => void;

// REQ-1494: per column its fake (blank shows the real value), its synthetic rule, whether the fake
// is stable, and a marker on a pii column with no fake. A simple fake is typed in its cell; the
// column dialog picks kinds and methods by name with their arguments.
export function TestDataColumns({
  table,
  updateEditCol,
}: {
  table: RegisteredTable;
  updateEditCol: Update;
}) {
  const { t } = useTranslation();
  const [catalog, setCatalog] = useState<FakeCatalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState<number | null>(null);
  const [filled, setFilled] = useState<string | null>(null);
  // REQ-1494: Fill from profile proposes for every column that declares neither a fake nor a rule;
  // the proposals fill the form, which saves as any edit.
  const fill = () => {
    setError(null);
    proposeFakes(table.id)
      .then((p) => {
        let n = 0;
        table.columns.forEach((c, i) => {
          const proposal = p.columns[c.columnName];
          if (!proposal || c.fake || c.syntheticRule) return;
          if (proposal.fake) updateEditCol(i, "fake", proposal.fake);
          if (proposal.syntheticRule) updateEditCol(i, "syntheticRule", proposal.syntheticRule);
          n += 1;
        });
        setFilled(
          t("testData.filled", { count: n, run: p.runId }) +
            (p.unmatchedPii.length
              ? " " + t("testData.unmatchedPii", { columns: p.unmatchedPii.join(", ") })
              : ""),
        );
      })
      .catch((e: Error) => setError(e.message));
  };
  useEffect(() => {
    fetchFakeCatalog()
      .then(setCatalog)
      .catch((e: Error) => setError(e.message));
  }, []);
  const dialogButton = (i: number, label: string) => (
    <ActionIcon
      variant="subtle"
      size="sm"
      aria-label={label}
      disabled={catalog === null}
      onClick={() => setOpen(i)}
      data-testid={`testdata-open-${table.columns[i].columnName}`}
    >
      <SlidersHorizontal size={14} />
    </ActionIcon>
  );
  return (
    <>
      <Group justify="space-between">
        <Button
          size="xs"
          variant="light"
          leftSection={<Sparkles size={14} />}
          onClick={fill}
          data-tour="fill-from-profile"
          data-testid="testdata-fill"
        >
          {t("testData.fillFromProfile")}
        </Button>
        {filled && (
          <Text size="sm" c="dimmed" data-testid="testdata-filled">
            {filled}
          </Text>
        )}
      </Group>
      {error && <div className="error">{error}</div>}
      <Table className="data-table" data-testid="testdata-columns">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("tableEditForm.columnHeader")}</Table.Th>
            <Table.Th>{t("testData.fake")}</Table.Th>
            <Table.Th>{t("testData.syntheticRule")}</Table.Th>
            <Table.Th>{t("testData.stable")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {table.columns.map((c, i) => (
            <Table.Tr key={c.id} data-testid={`testdata-row-${c.columnName}`}>
              <Table.Td>
                {c.columnName}
                {c.isPii && !c.fake && (
                  <Badge
                    ml="xs"
                    size="xs"
                    color="red"
                    variant="light"
                    data-testid={`testdata-pii-${c.columnName}`}
                  >
                    {t("testData.piiNoFake")}
                  </Badge>
                )}
              </Table.Td>
              <Table.Td>
                <TextInput
                  size="xs"
                  aria-label={t("testData.fake")}
                  placeholder={t("testData.realValue")}
                  value={c.fake || ""}
                  onChange={(e) => updateEditCol(i, "fake", e.target.value)}
                  rightSection={dialogButton(i, t("testData.openDialog"))}
                  data-testid={`testdata-fake-${c.columnName}`}
                />
              </Table.Td>
              <Table.Td>
                <TextInput
                  size="xs"
                  aria-label={t("testData.syntheticRule")}
                  value={c.syntheticRule || ""}
                  onChange={(e) => updateEditCol(i, "syntheticRule", e.target.value)}
                  rightSection={dialogButton(i, t("testData.openDialog"))}
                  data-testid={`testdata-rule-${c.columnName}`}
                />
              </Table.Td>
              <Table.Td>
                <Checkbox
                  aria-label={t("testData.stable")}
                  checked={!!c.fakeStable}
                  onChange={(e) => updateEditCol(i, "fakeStable", e.currentTarget.checked)}
                  data-testid={`testdata-stable-${c.columnName}`}
                />
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
      {open !== null && catalog && (
        <ColumnFakeDialog
          table={table}
          column={table.columns[open]}
          catalog={catalog}
          onClose={() => setOpen(null)}
          onSave={(fake, rule, stable) => {
            updateEditCol(open, "fake", fake);
            updateEditCol(open, "syntheticRule", rule);
            updateEditCol(open, "fakeStable", stable);
            setOpen(null);
          }}
        />
      )}
    </>
  );
}
