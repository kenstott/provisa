// Copyright (c) 2026 Kenneth Stott
// Canary: 7f4a2c96-3e18-4d5b-a2c7-9b6e0d8f1a53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: several tables of a schema picked at once and registered in one action, each with the
// defaults its registration alone would take. Discovery decides, table by table, whether a table
// needs input from the person: one that does is listed, unticked, saying why, and is registered
// on its own. A table registered already is listed, and cannot be picked again.

import { Checkbox, SegmentedControl, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import type { TableMetadata } from "../../api/admin";
import { FormField } from "../../components/form/FormLayout";
import type { Reading } from "./useManyTables";

function whyNot(t: TFunction, reading: Reading | undefined): string | null {
  if (!reading || reading.state === "reading") return t("registerManyTables.reading");
  if (reading.state === "failed")
    return t("registerManyTables.readFailed", { error: reading.error });
  if (reading.needs === null) return null;
  if (reading.needs.kind === "declared") return t("registerManyTables.needsDeclared");
  if (reading.needs.kind === "noColumns") return t("registerManyTables.needsNoColumns");
  return t("registerManyTables.needsUntyped", { columns: reading.needs.columns.join(", ") });
}

/** The schema's tables, each with a tick, or why it cannot be ticked. */
export function ManyTablesList({
  offered,
  isRegistered,
  readings,
  ticked,
  setTicked,
  loading,
}: {
  offered: TableMetadata[];
  isRegistered: (table: { name: string }) => boolean;
  readings: Record<string, Reading>;
  ticked: string[];
  setTicked: (names: string[]) => void;
  loading: boolean;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="full">
      <Checkbox.Group
        label={t("registerManyTables.tablesLabel")}
        description={
          loading
            ? t("registerManyTables.listing")
            : offered.length === 0
              ? t("registerManyTables.noTables")
              : t("registerManyTables.tickedCount", { count: ticked.length })
        }
        // What is ticked is said above the list, not under it.
        inputWrapperOrder={["label", "description", "input", "error"]}
        value={ticked}
        onChange={setTicked}
        data-testid="register-many-tables"
      >
        {offered.map((tbl) => {
          const registered = isRegistered(tbl);
          const why = registered
            ? t("registerManyTables.registered")
            : whyNot(t, readings[tbl.name]);
          return (
            <Checkbox
              key={tbl.name}
              value={tbl.name}
              mt={6}
              disabled={why !== null}
              label={
                <>
                  {tbl.name}
                  {tbl.comment && (
                    <Text span c="dimmed" fz="xs" ms={6}>
                      {tbl.comment}
                    </Text>
                  )}
                  {/* Why it cannot be ticked, on the line under its name. */}
                  {why && (
                    <Text c="dimmed" fz="xs">
                      {why}
                    </Text>
                  )}
                </>
              }
              data-testid={`register-many-table-${tbl.name}`}
            />
          );
        })}
      </Checkbox.Group>
    </FormField>
  );
}

/** One table at a time, or several picked at once. */
export function ManyTablesMode({
  several,
  onChange,
}: {
  several: boolean;
  onChange: (several: boolean) => void;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="medium">
      <SegmentedControl
        aria-label={t("registerManyTables.modeLabel")}
        value={several ? "several" : "one"}
        onChange={(v) => onChange(v === "several")}
        data={[
          { value: "one", label: t("registerManyTables.modeOne") },
          { value: "several", label: t("registerManyTables.modeSeveral") },
        ]}
        data-testid="register-table-many-mode"
      />
    </FormField>
  );
}
