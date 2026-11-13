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

import { Button, Checkbox, SegmentedControl, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import type { TableMetadata } from "../../api/admin";
import { FormField } from "../../components/form/FormLayout";
import { blockedBy, type Reading } from "./useManyTables";

function whyNot(t: TFunction, reading: Reading | undefined, needsCacheTtl: boolean): string | null {
  if (!reading || reading.state === "reading") return t("registerManyTables.reading");
  if (reading.state === "failed")
    return t("registerManyTables.readFailed", { error: reading.error });
  const needs = blockedBy(reading, needsCacheTtl);
  if (needs === null) return null;
  if (needs.kind === "declared") return t("registerManyTables.needsDeclared");
  if (needs.kind === "noColumns") return t("registerManyTables.needsNoColumns");
  if (needs.kind === "cacheTtl") return t("registerManyTables.needsCacheTtl");
  return t("registerManyTables.needsUntyped", { columns: needs.columns.join(", ") });
}

/** The schema's tables, each with a tick, or why it cannot be ticked. */
export function ManyTablesList({
  offered,
  isRegistered,
  readings,
  ticked,
  setTicked,
  loading,
  needsCacheTtl = false,
  onRegisterOne,
  opening = null,
}: {
  offered: TableMetadata[];
  isRegistered: (table: { name: string }) => boolean;
  readings: Record<string, Reading>;
  ticked: string[];
  setTicked: (names: string[]) => void;
  loading: boolean;
  /** Each table of the source needs a Cache TTL of its own: none is ticked with the others. */
  needsCacheTtl?: boolean;
  /** Registers on its own a table that needs input, opening it in edit (registerAsDraft). */
  onRegisterOne?: (tableName: string) => void;
  /** The table being opened to be registered on its own. */
  opening?: string | null;
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
            : whyNot(t, readings[tbl.name], needsCacheTtl);
          // A table read whole that needs input is registered on its own, from its own row.
          const onItsOwn =
            !registered &&
            onRegisterOne !== undefined &&
            blockedBy(readings[tbl.name], needsCacheTtl) !== null;
          const tick = (
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
          return onItsOwn ? (
            // The action under the row's reason, where its label's text starts (the tick's width,
            // Mantine's xl), so every row's action is in one column.
            <div key={tbl.name}>
              {tick}
              <Button
                variant="light"
                size="compact-xs"
                mt={4}
                ms="xl"
                loading={opening === tbl.name}
                disabled={opening !== null && opening !== tbl.name}
                onClick={() => onRegisterOne(tbl.name)}
                aria-label={t("registerManyTables.registerOneLabel", { table: tbl.name })}
                data-testid={`register-many-table-register-${tbl.name}`}
              >
                {t("registerManyTables.registerOne")}
              </Button>
            </div>
          ) : (
            tick
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
