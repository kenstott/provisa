// Copyright (c) 2026 Kenneth Stott
// Canary: 348dc6a5-1e5d-4c0e-aec3-eb8dc48d2e18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The register step's schema and table pickers: the source's schemas, then the tables of the
// chosen one, each marked when it is registered already.

import { Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { TableOptions } from "./TableOptions";

type OfferedTables = Parameters<typeof TableOptions>[0]["tables"];

export function SchemaTableSelects({
  sourceId,
  schemaName,
  setSchemaName,
  tableName,
  setTableName,
  isRegistered,
  schemas,
  loadingSchemas,
  fixedSchema,
  tables,
  loadingTables,
  allRegistered,
  starting,
  startingTimedOut,
}: {
  sourceId: string;
  schemaName: string;
  setSchemaName: (schema: string) => void;
  tableName: string;
  setTableName: (table: string) => void;
  isRegistered: (table: { name: string }) => boolean;
  schemas: string[];
  loadingSchemas: boolean;
  fixedSchema: boolean;
  tables: OfferedTables;
  loadingTables: boolean;
  allRegistered: boolean;
  starting: boolean;
  startingTimedOut: boolean;
}) {
  const { t } = useTranslation();
  return (
    <>
      <FormField size="medium">
        <label>
          {t("registerTableForm.schemaLabel")}
          <select
            className="form-select"
            value={schemaName}
            onChange={(e) => setSchemaName(e.target.value)}
            disabled={!sourceId || loadingSchemas || fixedSchema}
            data-testid="register-table-schema-select"
          >
            <option value="">
              {loadingSchemas
                ? t("registerTableForm.schemaLoading")
                : t("registerTableForm.schemaPlaceholder")}
            </option>
            {schemas.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
      </FormField>
      <FormField size="medium">
        <label>
          {t("registerTableForm.tableLabel")}
          <select
            className="form-select"
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            disabled={!schemaName || loadingTables || allRegistered}
            data-testid="register-table-table-select"
          >
            <option value="">
              {starting
                ? t("registerTableForm.tableStarting")
                : loadingTables
                  ? t("registerTableForm.tableLoading")
                  : allRegistered
                    ? t("registerTableForm.tableAllRegistered")
                    : t("registerTableForm.tablePlaceholder")}
            </option>
            <TableOptions tables={tables} isRegistered={isRegistered} />
          </select>
          {startingTimedOut && (
            <Text size="xs" c="red" mt={4} data-testid="register-table-starting-timeout">
              {t("registerTableForm.tableStartingTimedOut")}
            </Text>
          )}
        </label>
      </FormField>
    </>
  );
}
