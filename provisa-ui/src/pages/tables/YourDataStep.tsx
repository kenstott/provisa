// Copyright (c) 2026 Kenneth Stott
// Canary: 1b6d9e24-5f73-4a08-b3c1-8e2a7d4f6c95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 (SEE THE DATA RIGHT AFTER REGISTERING): the step a person lands on once tables are
// registered, from connecting to querying. It shows the first table, the others a click away: its
// first rows, read through the governed query as the person's own role; a button that opens the
// query explorer on a query ready to run; and the same table through the other access methods,
// each ready to copy. A table its registration refused is named, with why.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Alert, Badge, Button, Code, Group, Stack, Tabs, Text } from "@mantine/core";
import { GovernedTableViewer } from "../../components/GovernedTableViewer";
import { CopyButton } from "../../components/CopyButton";
import { useTableAccess, useTables } from "../../hooks/useAdminQueries";
import type { Registered } from "./useManyTables";
import type { ListedTable } from "../../types/admin";

function AccessMethod({ label, text, testId }: { label: string; text: string; testId: string }) {
  const { t } = useTranslation();
  return (
    <Stack gap={4} data-testid={testId}>
      <Group gap="xs">
        <Text size="sm" fw={600}>
          {label}
        </Text>
        <CopyButton text={text} hint={t("yourData.copy", { what: label })} />
      </Group>
      <Code block>{text}</Code>
    </Stack>
  );
}

function TableData({ table, role }: { table: ListedTable; role: string | null }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { access, error } = useTableAccess(table.id, role);
  return (
    <Stack gap="md">
      <GovernedTableViewer key={table.id} table={table} role={role} />
      <Group gap="xs">
        <Button
          disabled={access === null}
          onClick={() => access && navigate("/query", { state: { query: access.graphql } })}
          data-testid="your-data-open-query"
        >
          {t("yourData.openQuery")}
        </Button>
      </Group>
      {error && (
        <Alert color="red" variant="light" data-testid="your-data-access-error">
          {error}
        </Alert>
      )}
      {access && (
        <Stack gap="sm">
          <Group gap="xs">
            <Text size="sm" fw={600}>
              {t("yourData.otherWays")}
            </Text>
            {/* REQ-1921: a draft is reached these ways by a role holding org_admin alone. */}
            {access.draft && (
              <Badge size="xs" variant="light" color="gray" data-testid="your-data-draft">
                {t("yourData.draft")}
              </Badge>
            )}
          </Group>
          {access.draft && (
            <Text size="sm" c="dimmed" data-testid="your-data-draft-note">
              {t("yourData.draftNote")}
            </Text>
          )}
          <AccessMethod
            label={t("yourData.graphql")}
            text={access.graphql}
            testId="your-data-graphql"
          />
          {access.pgwire && (
            <AccessMethod
              label={t("yourData.pgwire")}
              text={access.pgwire}
              testId="your-data-pgwire"
            />
          )}
          <AccessMethod label={t("yourData.sql")} text={access.sql} testId="your-data-sql" />
          {access.rest && (
            <AccessMethod label={t("yourData.rest")} text={access.rest} testId="your-data-rest" />
          )}
          <AccessMethod label={t("yourData.mcp")} text={access.mcp} testId="your-data-mcp" />
        </Stack>
      )}
    </Stack>
  );
}

export function YourDataStep({
  registered,
  role,
}: {
  registered: Registered;
  role: string | null;
}) {
  const { t } = useTranslation();
  const { tables } = useTables();
  const shown = registered.tableIds
    .map((id) => tables.find((tbl) => tbl.id === id))
    .filter((tbl): tbl is ListedTable => tbl !== undefined);
  const [chosen, setChosen] = useState<string | null>(null);
  const current = shown.find((tbl) => String(tbl.id) === chosen) ?? shown[0];
  return (
    <Stack gap="md" data-testid="your-data-step">
      <Text fw={600}>{t("yourData.title")}</Text>
      {registered.refused.length > 0 && (
        <Alert color="orange" variant="light" data-testid="your-data-refused">
          {registered.refused.map((o) => (
            <Text key={o.tableName} size="sm">
              {t("registerManyTables.refused", { table: o.tableName, message: o.result.message })}
            </Text>
          ))}
        </Alert>
      )}
      {current === undefined ? (
        <Text size="sm" c="dimmed">
          {t("yourData.loading")}
        </Text>
      ) : (
        <Tabs value={String(current.id)} onChange={setChosen} keepMounted={false}>
          <Tabs.List>
            {shown.map((tbl) => (
              <Tabs.Tab
                key={tbl.id}
                value={String(tbl.id)}
                data-testid={`your-data-tab-${tbl.tableName}`}
              >
                {tbl.alias || tbl.tableName}
              </Tabs.Tab>
            ))}
          </Tabs.List>
          {shown.map((tbl) => (
            <Tabs.Panel key={tbl.id} value={String(tbl.id)} pt="sm">
              <TableData table={tbl} role={role} />
            </Tabs.Panel>
          ))}
        </Tabs>
      )}
    </Stack>
  );
}
