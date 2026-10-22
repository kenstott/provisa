// Copyright (c) 2026 Kenneth Stott
// Canary: 62953d7c-feb2-437b-8814-413ea855439f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1954: a ServiceNow source's own APIs. Every API the instance publishes is listed under its
// namespace; one switched on is offered through the source's companion OpenAPI source
// (`<id>-api`), where its operations are registered as tables and commands like any OpenAPI
// source's. An import set added here is offered as a typed insert, its fields the import set's
// columns.

import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@apollo/client/react";
import { ActionIcon, Alert, Button, Loader, Select, Stack, Text, TextInput } from "@mantine/core";
import { Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  AvailableImportSets,
  ServicenowApis,
  SetServicenowApi,
  SetServicenowImportSet,
} from "../../hooks/admin.graphql";
import type { MutationResult } from "../../types/admin";
import { FilterInput } from "../../components/admin/FilterInput";
import { FieldGroup, FormField, FormLayout } from "../../components/form/FormLayout";
import { serverMessage } from "../../i18n/serverMessage";
import { Tip } from "../../components/Tip";
import { ServiceNowApiList, type InstanceApi } from "./ServiceNowApiList";
import { matchesQuery } from "../../lib/searchItems";

interface CompanionImportSet {
  staging: string;
  table: string;
}

export function ServiceNowApisSection({ sourceId }: { sourceId: string }) {
  const { t } = useTranslation();
  const { data, loading, error, refetch } = useQuery<{
    servicenowApis: InstanceApi[];
    servicenowImportSets: CompanionImportSet[];
  }>(ServicenowApis, { variables: { sourceId }, fetchPolicy: "no-cache" });
  const [setApi] = useMutation<{ setServicenowApi: MutationResult }>(SetServicenowApi);
  const [setImportSet] = useMutation<{ setServicenowImportSet: MutationResult }>(
    SetServicenowImportSet,
  );
  const [filter, setFilter] = useState("");
  const [problem, setProblem] = useState<string | null>(null);
  const [table, setTable] = useState("");
  const [importSet, setImportSetChoice] = useState<string | null>(null);
  const maps = useQuery<{ availableImportSets: { name: string; importSet: string }[] }>(
    AvailableImportSets,
    {
      variables: { sourceId, schemaName: sourceId.replace(/-/g, "_"), tableName: table.trim() },
      skip: !table.trim(),
      fetchPolicy: "no-cache",
    },
  );

  const byNamespace = useMemo(() => {
    const groups = new Map<string, InstanceApi[]>();
    for (const api of data?.servicenowApis ?? []) {
      if (!matchesQuery([api.namespace, api.name, api.route], filter)) continue;
      groups.set(api.namespace, [...(groups.get(api.namespace) ?? []), api]);
    }
    return [...groups];
  }, [data, filter]);

  const done = async (result: MutationResult | undefined) => {
    if (result && !result.success) setProblem(serverMessage(result, result.message));
    else setProblem(null);
    await refetch();
  };

  if (loading) return <Loader size="sm" />;
  if (error) {
    return (
      <Alert color="red" data-testid="servicenow-apis-error">
        {t("serviceNowApis.loadFailed", { error: error.message })}
      </Alert>
    );
  }
  const added = (data?.servicenowApis ?? []).filter((a) => a.added).length;
  return (
    <Stack gap="sm" data-testid="servicenow-apis">
      <Text fw={600} size="sm">
        {t("serviceNowApis.title")}
      </Text>
      <Text size="sm" c="dimmed">
        {t("serviceNowApis.lead", { companion: `${sourceId}-api`, count: added })}
      </Text>
      {problem && (
        <Alert color="red" data-testid="servicenow-apis-problem">
          {problem}
        </Alert>
      )}
      <FilterInput
        value={filter}
        onChange={setFilter}
        placeholder={t("serviceNowApis.filter")}
        testId="servicenow-apis-filter"
      />
      <ServiceNowApiList
        groups={byNamespace}
        onSwitch={async (api, added) => {
          const { data: answered } = await setApi({
            variables: { sourceId, namespace: api.namespace, name: api.name, added },
          });
          await done(answered?.setServicenowApi);
        }}
      />

      <Text fw={600} size="sm" mt="sm">
        {t("serviceNowApis.importSetsTitle")}
      </Text>
      {(data?.servicenowImportSets ?? []).map((s) => (
        <Text key={s.staging} size="sm" data-testid={`servicenow-import-set-${s.staging}`}>
          {t("serviceNowApis.importSetRow", { staging: s.staging, table: s.table })}{" "}
          <Tip text={t("serviceNowApis.removeImportSet")} named>
            <ActionIcon
              size="sm"
              variant="subtle"
              aria-label={t("serviceNowApis.removeImportSet")}
              onClick={async () => {
                const { data: answered } = await setImportSet({
                  variables: { sourceId, tableName: s.table, importSet: s.staging, added: false },
                });
                await done(answered?.setServicenowImportSet);
              }}
            >
              <Trash2 size={12} />
            </ActionIcon>
          </Tip>
        </Text>
      ))}
      <FormLayout>
        <FieldGroup>
          <FormField size="medium">
            <TextInput
              label={t("serviceNowApis.table")}
              value={table}
              onChange={(e) => {
                setTable(e.currentTarget.value);
                setImportSetChoice(null);
              }}
              placeholder="sys_user"
              data-testid="servicenow-import-set-table"
            />
          </FormField>
          <FormField size="medium">
            <Select
              label={t("serviceNowApis.importSet")}
              value={importSet}
              onChange={setImportSetChoice}
              data={(maps.data?.availableImportSets ?? []).map((m) => ({
                value: m.importSet,
                label: t("registerTableForm.importSetOption", {
                  name: m.name,
                  importSet: m.importSet,
                }),
              }))}
              disabled={!table.trim()}
              comboboxProps={{ withinPortal: true }}
              data-testid="servicenow-import-set-choice"
            />
          </FormField>
        </FieldGroup>
        <div>
          <Button
            variant="light"
            disabled={!importSet}
            data-testid="servicenow-import-set-add"
            onClick={async () => {
              if (!importSet) return;
              const { data: answered } = await setImportSet({
                variables: { sourceId, tableName: table.trim(), importSet, added: true },
              });
              setImportSetChoice(null);
              await done(answered?.setServicenowImportSet);
            }}
          >
            {t("serviceNowApis.addImportSet")}
          </Button>
        </div>
      </FormLayout>
    </Stack>
  );
}
