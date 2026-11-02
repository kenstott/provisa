// Copyright (c) 2026 Kenneth Stott
// Canary: 8b7bec13-df2c-44bd-93ed-cc23639c19e2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-1949, REQ-2193

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Code, Group, Stack, Table, Text } from "@mantine/core";
import { Sheet } from "lucide-react";
import { CopyButton } from "../components/CopyButton";
import { PageTitle } from "../components/PageTitle";
import { PageLoading } from "../components/PageLoading";
import { SplitPane } from "../components/list/SplitPane";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";
import { useAuth } from "../context/AuthContext";
import { fetchODataMetadata, SERVICE_ROOT } from "../api/odata";
import { parseMetadata, type ODataEntitySet } from "./odata/metadata";
import { EntitySetPanel } from "./odata/EntitySetPanel";
import "./odata/ODataPage.css";

interface AddressProps {
  testId: string;
  label: string;
  help: string;
  url: string;
}

function Address({ testId, label, help, url }: AddressProps) {
  const { t } = useTranslation();
  return (
    <Stack gap={4}>
      <Text size="sm" fw={600}>
        {label}
      </Text>
      <Group gap="xs" wrap="nowrap">
        <Code data-testid={testId}>{url}</Code>
        <CopyButton text={url} hint={t("odataPage.copyUrl")} />
      </Group>
      <Text size="sm" c="dimmed">
        {help}
      </Text>
    </Stack>
  );
}

/** The role's model as $metadata gives it: loading, refused, or its entity sets. */
type Model =
  | { state: "loading" }
  | { state: "failed"; error: string }
  | { state: "read"; sets: ODataEntitySet[] };

function useODataModel(role: string): Model {
  const [model, setModel] = useState<{ role: string; model: Model } | null>(null);
  useEffect(() => {
    let current = true;
    fetchODataMetadata(role).then(
      (xml) => {
        if (!current) return;
        try {
          setModel({ role, model: { state: "read", sets: parseMetadata(xml) } });
        } catch (e) {
          setModel({ role, model: { state: "failed", error: (e as Error).message } });
        }
      },
      (e: unknown) => {
        if (current) {
          setModel({
            role,
            model: { state: "failed", error: e instanceof Error ? e.message : String(e) },
          });
        }
      },
    );
    return () => {
      current = false;
    };
  }, [role]);
  // The model read for another role is not this one's: it is read again.
  return model?.role === role ? model.model : { state: "loading" };
}

/** Where an OData client reaches this deployment, and a viewer of what it serves the acting role:
 * its entity sets read from $metadata, and a read of each built, shown as its URL, and run. */
export function ODataPage() {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const role = actingRoles ?? "";
  const root = `${window.location.origin}${SERVICE_ROOT}`;
  const model = useODataModel(role);
  const sets = model.state === "read" ? model.sets : [];
  const [selected, setSelectedSet] = useState<string | null>(null);
  // A query is being built in the panel: Escape (a picker's, as often as the reader's) does not
  // close it and drop the query. Its close button and another row still do.
  const [edited, setEdited] = useState(false);
  const setSelected = (name: string | null) => {
    setSelectedSet(name);
    setEdited(false);
  };
  const entity = sets.find((s) => s.name === selected) ?? null;

  const columns: ListColumn<ODataEntitySet>[] = [
    { key: "name", label: t("odataPage.entitySet"), sortValue: (s) => s.name },
    { key: "key", label: t("odataPage.key"), sortValue: (s) => s.key.join(",") },
    { key: "properties", label: t("odataPage.properties"), sortValue: (s) => s.properties.length },
    {
      key: "navigations",
      label: t("odataPage.navigations"),
      sortValue: (s) => s.navigations.length,
    },
  ];
  const sortGroup = useListSortGroup(sets, columns, "odata-sets", "name");

  return (
    <div className="page-sticky-head odata-page">
      <Group justify="space-between" mb="sm">
        <PageTitle icon={Sheet} order={3}>
          {t("odataPage.title")}
        </PageTitle>
      </Group>
      <Text size="sm" mb="sm">
        {t("odataPage.description")}
      </Text>
      <Group gap="xl" align="flex-start" mb="md" className="odata-addresses">
        <Address
          testId="odata-service-root"
          label={t("odataPage.serviceRoot")}
          help={t("odataPage.serviceRootHelp")}
          url={root}
        />
        <Address
          testId="odata-metadata"
          label={t("odataPage.metadata")}
          help={t("odataPage.metadataHelp")}
          url={`${root}/$metadata`}
        />
      </Group>

      {model.state === "failed" && (
        <Alert color="red" mb="sm" data-testid="odata-metadata-error">
          {model.error}
        </Alert>
      )}
      {model.state === "loading" ? (
        <PageLoading message={t("odataPage.loadingMetadata")} />
      ) : (
        <SplitPane
          testId="odata-split"
          storageKey="provisa.odata.detailWidth"
          title={entity?.name ?? ""}
          onClose={() => setSelected(null)}
          closeOnEscape={!edited}
          labels={{
            panel: t("odataPage.detailPanel"),
            resize: t("tablesPage.resizeDetail"),
            expand: t("tablesPage.expandDetail"),
            restore: t("tablesPage.restoreDetail"),
            close: t("tablesPage.closeDetail"),
            backToList: t("tablesPage.backToList"),
          }}
          list={
            <ListTable testId="odata-sets">
              <ListHead
                sortGroup={sortGroup}
                columns={[
                  { col: "name" },
                  { col: "key" },
                  { col: "properties" },
                  { col: "navigations" },
                ]}
              />
              <Table.Tbody>
                {sets.length === 0 && model.state === "read" && (
                  <ListEmpty colSpan={4} testId="odata-sets-empty">
                    {t("odataPage.noEntitySets")}
                  </ListEmpty>
                )}
                <ListItems
                  state={sortGroup}
                  colSpan={4}
                  rowKey={(s) => s.name}
                  render={(s) => (
                    <ListRow
                      key={s.name}
                      testId={`odata-set-row-${s.name}`}
                      selected={selected === s.name}
                      onClick={() => setSelected(selected === s.name ? null : s.name)}
                    >
                      <Table.Td>
                        <Text size="sm" fw={600} ff="monospace">
                          {s.name}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs" ff="monospace">
                          {s.key.join(", ")}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{s.properties.length}</Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{s.navigations.length}</Text>
                      </Table.Td>
                    </ListRow>
                  )}
                />
              </Table.Tbody>
            </ListTable>
          }
        >
          {entity && (
            // A set's own builder and rows: another set starts from an empty query.
            <EntitySetPanel
              key={`${role}/${entity.name}`}
              entity={entity}
              role={role}
              root={root}
              onEdited={() => setEdited(true)}
            />
          )}
        </SplitPane>
      )}
    </div>
  );
}
