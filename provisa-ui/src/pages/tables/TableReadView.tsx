// Copyright (c) 2026 Kenneth Stott
// Canary: 2b6d9e47-3f1a-4c8b-a5e2-7d0f4c9b2e65
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { maskSpecLabel, maskToLabel } from "../../lib/masks";
import { grantLabel } from "../../lib/grants";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";
import { ChartColumn, DatabaseZap, Eye, Pencil, ShieldCheck, Trash2 } from "lucide-react";
import { Tip } from "../../components/Tip";
import { ActionIcon, Badge, Box, Button, Checkbox, Group, Table, Text } from "@mantine/core";
import type { NavigateFunction } from "react-router-dom";
import type { RegisteredTable, DataProduct, Relationship, Source } from "../../types/admin";
import type { ReplicaBuild } from "../../api/admin";
import { ReplicatingMark } from "./ReplicatingMark";
import { computeProfile, masksSummary } from "./helpers";
import { TagControl } from "../../components/TagControl";
import { ColumnGlossaryHover } from "./ColumnGlossaryHover";
import { OwnerResolutionIcon } from "../../components/OwnerResolution";
import { LatestProfilePanel } from "./LatestProfilePanel";
import { PanelActions } from "../../components/list/SplitPane";
import { ColumnTabs } from "./ColumnTabs";
import { COLUMN_TABS, useColumnTab, type ColumnTab } from "./columnTopics";
import { toIrType } from "../../irTypes";
import { TableSectionTabs } from "./TableSectionTabs";
import { WriteGrantsPanel } from "./WriteGrantsPanel";
import { useTableSection, type TableSection } from "./tableSections";
import { TableAssociations } from "./TableAssociations";
import { ParametersPanel } from "./ParametersPanel";
import { IdentityRead, LoadRead, QualityRead, QueryRead } from "./TableSectionsRead";
import { dataProductField } from "./tableSectionFields";

interface TableProfileResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
}

interface TableReadViewProps {
  t: RegisteredTable;
  /** The table's replica build as last reported; undefined when it has none. */
  build?: ReplicaBuild;
  dataProducts: DataProduct[]; // REQ-1634
  navigate: NavigateFunction;
  viewsOnly: boolean;
  deploying: Record<number, boolean>;
  setDeploying: React.Dispatch<React.SetStateAction<Record<number, boolean>>>;
  deployMsg: Record<number, { success: boolean; message: string }>;
  setDeployMsg: React.Dispatch<
    React.SetStateAction<Record<number, { success: boolean; message: string }>>
  >;
  tableProfiles: Record<number, TableProfileResult | "loading" | string>;
  deployViewToDb: (id: number) => Promise<{ success: boolean; message: string }>;
  reload: () => void;
  startEditing: (t: RegisteredTable) => void;
  handleDelete: (id: number) => void;
  handleProfile: (id: number) => void;
  /** REQ-1944: a governance-only viewer: no registration, deploy, profiling or delete. */
  hidingOnly?: boolean;
  // REQ-1921: put the table out of service (draft) or release it. The view shows the stored
  // state: the box follows `table.draft` once the change is saved and the table reloaded.
  onSetDraft?: (table: RegisteredTable, draft: boolean) => void;
  // REQ-1386: opens the governed SELECT * preview modal.
  onPreview: (t: RegisteredTable) => void;
  // REQ-1318: opens the Views-page definition-mode editor (SQL | Metrics toggle).
  onEditDefinition?: (t: RegisteredTable) => void;
  // REQ-1934: the table's registered profile columns table, when one is registered.
  profileColumnsTable?: RegisteredTable | null;
  /** Render the action row alone: the edit form shows it, disabled, in the panel's head. */
  actionsOnly?: boolean;
  /** The table's source: what Identity, Load and timeliness and Quality say depends on it. */
  source?: Source;
  /** The relationships the table takes part in are listed on Associations. */
  relationships: Relationship[];
  /**
   * Drops the table's cached results, for a table whose source caches them; absent for any
   * other table, which then shows no such action.
   */
  onInvalidateCache?: (id: number) => void;
  /** Whether the table's cache is being dropped now. */
  invalidating?: boolean;
  /** The section in view, kept by the page across the read view and the edit form. */
  section?: TableSection;
  onSectionChange?: (section: TableSection) => void;
  /** The column topic the Columns section opens on (a link's ``tab``); else its first. */
  columnTab?: ColumnTab;
}

export function TableReadView({
  t: table,
  build,
  dataProducts,
  navigate,
  viewsOnly,
  deploying,
  setDeploying,
  deployMsg,
  setDeployMsg,
  tableProfiles,
  deployViewToDb,
  reload,
  startEditing,
  handleDelete,
  handleProfile,
  hidingOnly = false,
  onSetDraft,
  onPreview,
  onEditDefinition,
  profileColumnsTable,
  actionsOnly = false,
  source,
  relationships,
  section: sectionIn,
  onSectionChange,
  columnTab,
  onInvalidateCache,
  invalidating = false,
}: TableReadViewProps) {
  const { t } = useTranslation();
  const [section, setSection] = useTableSection(sectionIn, onSectionChange);
  // The same topics as the column editor. A caller who may change only how columns are hidden
  // is not offered Testing, here as in the editor.
  const shownTabs = COLUMN_TABS.filter((topic) => !(topic === "testing" && hidingOnly));
  const [tab, setTab] = useColumnTab(shownTabs, columnTab);

  // The table's actions. In a detail panel they sit in its pinned head (PanelActions); under a
  // list row they stay in place. While the table is being edited they are not there: the form's
  // Save and Cancel have the head (FormFooter `panel`).
  const actions = (
    <PanelActions>
      <fieldset className="panel-action-set">
        <Group justify="flex-start" wrap="wrap">
          <ReplicatingMark build={build} size="detail" />
          {table.viewSql && (
            <Tip text={t("tableReadView.editSqlTitle")}>
              <Button
                data-testid="table-read-view-edit-sql"
                onClick={(e) => {
                  e.stopPropagation();
                  navigate("/sql", { state: { sql: table.viewSql, viewTable: table } });
                }}
              >
                {viewsOnly
                  ? t("tableReadView.editSqlButton")
                  : t("tableReadView.openInExplorerButton")}
              </Button>
            </Tip>
          )}
          {table.viewSql && onEditDefinition && !hidingOnly && (
            <Tip text={t("tableReadView.editDefinitionTitle")}>
              <Button
                data-testid="table-read-view-edit-definition"
                onClick={(e) => {
                  e.stopPropagation();
                  onEditDefinition(table);
                }}
              >
                {t("tableReadView.editDefinitionButton")}
              </Button>
            </Tip>
          )}
          {table.canDeployToDb && !hidingOnly && (
            <Tip text={t("tableReadView.deployToDbTitle")}>
              <Button
                data-testid="table-read-view-deploy"
                onClick={async (e) => {
                  e.stopPropagation();
                  setDeploying((prev) => ({ ...prev, [table.id]: true }));
                  setDeployMsg((prev) => {
                    const next = { ...prev };
                    delete next[table.id];
                    return next;
                  });
                  const result = await deployViewToDb(table.id);
                  setDeploying((prev) => ({ ...prev, [table.id]: false }));
                  setDeployMsg((prev) => ({ ...prev, [table.id]: result }));
                  if (result.success) reload();
                }}
                disabled={deploying[table.id]}
              >
                {deploying[table.id]
                  ? t("tableReadView.deployingButton")
                  : t("tableReadView.deployToDbButton")}
              </Button>
            </Tip>
          )}
          {/* A table that needs a value for a required parameter is asked for it first: Preview
              in its own viewer, Profile in the dialog its handler opens. */}
          {/* Preview, Profile and Policies are icon actions, named by their word, with the
              word's longer telling in the tip. */}
          <Tip text={t("tableReadView.previewTitle")}>
            <ActionIcon
              aria-label={t("tableReadView.previewButton")}
              data-testid="table-read-view-preview"
              onClick={(e) => {
                e.stopPropagation();
                onPreview(table);
              }}
            >
              <Eye size={14} />
            </ActionIcon>
          </Tip>
          {!hidingOnly && (
            <Tip text={t("tableReadView.profileTitle")}>
              <ActionIcon
                aria-label={
                  tableProfiles[table.id] === "loading"
                    ? t("tableReadView.profilingButton")
                    : t("tableReadView.profileButton")
                }
                data-testid="table-read-view-profile"
                onClick={(e) => {
                  e.stopPropagation();
                  handleProfile(table.id);
                  // The profile is read on Quality, where it lands.
                  setSection("quality");
                }}
                loading={tableProfiles[table.id] === "loading"}
                disabled={tableProfiles[table.id] === "loading"}
              >
                <ChartColumn size={14} />
              </ActionIcon>
            </Tip>
          )}
          <Tip text={t("tableReadView.policiesTitle")}>
            <ActionIcon
              aria-label={t("tableReadView.policiesButton")}
              data-testid="table-read-view-policies"
              onClick={(e) => {
                e.stopPropagation();
                navigate("/security/rls", {
                  state: { tableFilter: table.tableName },
                });
              }}
            >
              <ShieldCheck size={14} />
            </ActionIcon>
          </Tip>
          {onInvalidateCache && (
            <Tip text={t("tablesPage.invalidateCache")} named>
              <ActionIcon
                aria-label={t("tablesPage.invalidateCache")}
                data-testid="table-read-view-invalidate-cache"
                loading={invalidating}
                disabled={invalidating}
                onClick={(e) => {
                  e.stopPropagation();
                  onInvalidateCache(table.id);
                }}
              >
                <DatabaseZap size={14} />
              </ActionIcon>
            </Tip>
          )}
          {/* REQ-1958: the editor opens for a caller who is not shown the grant lists and masks
              too; it shows none of them and its save names none, so the store keeps them. */}
          <ActionIcon
            aria-label={t("tableReadView.editButtonLabel", { name: table.tableName })}
            data-testid="table-read-view-edit"
            onClick={(e) => {
              e.stopPropagation();
              startEditing(table);
            }}
          >
            <Pencil size={14} />
          </ActionIcon>
          {!hidingOnly && (
            <ActionIcon
              color="red"
              aria-label={t("tableReadView.deleteButtonLabel", { name: table.tableName })}
              data-testid="table-read-view-delete"
              onClick={(e) => {
                e.stopPropagation();
                handleDelete(table.id);
              }}
            >
              <Trash2 size={14} />
            </ActionIcon>
          )}
          {/* REQ-1921: draft is set and cleared here, beside the table's other actions. It needs
              the right to register tables in the table's domain; without it the box only shows
              the state. */}
          <Tip
            text={
              hidingOnly || !onSetDraft
                ? t("tableReadView.draftNoRightTooltip")
                : t("tableReadView.draftTooltip")
            }
          >
            <Checkbox
              size="xs"
              label={t("tableReadView.draftLabel")}
              checked={table.draft}
              disabled={hidingOnly || !onSetDraft}
              data-testid="table-read-view-draft"
              onClick={(e) => e.stopPropagation()}
              onChange={(e) => onSetDraft?.(table, e.currentTarget.checked)}
            />
          </Tip>
        </Group>
      </fieldset>
    </PanelActions>
  );
  if (actionsOnly) return actions;

  return (
    <>
      {/* The same seven sections as the edit form, each read-only. */}
      <TableSectionTabs value={section} onChange={setSection} />
      {/* The section in view, as a demo tour step anchors on it (`open: <section>`); Columns
          carries its own. */}
      <div data-testid={section === "columns" ? undefined : `table-section-${section}-panel`}>
        {section === "identity" && <IdentityRead table={table} source={source} />}
        {section === "associations" && (
          <TableAssociations
            table={table}
            relationships={relationships}
            leading={[dataProductField(t, table, dataProducts)]}
          />
        )}
        {section === "load" && <LoadRead table={table} source={source} />}
        {section === "query" && (
          <>
            <QueryRead table={table} />
            {/* REQ-1986: the table's parameters and which are required. */}
            <ParametersPanel columns={table.columns} />
          </>
        )}
        {section === "quality" && (
          <>
            <QualityRead table={table} source={source} />
            {profileColumnsTable && <LatestProfilePanel profileTable={profileColumnsTable} />}
            {(() => {
              const p = tableProfiles[table.id];
              if (!p || p === "loading") return null;
              if (typeof p === "string")
                return (
                  <Box px="0.75rem" py="0.5rem" c="var(--destructive)" fz="0.8rem">
                    {p}
                  </Box>
                );
              const prof = computeProfile(p.columns, p.rows);
              return (
                <Box style={{ borderTop: "1px solid var(--border)" }} px="0.75rem" py="0.5rem">
                  <Box fz="0.75rem" c="dimmed" mb="0.4rem">
                    {t("tableReadView.profileHeading", { count: p.rowCount })}
                  </Box>
                  <Table.ScrollContainer minWidth={640}>
                    <Table className="data-table" style={{ fontSize: "0.72rem" }}>
                      <Table.Thead>
                        <Table.Tr>
                          <Table.Th>{t("tableReadView.colColumn")}</Table.Th>
                          <Table.Th title={t("tableReadView.colNullsTitle")}>
                            {t("tableReadView.colNulls")}
                          </Table.Th>
                          <Table.Th title={t("tableReadView.colBlanksTitle")}>
                            {t("tableReadView.colBlanks")}
                          </Table.Th>
                          <Table.Th title={t("tableReadView.colDistinctTitle")}>
                            {t("tableReadView.colDistinct")}
                          </Table.Th>
                          <Table.Th>{t("tableReadView.colMin")}</Table.Th>
                          <Table.Th>{t("tableReadView.colMax")}</Table.Th>
                          <Table.Th>{t("tableReadView.colMean")}</Table.Th>
                          <Table.Th>{t("tableReadView.colTopValues")}</Table.Th>
                        </Table.Tr>
                      </Table.Thead>
                      <Table.Tbody>
                        {prof.map((c) => {
                          const nullPct =
                            p.rowCount > 0 ? Math.round((c.nullCount / p.rowCount) * 100) : 0;
                          const isHighNull = nullPct >= 50;
                          return (
                            <Table.Tr key={c.col}>
                              <Table.Td style={{ fontFamily: "monospace", fontWeight: 600 }}>
                                {c.col}
                              </Table.Td>
                              <Table.Td>
                                <Group gap="0.4rem" wrap="nowrap">
                                  <Box
                                    style={{
                                      width: 52,
                                      height: 5,
                                      borderRadius: 3,
                                      background: "var(--border)",
                                      position: "relative",
                                      flexShrink: 0,
                                    }}
                                  >
                                    {c.nullCount > 0 && (
                                      <Box
                                        style={{
                                          position: "absolute",
                                          left: 0,
                                          top: 0,
                                          bottom: 0,
                                          width: `${nullPct}%`,
                                          borderRadius: 3,
                                          background: isHighNull
                                            ? "var(--destructive)"
                                            : "var(--text-muted)",
                                        }}
                                      />
                                    )}
                                  </Box>
                                  <Text
                                    span
                                    fz="0.7rem"
                                    c={
                                      isHighNull
                                        ? "var(--destructive)"
                                        : c.nullCount > 0
                                          ? undefined
                                          : "dimmed"
                                    }
                                  >
                                    {c.nullCount > 0 ? `${nullPct}%` : "—"}
                                  </Text>
                                </Group>
                              </Table.Td>
                              <Table.Td c={c.blankCount > 0 ? undefined : "dimmed"}>
                                {c.blankCount > 0 ? c.blankCount : "—"}
                              </Table.Td>
                              <Table.Td>{c.distinctCount}</Table.Td>
                              <Table.Td style={{ fontFamily: "monospace" }}>
                                {c.min !== null ? String(c.min).slice(0, 16) : "—"}
                              </Table.Td>
                              <Table.Td style={{ fontFamily: "monospace" }}>
                                {c.max !== null ? String(c.max).slice(0, 16) : "—"}
                              </Table.Td>
                              <Table.Td style={{ fontFamily: "monospace" }}>
                                {c.mean !== null ? c.mean.toFixed(2) : "—"}
                              </Table.Td>
                              <Table.Td>
                                <Box
                                  style={{
                                    display: "flex",
                                    flexDirection: "column",
                                    gap: "0.18rem",
                                    minWidth: 140,
                                  }}
                                >
                                  {c.topValues.map(({ value, count }) => {
                                    const barPct =
                                      c.topValues[0].count > 0
                                        ? (count / c.topValues[0].count) * 100
                                        : 0;
                                    return (
                                      <Group key={value} gap="0.3rem" wrap="nowrap">
                                        <Box
                                          style={{
                                            width: 52,
                                            height: 5,
                                            borderRadius: 2,
                                            background: "var(--border)",
                                            position: "relative",
                                            flexShrink: 0,
                                          }}
                                        >
                                          <Box
                                            style={{
                                              position: "absolute",
                                              left: 0,
                                              top: 0,
                                              bottom: 0,
                                              width: `${barPct}%`,
                                              borderRadius: 2,
                                              background: "var(--primary)",
                                            }}
                                          />
                                        </Box>
                                        <Text
                                          span
                                          style={{
                                            fontFamily: "monospace",
                                            fontSize: "0.68rem",
                                            whiteSpace: "nowrap",
                                            overflow: "hidden",
                                            maxWidth: 110,
                                            textOverflow: "ellipsis",
                                          }}
                                          title={value}
                                        >
                                          {value.slice(0, 22)}
                                        </Text>
                                        <Text
                                          span
                                          c="dimmed"
                                          fz="0.65rem"
                                          ml="auto"
                                          style={{ flexShrink: 0 }}
                                        >
                                          ×{count}
                                        </Text>
                                      </Group>
                                    );
                                  })}
                                </Box>
                              </Table.Td>
                            </Table.Tr>
                          );
                        })}
                      </Table.Tbody>
                    </Table>
                  </Table.ScrollContainer>
                </Box>
              );
            })()}
          </>
        )}
        {section === "columns" && (
          <div data-testid="table-section-columns-panel">
            {/* The same topics as the column editor: the name first, a tab's attributes after. */}
            <ColumnTabs value={tab} onChange={setTab} shown={shownTabs} nested />
            <Table.ScrollContainer minWidth={480}>
              <Table className="data-table" style={{ margin: 0 }}>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>{t("tableReadView.colColumn")}</Table.Th>
                    {tab === "naming" && (
                      <>
                        <Table.Th>{t("tableReadView.colSqlAlias")}</Table.Th>
                        <Table.Th w="60%">{t("tableReadView.colDescription")}</Table.Th>
                      </>
                    )}
                    {tab === "type" && (
                      <>
                        <Table.Th>{t("tableEditForm.dataTypeHeader")}</Table.Th>
                        <Table.Th>{t("tableReadView.colPk")}</Table.Th>
                        <Table.Th>{t("tableReadView.colScope")}</Table.Th>
                      </>
                    )}
                    {tab === "access" && (
                      <>
                        <Table.Th>{t("tableReadView.colVisibleTo")}</Table.Th>
                        <Table.Th>{t("tableReadView.colMasking")}</Table.Th>
                      </>
                    )}
                    {tab === "testing" && (
                      <>
                        <Table.Th>{t("testData.fake")}</Table.Th>
                        <Table.Th>{t("testData.syntheticRule")}</Table.Th>
                        <Table.Th>{t("testData.stable")}</Table.Th>
                      </>
                    )}
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {table.columns.map((c) => (
                    <Fragment key={c.id}>
                      <Table.Tr>
                        <Table.Td>
                          {/* REQ-1387: glossary term summary card on column-name hover. */}
                          <ColumnGlossaryHover
                            tableId={table.id}
                            columnName={c.columnName}
                            // Every attribute of the column, whichever tab shows it.
                            definition={[
                              { label: t("tableReadView.colColumn"), value: c.columnName },
                              {
                                label: t("tableReadView.colSqlAlias"),
                                value: c.computedSqlAlias || "",
                              },
                              {
                                label: t("tableReadView.colDescription"),
                                value: c.description || "",
                              },
                              {
                                label: t("tableEditForm.dataTypeHeader"),
                                value: c.dataType ? toIrType(c.dataType) : "",
                              },
                              { label: t("tableReadView.colPk"), value: c.isPrimaryKey ? "✓" : "" },
                              {
                                label: t("tableReadView.colScope"),
                                value: c.scope || t("tableReadView.scopeDomain"),
                              },
                              {
                                label: t("tableReadView.colVisibleTo"),
                                value:
                                  c.visibleTo === null
                                    ? ""
                                    : c.visibleTo.map((e) => grantLabel(e, t)).join(", "),
                              },
                              {
                                label: t("tableReadView.colMasking"),
                                value: c.masks === null ? "" : masksSummary(c.masks, t),
                              },
                            ]}
                          >
                            <code>{c.columnName}</code>
                          </ColumnGlossaryHover>
                          {c.nativeFilterType && (
                            <Badge
                              ml="0.4rem"
                              size="xs"
                              variant="light"
                              color={c.nativeFilterType === "path_param" ? "yellow" : "blue"}
                              style={{ fontFamily: "monospace" }}
                            >
                              {c.nativeFilterType === "path_param"
                                ? t("tableReadView.pathBadge")
                                : t("tableReadView.queryBadge")}
                            </Badge>
                          )}
                          {c.isForeignKey && (
                            <Badge
                              ml="0.4rem"
                              size="xs"
                              variant="light"
                              color="green"
                              style={{ fontFamily: "monospace" }}
                            >
                              {t("tableReadView.fkBadge")}
                            </Badge>
                          )}
                          {c.isAlternateKey && (
                            <Badge
                              ml="0.4rem"
                              size="xs"
                              variant="light"
                              color="yellow"
                              style={{ fontFamily: "monospace" }}
                            >
                              {t("tableReadView.akBadge")}
                            </Badge>
                          )}{" "}
                          <TagControl
                            objectType="column"
                            tableId={table.id}
                            columnName={c.columnName}
                          />
                        </Table.Td>
                        {tab === "naming" && (
                          <>
                            <Table.Td c={c.alias ? "white" : "dimmed"}>
                              {c.computedSqlAlias}
                            </Table.Td>
                            <Table.Td className="reasoning-cell">{c.description || ""}</Table.Td>
                          </>
                        )}
                        {tab === "type" && (
                          <>
                            <Table.Td ff="monospace">
                              {c.dataType ? toIrType(c.dataType) : ""}
                            </Table.Td>
                            <Table.Td ta="center">
                              {c.isPrimaryKey && (
                                <Text span c="blue">
                                  &#10003;
                                </Text>
                              )}
                            </Table.Td>
                            <Table.Td>{c.scope || t("tableReadView.scopeDomain")}</Table.Td>
                          </>
                        )}
                        {tab === "testing" && (
                          <>
                            {/* REQ-1494: what the column shows in test data. No fake is the real
                              value, said as that; a pii column with none is marked. */}
                            <Table.Td data-testid={`read-fake-${c.columnName}`}>
                              {c.fake ? (
                                <Text span ff="monospace" size="sm">
                                  {c.fake}
                                </Text>
                              ) : (
                                <Text span size="sm" c="dimmed">
                                  {t("testData.realValue")}
                                </Text>
                              )}
                              {c.isPii && !c.fake && (
                                <Badge ml="xs" size="xs" color="red" variant="light">
                                  {t("testData.piiNoFake")}
                                </Badge>
                              )}
                            </Table.Td>
                            <Table.Td ff="monospace" data-testid={`read-rule-${c.columnName}`}>
                              {c.syntheticRule || ""}
                            </Table.Td>
                            <Table.Td ta="center">
                              {c.fakeStable && (
                                <Text span c="blue">
                                  &#10003;
                                </Text>
                              )}
                            </Table.Td>
                          </>
                        )}
                        {tab === "access" && (
                          <>
                            {/* REQ-1958/REQ-1134: null grant lists are withheld (the caller lacks
                              view_governance), which is said — never shown as "all" or "none". */}
                            <Table.Td>
                              {c.visibleTo === null ? (
                                <Text span size="sm" c="dimmed" data-testid="grants-not-shown">
                                  {t("tableReadView.notShown")}
                                </Text>
                              ) : (
                                <Group gap={4} wrap="nowrap">
                                  <Text span size="sm">
                                    {c.visibleTo.length > 0
                                      ? c.visibleTo.map((e) => grantLabel(e, t)).join(", ")
                                      : t("tableReadView.all")}
                                  </Text>
                                  {c.visibleTo.length > 0 && (
                                    <OwnerResolutionIcon
                                      refs={c.visibleTo}
                                      ariaLabel={t("ownerResolution.columnIconLabel")}
                                    />
                                  )}
                                </Group>
                              )}
                            </Table.Td>
                            <Table.Td c={c.visibleTo === null ? "dimmed" : undefined}>
                              {c.visibleTo === null
                                ? t("tableReadView.notShown")
                                : c.masks !== null && masksSummary(c.masks, t)}
                            </Table.Td>
                          </>
                        )}
                      </Table.Tr>
                      {tab === "access" && c.masks !== null && c.masks.length > 0 && (
                        <Table.Tr>
                          <Table.Td
                            colSpan={4}
                            c="dimmed"
                            fz="0.75rem"
                            pl="1.5rem"
                            data-testid={`mask-list-read-${c.columnName}`}
                          >
                            {c.masks.map((e, n) => (
                              <div key={n}>
                                {t("maskList.entryLine", {
                                  n: n + 1,
                                  to: maskToLabel(e.to, t),
                                  mask: maskSpecLabel(e.mask, t),
                                })}
                              </div>
                            ))}
                          </Table.Td>
                        </Table.Tr>
                      )}
                    </Fragment>
                  ))}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          </div>
        )}
        {actions}
        {table.draft && (
          <Text
            size="xs"
            c="dimmed"
            px="0.5rem"
            pb="0.25rem"
            data-testid="table-read-view-draft-note"
          >
            {t("tableReadView.draftPreviewNote")}
          </Text>
        )}
        {section === "writes" && (
          // REQ-2074: what each role may write to the table.
          <WriteGrantsPanel
            grants={table.writeGrants}
            columns={table.columns.map((c) => c.columnName)}
            roles={[]}
          />
        )}
      </div>
      {deployMsg[table.id] && (
        <Box
          px="0.75rem"
          py="0.5rem"
          fz="0.8rem"
          c={deployMsg[table.id].success ? "var(--color-success, #22c55e)" : "var(--destructive)"}
        >
          {deployMsg[table.id].message}
        </Box>
      )}
    </>
  );
}
