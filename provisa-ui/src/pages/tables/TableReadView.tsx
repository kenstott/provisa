// Copyright (c) 2026 Kenneth Stott
// Canary: 2b6d9e47-3f1a-4c8b-a5e2-7d0f4c9b2e65
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment } from "react";
import { useTranslation } from "react-i18next";
import { Trash2, Pencil } from "lucide-react";
import { ActionIcon, Badge, Box, Button, Group, Table, Text } from "@mantine/core";
import type { NavigateFunction } from "react-router-dom";
import type { RegisteredTable } from "../../types/admin";
import { computeProfile } from "./helpers";

interface TableProfileResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
}

interface TableReadViewProps {
  t: RegisteredTable;
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
}

export function TableReadView({
  t: table,
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
}: TableReadViewProps) {
  const { t } = useTranslation();

  return (
    <>
      {table.description && (
        <Box
          px="0.75rem"
          py="0.5rem"
          fz="0.85rem"
          c="dimmed"
          style={{ borderBottom: "1px solid var(--border)" }}
        >
          {table.description}
        </Box>
      )}
      <Table.ScrollContainer minWidth={640}>
        <Table className="data-table" style={{ margin: 0 }}>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("tableReadView.colColumn")}</Table.Th>
              <Table.Th>{t("tableReadView.colPk")}</Table.Th>
              <Table.Th>{t("tableReadView.colSqlAlias")}</Table.Th>
              <Table.Th>{t("tableReadView.colDescription")}</Table.Th>
              <Table.Th>{t("tableReadView.colVisibleTo")}</Table.Th>
              <Table.Th>{t("tableReadView.colWritableBy")}</Table.Th>
              <Table.Th>{t("tableReadView.colMasking")}</Table.Th>
              <Table.Th>{t("tableReadView.colScope")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {table.columns.map((c) => (
              <Fragment key={c.id}>
                <Table.Tr>
                  <Table.Td>
                    <code>{c.columnName}</code>
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
                    )}
                  </Table.Td>
                  <Table.Td ta="center">
                    {c.isPrimaryKey && (
                      <Text span c="blue">
                        &#10003;
                      </Text>
                    )}
                  </Table.Td>
                  <Table.Td c={c.alias ? "white" : "dimmed"}>{c.computedSqlAlias}</Table.Td>
                  <Table.Td className="reasoning-cell">{c.description || ""}</Table.Td>
                  <Table.Td>
                    {c.visibleTo.length > 0 ? c.visibleTo.join(", ") : t("tableReadView.all")}
                  </Table.Td>
                  <Table.Td>
                    {c.writableBy.length > 0 ? c.writableBy.join(", ") : t("tableReadView.none")}
                  </Table.Td>
                  <Table.Td>{c.maskType || t("tableReadView.maskNone")}</Table.Td>
                  <Table.Td>{c.scope || t("tableReadView.scopeDomain")}</Table.Td>
                </Table.Tr>
                {c.maskType && (
                  <Table.Tr>
                    <Table.Td colSpan={2} c="dimmed" fz="0.75rem" pl="1.5rem">
                      ↳{" "}
                      {c.maskType === "regex"
                        ? t("tableReadView.maskRegex", {
                            pattern: c.maskPattern,
                            replace: c.maskReplace,
                          })
                        : c.maskType === "constant"
                          ? t("tableReadView.maskConstant", {
                              value: c.maskValue ?? t("tableReadView.maskConstantNull"),
                            })
                          : t("tableReadView.maskTruncate", { precision: c.maskPrecision })}
                    </Table.Td>
                    <Table.Td colSpan={4} c="dimmed" fz="0.75rem">
                      {t("tableReadView.unmaskedTo", {
                        list:
                          c.unmaskedTo.length > 0
                            ? c.unmaskedTo.join(", ")
                            : t("tableReadView.none"),
                      })}
                    </Table.Td>
                  </Table.Tr>
                )}
              </Fragment>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {table.apiEndpoint && (
        <Box px="0.75rem" py="0.5rem" fz="0.85rem" c="dimmed">
          {t("tableReadView.apiEndpoint")} <code>{table.apiEndpoint}</code>
        </Box>
      )}
      {table.watermarkColumn && (
        <Box px="0.75rem" py="0.5rem" fz="0.85rem" c="dimmed">
          {t("tableReadView.watermarkColumn")} <code>{table.watermarkColumn}</code>
        </Box>
      )}
      {table.viewSql && (
        <Box px="0.75rem" py="0.5rem" fz="0.85rem">
          <Text span c="dimmed" mr="0.5rem">
            {t("tableReadView.viewSql")}
          </Text>
          <code style={{ fontSize: "0.78rem", wordBreak: "break-all" }}>
            {table.viewSql.length > 120 ? table.viewSql.slice(0, 120) + "…" : table.viewSql}
          </code>
        </Box>
      )}
      <Group px="0.75rem" py="0.5rem" gap="0.4rem">
        <Text c="dimmed">{t("tableReadView.dataProduct")}</Text>
        {table.dataProduct ? (
          <Text c="var(--color-success, #22c55e)" fw={600}>
            {t("tableReadView.dataProductYes")}
          </Text>
        ) : (
          <Text c="dimmed">{t("tableReadView.dataProductNo")}</Text>
        )}
      </Group>
      <Group justify="flex-start" p="0.5rem" gap="0.5rem" wrap="wrap">
        {table.viewSql && (
          <Button
            size="compact-sm"
            variant="default"
            data-testid="table-read-view-edit-sql"
            onClick={(e) => {
              e.stopPropagation();
              navigate("/sql", { state: { sql: table.viewSql, viewTable: table } });
            }}
            title={t("tableReadView.editSqlTitle")}
          >
            {viewsOnly ? t("tableReadView.editSqlButton") : t("tableReadView.openInExplorerButton")}
          </Button>
        )}
        {table.canDeployToDb && (
          <Button
            size="compact-sm"
            variant="default"
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
            title={t("tableReadView.deployToDbTitle")}
            disabled={deploying[table.id]}
          >
            {deploying[table.id]
              ? t("tableReadView.deployingButton")
              : t("tableReadView.deployToDbButton")}
          </Button>
        )}
        <Button
          size="compact-sm"
          variant="default"
          data-testid="table-read-view-profile"
          onClick={(e) => {
            e.stopPropagation();
            handleProfile(table.id);
          }}
          title={t("tableReadView.profileTitle")}
          disabled={tableProfiles[table.id] === "loading"}
        >
          {tableProfiles[table.id] === "loading"
            ? t("tableReadView.profilingButton")
            : t("tableReadView.profileButton")}
        </Button>
        <Button
          size="compact-sm"
          variant="subtle"
          data-testid="table-read-view-policies"
          title={t("tableReadView.policiesTitle")}
          onClick={(e) => {
            e.stopPropagation();
            navigate("/security/rls", {
              state: { tableFilter: table.tableName },
            });
          }}
        >
          {t("tableReadView.policiesButton")}
        </Button>
        <ActionIcon
          variant="subtle"
          aria-label={t("tableReadView.editButtonLabel", { name: table.tableName })}
          data-testid="table-read-view-edit"
          onClick={(e) => {
            e.stopPropagation();
            startEditing(table);
          }}
        >
          <Pencil size={14} />
        </ActionIcon>
        <ActionIcon
          variant="subtle"
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
      </Group>
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
                                  <Text span c="dimmed" fz="0.65rem" ml="auto" style={{ flexShrink: 0 }}>
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
  );
}
