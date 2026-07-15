// Copyright (c) 2026 Kenneth Stott
// Canary: 9c9d740c-8fca-47e8-ad1a-32409bac647d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Checkbox,
  Group,
  Select,
  Stack,
  Text,
  TextInput,
  NumberInput,
} from "@mantine/core";
import { Trash2, Pencil, Check, X, ArrowLeftRight } from "lucide-react";
import type { Relationship, RegisteredTable } from "../../types/admin";
import type { TrackedFunction } from "../../api/actions";
import type { RelForm } from "./relationship-types";

interface RelationshipRowProps {
  rel: Relationship;
  isExpanded: boolean;
  onToggle: () => void;
  editingRel: RelForm | null;
  setEditingRel: (f: RelForm | null) => void;
  canManage: boolean;
  onStartEdit: () => void;
  onReverse: () => void;
  onDelete: () => void;
  onEditSave: () => void;
  saving: string | null;
  tables: RegisteredTable[];
  functions: TrackedFunction[];
  tableDomainById: Record<string, string>;
  normalizeDomain: (id: string) => string;
  domainsEnabled: boolean;
}

export function RelationshipRow({
  rel: r,
  isExpanded,
  onToggle,
  editingRel,
  setEditingRel,
  canManage,
  onStartEdit,
  onReverse,
  onDelete,
  onEditSave,
  saving,
  tables,
  functions,
  tableDomainById,
  normalizeDomain,
  domainsEnabled,
}: RelationshipRowProps) {
  const { t } = useTranslation();

  const targetLabel = (() => {
    if (r.targetFunctionName) return `fn:${r.targetFunctionName}(${r.functionArg ?? ""})`;
    const tDomain = domainsEnabled ? tableDomainById[r.targetTableId!] : undefined;
    const sDomain = domainsEnabled ? normalizeDomain(r.sourceDomainId ?? "") : undefined;
    return tDomain && tDomain !== sDomain
      ? `${tDomain}.${r.targetTableName}.${r.targetColumn}`
      : `${r.targetTableName}.${r.targetColumn}`;
  })();

  return (
    <React.Fragment>
      <tr
        onClick={onToggle}
        role="button"
        tabIndex={0}
        aria-expanded={isExpanded}
        aria-label={t("relationshipRow.toggleDetails")}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggle();
          }
        }}
        style={{
          cursor: "pointer",
          background: isExpanded ? "var(--surface)" : undefined,
        }}
      >
        {domainsEnabled && (
          <td style={{ whiteSpace: "nowrap" }}>{r.sourceDomainId || t("relationshipRow.none")}</td>
        )}
        <td style={{ wordBreak: "break-all", overflowWrap: "anywhere" }}>
          <Group gap="0.4rem" wrap="nowrap">
            {r.autoSuggested && (
              <Badge
                title={t("relationshipRow.autoTrackedTitle")}
                size="xs"
                color="gray"
                variant="filled"
                style={{ flexShrink: 0 }}
              >
                {t("relationshipRow.fkBadge")}
              </Badge>
            )}
            <span>{`${r.sourceTableName}.${r.sourceColumn}`}</span>
          </Group>
        </td>
        <td style={{ wordBreak: "break-all", overflowWrap: "anywhere" }}>{targetLabel}</td>
        <td>
          <div style={{ fontSize: "0.8rem", lineHeight: 1.4 }}>
            <div>
              <Text span c="dimmed">
                {t("relationshipRow.gqlLabel")}
              </Text>{" "}
              <code>{r.graphqlAlias ?? t("relationshipRow.none")}</code>
            </div>
            <div>
              <Text span c="dimmed">
                {t("relationshipRow.cqlLabel")}
              </Text>{" "}
              <code>
                {r.alias ?? (
                  <Text span c="dimmed" fs="italic">
                    {r.computedCypherAlias ?? t("relationshipRow.none")}
                  </Text>
                )}
              </code>
            </div>
          </div>
        </td>
        <td>{r.cardinality}</td>
        <td>{r.materialize ? t("relationshipRow.yes") : t("relationshipRow.no")}</td>
        <td>{r.materialize ? r.refreshInterval : t("relationshipRow.none")}</td>
      </tr>
      {isExpanded && (
        <tr>
          <td
            colSpan={domainsEnabled ? 7 : 6}
            style={{
              padding: "0.75rem 1rem",
              background: "var(--bg)",
              borderTop: "1px solid var(--border)",
            }}
          >
            {!editingRel ? (
              <Stack gap="sm">
                <dl
                  style={{
                    display: "grid",
                    gridTemplateColumns: "max-content 1fr",
                    gap: "0.25rem 1rem",
                    margin: 0,
                    color: "var(--text)",
                  }}
                >
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>{t("relationshipRow.source")}</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    {`${r.sourceTableName}.${r.sourceColumn}`}
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>{t("relationshipRow.target")}</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>{targetLabel}</dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>{t("relationshipRow.gqlAlias")}</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    <code>{r.graphqlAlias ?? t("relationshipRow.none")}</code>
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>{t("relationshipRow.cqlAlias")}</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    <code>
                      {r.alias ?? (
                        <Text span c="dimmed" fs="italic">
                          {r.computedCypherAlias ?? t("relationshipRow.none")}
                        </Text>
                      )}
                    </code>
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>{t("relationshipRow.cardinality")}</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>{r.cardinality}</dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>{t("relationshipRow.materialize")}</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    {r.materialize ? t("relationshipRow.yes") : t("relationshipRow.no")}
                  </dd>
                  {r.materialize && (
                    <>
                      <dt style={{ color: "var(--text-muted)" }}>
                        <strong>{t("relationshipRow.refreshIntervalSeconds")}</strong>
                      </dt>
                      <dd style={{ color: "var(--text)", margin: 0 }}>
                        {r.refreshInterval ?? t("relationshipRow.none")}
                      </dd>
                    </>
                  )}
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>{t("relationshipRow.cypherGraph")}</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    {r.disableCypher ? (
                      <Text span c="dimmed" fs="italic">
                        {t("relationshipRow.excluded")}
                      </Text>
                    ) : (
                      t("relationshipRow.included")
                    )}
                  </dd>
                </dl>
                {canManage && (
                  <Group gap="sm" mt="0.25rem">
                    <ActionIcon
                      variant="subtle"
                      aria-label={t("relationshipRow.edit")}
                      title={t("relationshipRow.edit")}
                      onClick={(e) => {
                        e.stopPropagation();
                        onStartEdit();
                      }}
                    >
                      <Pencil size={14} />
                    </ActionIcon>
                    <ActionIcon
                      variant="subtle"
                      aria-label={t("relationshipRow.generateReverse")}
                      title={t("relationshipRow.generateReverse")}
                      onClick={(e) => {
                        e.stopPropagation();
                        onReverse();
                      }}
                    >
                      <ArrowLeftRight size={14} />
                    </ActionIcon>
                    <ActionIcon
                      variant="subtle"
                      color="red"
                      aria-label={t("relationshipRow.delete")}
                      title={t("relationshipRow.delete")}
                      onClick={(e) => {
                        e.stopPropagation();
                        onDelete();
                      }}
                    >
                      <Trash2 size={14} />
                    </ActionIcon>
                  </Group>
                )}
              </Stack>
            ) : (
              <Stack gap="0.75rem">
                <Group gap="sm" wrap="wrap">
                  <TextInput
                    label={t("relationshipRow.aliasLabel")}
                    value={editingRel.alias}
                    onChange={(e) => setEditingRel({ ...editingRel, alias: e.target.value })}
                    placeholder={r.computedCypherAlias ?? "PLACED_BY"}
                  />
                  <TextInput
                    label={t("relationshipRow.graphqlAliasLabel")}
                    value={editingRel.graphqlAlias}
                    onChange={(e) => setEditingRel({ ...editingRel, graphqlAlias: e.target.value })}
                    placeholder={r.graphqlAlias ?? ""}
                  />
                </Group>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
                  {/* Source panel */}
                  {(() => {
                    const uniqueDomains = [
                      ...new Set(tables.map((t2) => normalizeDomain(t2.domainId)).filter(Boolean)),
                    ].sort();
                    const filteredSrcTables = editingRel.sourceDomain
                      ? tables.filter((t2) => normalizeDomain(t2.domainId) === editingRel.sourceDomain)
                      : tables;
                    return (
                      <Stack
                        gap="sm"
                        style={{
                          border: "1px solid var(--border)",
                          borderRadius: "4px",
                          padding: "0.75rem",
                        }}
                      >
                        <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase">
                          {t("relationshipRow.source")}
                        </Text>
                        {domainsEnabled && (
                          <Select
                            label={t("relationshipRow.domainLabel")}
                            data={[
                              { value: "", label: t("relationshipRow.domainAll") },
                              ...uniqueDomains.map((d) => ({ value: d, label: d })),
                            ]}
                            value={editingRel.sourceDomain}
                            onChange={(v) =>
                              setEditingRel({
                                ...editingRel,
                                sourceDomain: v ?? "",
                                sourceTableId: "",
                              })
                            }
                            allowDeselect={false}
                          />
                        )}
                        <Select
                          label={t("relationshipRow.tableLabel")}
                          data={[
                            { value: "", label: t("relationshipRow.selectPlaceholder") },
                            ...filteredSrcTables.map((t2) => ({ value: t2.tableName, label: t2.tableName })),
                          ]}
                          value={editingRel.sourceTableId}
                          onChange={(v) => setEditingRel({ ...editingRel, sourceTableId: v ?? "" })}
                          allowDeselect={false}
                        />
                        <Select
                          label={t("relationshipRow.columnLabel")}
                          data={[
                            { value: "", label: t("relationshipRow.selectPlaceholder") },
                            ...(tables.find((t2) => t2.tableName === editingRel.sourceTableId)?.columns ?? []).map(
                              (c) => ({ value: c.columnName, label: c.columnName }),
                            ),
                          ]}
                          value={editingRel.sourceColumn}
                          onChange={(v) => setEditingRel({ ...editingRel, sourceColumn: v ?? "" })}
                          allowDeselect={false}
                        />
                      </Stack>
                    );
                  })()}
                  {/* Target panel */}
                  <Stack
                    gap="sm"
                    style={{
                      border: "1px solid var(--border)",
                      borderRadius: "4px",
                      padding: "0.75rem",
                    }}
                  >
                    <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase">
                      {t("relationshipRow.target")}
                    </Text>
                    <Select
                      label={t("relationshipRow.targetTypeLabel")}
                      data={[
                        { value: "table", label: t("relationshipRow.targetTypeTable") },
                        { value: "function", label: t("relationshipRow.targetTypeFunction") },
                      ]}
                      value={editingRel.targetType}
                      onChange={(v) =>
                        setEditingRel({
                          ...editingRel,
                          targetType: (v ?? "table") as "table" | "function",
                          targetTableId: "",
                          targetColumn: "",
                          targetFunctionName: "",
                          functionArg: "",
                        })
                      }
                      allowDeselect={false}
                    />
                    {editingRel.targetType === "table" ? (
                      (() => {
                        const uniqueDomains = [
                          ...new Set(tables.map((t2) => normalizeDomain(t2.domainId)).filter(Boolean)),
                        ].sort();
                        const filteredTgtTables = editingRel.targetDomain
                          ? tables.filter(
                              (t2) => normalizeDomain(t2.domainId) === editingRel.targetDomain,
                            )
                          : tables;
                        return (
                          <>
                            {domainsEnabled && (
                              <Select
                                label={t("relationshipRow.domainLabel")}
                                data={[
                                  { value: "", label: t("relationshipRow.domainAll") },
                                  ...uniqueDomains.map((d) => ({ value: d, label: d })),
                                ]}
                                value={editingRel.targetDomain}
                                onChange={(v) =>
                                  setEditingRel({
                                    ...editingRel,
                                    targetDomain: v ?? "",
                                    targetTableId: "",
                                  })
                                }
                                allowDeselect={false}
                              />
                            )}
                            <Select
                              label={t("relationshipRow.tableLabel")}
                              data={[
                                { value: "", label: t("relationshipRow.selectPlaceholder") },
                                ...filteredTgtTables.map((t2) => ({ value: t2.tableName, label: t2.tableName })),
                              ]}
                              value={editingRel.targetTableId}
                              onChange={(v) => setEditingRel({ ...editingRel, targetTableId: v ?? "" })}
                              allowDeselect={false}
                            />
                            <Select
                              label={t("relationshipRow.columnLabel")}
                              data={[
                                { value: "", label: t("relationshipRow.selectPlaceholder") },
                                ...(
                                  tables.find((t2) => t2.tableName === editingRel.targetTableId)?.columns ?? []
                                ).map((c) => ({ value: c.columnName, label: c.columnName })),
                              ]}
                              value={editingRel.targetColumn}
                              onChange={(v) => setEditingRel({ ...editingRel, targetColumn: v ?? "" })}
                              allowDeselect={false}
                            />
                          </>
                        );
                      })()
                    ) : (
                      <>
                        <Select
                          label={t("relationshipRow.functionLabel")}
                          data={[
                            { value: "", label: t("relationshipRow.selectPlaceholder") },
                            ...functions.map((f) => ({ value: f.name, label: f.name })),
                          ]}
                          value={editingRel.targetFunctionName}
                          onChange={(v) => setEditingRel({ ...editingRel, targetFunctionName: v ?? "" })}
                          allowDeselect={false}
                        />
                        <TextInput
                          label={t("relationshipRow.functionArgLabel")}
                          value={editingRel.functionArg}
                          onChange={(e) => setEditingRel({ ...editingRel, functionArg: e.target.value })}
                          placeholder={t("relationshipRow.functionArgPlaceholder")}
                        />
                      </>
                    )}
                  </Stack>
                </div>
                <Group gap="sm" align="flex-end" wrap="wrap">
                  {editingRel.targetType === "table" && (
                    <Select
                      label={t("relationshipRow.cardinality")}
                      data={[
                        { value: "many-to-one", label: t("relationshipRow.cardinalityManyToOne") },
                        { value: "one-to-many", label: t("relationshipRow.cardinalityOneToMany") },
                      ]}
                      value={editingRel.cardinality}
                      onChange={(v) => setEditingRel({ ...editingRel, cardinality: v ?? "many-to-one" })}
                      allowDeselect={false}
                      style={{ width: `${editingRel.cardinality.length + 6}ch` }}
                    />
                  )}
                  <Checkbox
                    label={t("relationshipRow.materializeCheckbox")}
                    checked={editingRel.materialize}
                    onChange={(e) => setEditingRel({ ...editingRel, materialize: e.currentTarget.checked })}
                  />
                  <Checkbox
                    label={t("relationshipRow.excludeFromCypher")}
                    checked={editingRel.disableCypher}
                    onChange={(e) =>
                      setEditingRel({ ...editingRel, disableCypher: e.currentTarget.checked })
                    }
                  />
                  {editingRel.materialize && (
                    <NumberInput
                      label={t("relationshipRow.refreshIntervalSeconds")}
                      value={editingRel.refreshInterval}
                      onChange={(v) => setEditingRel({ ...editingRel, refreshInterval: String(v) })}
                    />
                  )}
                </Group>
                {editingRel.targetType === "table" && editingRel.cardinality === "many-to-one" && (
                  <Text c="orange.7" fz="0.78rem">
                    {t("relationshipRow.cardinalityWarning")}
                  </Text>
                )}
                <Group gap="sm" justify="flex-end">
                  <ActionIcon
                    variant="subtle"
                    aria-label={t("relationshipRow.cancel")}
                    title={t("relationshipRow.cancel")}
                    onClick={() => setEditingRel(null)}
                  >
                    <X size={14} />
                  </ActionIcon>
                  <ActionIcon
                    variant="filled"
                    aria-label={t("relationshipRow.save")}
                    title={t("relationshipRow.save")}
                    onClick={onEditSave}
                    disabled={!!saving}
                  >
                    <Check size={14} />
                  </ActionIcon>
                </Group>
              </Stack>
            )}
          </td>
        </tr>
      )}
    </React.Fragment>
  );
}
