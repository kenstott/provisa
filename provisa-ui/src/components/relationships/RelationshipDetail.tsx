// Copyright (c) 2026 Kenneth Stott
// Canary: 9c9d740c-8fca-47e8-ad1a-32409bac647d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A relationship's detail, shown in the page's right-hand panel: what it joins, or the form that
// edits it. Its actions sit in the panel's pinned head; the form's Save and Cancel take their
// place while it is open.

import { useTranslation } from "react-i18next";
import { ConfirmDialog } from "../ConfirmDialog";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { ActionIcon, Checkbox, Group, Select, Text, TextInput, NumberInput } from "@mantine/core";
import { Trash2, Pencil, ArrowLeftRight } from "lucide-react";
import type { Relationship, RegisteredTable } from "../../types/admin";
import type { TrackedFunction } from "../../api/actions";
import { tableRef, type RelForm } from "./relationship-types";
import { domainToSqlName } from "../../naming";
import { RelationshipRead } from "./RelationshipRead";
import { JunctionPanel } from "./JunctionPanel";
import { ParametersPanel } from "./ParametersPanel";
import { PanelActions } from "../list/SplitPane";
import { Tip } from "../Tip";

interface RelationshipDetailProps {
  rel: Relationship;
  editingRel: RelForm | null;
  setEditingRel: (f: RelForm | null) => void;
  canManage: boolean;
  onStartEdit: () => void;
  /** Opens the form that creates this relationship's reverse. */
  onReverse: () => void;
  onDelete: () => void;
  onEditSave: () => void;
  saving: string | null;
  /** What the form says of a save that did not go through. */
  problem: string;
  tables: RegisteredTable[];
  functions: TrackedFunction[];
  tableDomainById: Record<string, string>;
  domainsEnabled: boolean;
}

export function RelationshipDetail({
  rel: r,
  editingRel,
  setEditingRel,
  canManage,
  onStartEdit,
  onReverse,
  onDelete,
  onEditSave,
  saving,
  problem,
  tables,
  functions,
  tableDomainById,
  domainsEnabled,
}: RelationshipDetailProps) {
  const { t } = useTranslation();

  return (
    <>
      {/* While the form is open the panel's head is the form's: its Save and Cancel. */}
      {canManage && editingRel === null && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="sm">
              <Tip text={t("relationshipRow.edit")} named>
                <ActionIcon
                  aria-label={t("relationshipRow.edit")}
                  onClick={(e) => {
                    e.stopPropagation();
                    onStartEdit();
                  }}
                >
                  <Pencil size={14} />
                </ActionIcon>
              </Tip>
              <Tip text={t("relationshipRow.generateReverse")} named>
                <ActionIcon
                  aria-label={t("relationshipRow.generateReverse")}
                  onClick={(e) => {
                    e.stopPropagation();
                    onReverse();
                  }}
                >
                  <ArrowLeftRight size={14} />
                </ActionIcon>
              </Tip>
              <ConfirmDialog
                title={t("relationshipRow.deleteTitle", { id: r.id })}
                consequence={t("relationshipRow.deleteConsequence")}
                onConfirm={onDelete}
              >
                {(open) => (
                  <Tip text={t("relationshipRow.delete")} named>
                    <ActionIcon
                      color="red"
                      aria-label={t("relationshipRow.delete")}
                      onClick={(e) => {
                        e.stopPropagation();
                        open();
                      }}
                    >
                      <Trash2 size={14} />
                    </ActionIcon>
                  </Tip>
                )}
              </ConfirmDialog>
            </Group>
          </fieldset>
        </PanelActions>
      )}
      {!editingRel ? (
        <RelationshipRead
          rel={r}
          tableDomainById={tableDomainById}
          domainsEnabled={domainsEnabled}
        />
      ) : (
        <FormLayout>
          <FormField size="medium">
            <TextInput
              label={t("relationshipRow.aliasLabel")}
              value={editingRel.alias}
              onChange={(e) => setEditingRel({ ...editingRel, alias: e.target.value })}
              placeholder={r.computedCypherAlias ?? "PLACED_BY"}
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              label={t("relationshipRow.graphqlAliasLabel")}
              value={editingRel.graphqlAlias}
              onChange={(e) => setEditingRel({ ...editingRel, graphqlAlias: e.target.value })}
              placeholder={r.graphqlAlias ?? ""}
            />
          </FormField>
          {/* Source panel */}
          {(() => {
            const uniqueDomains = [
              ...new Set(tables.map((t2) => domainToSqlName(t2.domainId)).filter(Boolean)),
            ].sort();
            const filteredSrcTables = editingRel.sourceDomain
              ? tables.filter((t2) => domainToSqlName(t2.domainId) === editingRel.sourceDomain)
              : tables;
            return (
              <FormLayout aria-label={t("relationshipRow.source")}>
                <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase">
                  {t("relationshipRow.source")}
                </Text>
                {domainsEnabled && (
                  <FormField size="medium">
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
                  </FormField>
                )}
                <FormField size="medium">
                  <Select
                    label={t("relationshipRow.tableLabel")}
                    data={[
                      { value: "", label: t("relationshipRow.selectPlaceholder") },
                      ...filteredSrcTables.map((t2) => ({
                        value: tableRef(t2),
                        label: tableRef(t2),
                      })),
                    ]}
                    value={editingRel.sourceTableId}
                    onChange={(v) => setEditingRel({ ...editingRel, sourceTableId: v ?? "" })}
                    allowDeselect={false}
                  />
                </FormField>
                <FormField size="medium">
                  <Select
                    label={t("relationshipRow.columnLabel")}
                    data={[
                      { value: "", label: t("relationshipRow.selectPlaceholder") },
                      ...(
                        tables.find((t2) => tableRef(t2) === editingRel.sourceTableId)?.columns ??
                        []
                      ).map((c) => ({ value: c.columnName, label: c.columnName })),
                    ]}
                    value={editingRel.sourceColumn}
                    onChange={(v) => setEditingRel({ ...editingRel, sourceColumn: v ?? "" })}
                    allowDeselect={false}
                  />
                </FormField>
              </FormLayout>
            );
          })()}
          {/* Target panel */}
          <FormLayout aria-label={t("relationshipRow.target")}>
            <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase">
              {t("relationshipRow.target")}
            </Text>
            <FormField size="medium">
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
            </FormField>
            {editingRel.targetType === "table" ? (
              (() => {
                const uniqueDomains = [
                  ...new Set(tables.map((t2) => domainToSqlName(t2.domainId)).filter(Boolean)),
                ].sort();
                const filteredTgtTables = editingRel.targetDomain
                  ? tables.filter((t2) => domainToSqlName(t2.domainId) === editingRel.targetDomain)
                  : tables;
                return (
                  <>
                    {domainsEnabled && (
                      <FormField size="medium">
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
                      </FormField>
                    )}
                    <FormField size="medium">
                      <Select
                        label={t("relationshipRow.tableLabel")}
                        data={[
                          { value: "", label: t("relationshipRow.selectPlaceholder") },
                          ...filteredTgtTables.map((t2) => ({
                            value: tableRef(t2),
                            label: tableRef(t2),
                          })),
                        ]}
                        value={editingRel.targetTableId}
                        onChange={(v) => setEditingRel({ ...editingRel, targetTableId: v ?? "" })}
                        allowDeselect={false}
                      />
                    </FormField>
                    <FormField size="medium">
                      <Select
                        label={t("relationshipRow.columnLabel")}
                        data={[
                          { value: "", label: t("relationshipRow.selectPlaceholder") },
                          ...(
                            tables.find((t2) => tableRef(t2) === editingRel.targetTableId)
                              ?.columns ?? []
                          ).map((c) => ({ value: c.columnName, label: c.columnName })),
                        ]}
                        value={editingRel.targetColumn}
                        onChange={(v) => setEditingRel({ ...editingRel, targetColumn: v ?? "" })}
                        allowDeselect={false}
                      />
                    </FormField>
                  </>
                );
              })()
            ) : (
              <>
                <FormField size="medium">
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
                </FormField>
                <FormField size="medium">
                  <TextInput
                    label={t("relationshipRow.functionArgLabel")}
                    value={editingRel.functionArg}
                    onChange={(e) => setEditingRel({ ...editingRel, functionArg: e.target.value })}
                    placeholder={t("relationshipRow.functionArgPlaceholder")}
                  />
                </FormField>
              </>
            )}
          </FormLayout>
          {editingRel.targetType === "table" && (
            <FormField size="medium">
              <Select
                label={t("relationshipRow.cardinality")}
                data={[
                  { value: "many-to-one", label: t("relationshipRow.cardinalityManyToOne") },
                  { value: "one-to-many", label: t("relationshipRow.cardinalityOneToMany") },
                ]}
                value={editingRel.cardinality}
                onChange={(v) => setEditingRel({ ...editingRel, cardinality: v ?? "many-to-one" })}
                allowDeselect={false}
              />
            </FormField>
          )}
          <FormChecks>
            <FormField size="short">
              <Checkbox
                label={t("relationshipRow.materializeCheckbox")}
                checked={editingRel.materialize}
                onChange={(e) =>
                  setEditingRel({ ...editingRel, materialize: e.currentTarget.checked })
                }
              />
            </FormField>
            <FormField size="short">
              <Checkbox
                label={t("relationshipRow.excludeFromCypher")}
                checked={editingRel.disableCypher}
                onChange={(e) =>
                  setEditingRel({ ...editingRel, disableCypher: e.currentTarget.checked })
                }
              />
            </FormField>
          </FormChecks>
          {editingRel.materialize && (
            <FormField size="short">
              <NumberInput
                label={t("relationshipRow.refreshIntervalSeconds")}
                value={editingRel.refreshInterval}
                onChange={(v) => setEditingRel({ ...editingRel, refreshInterval: String(v) })}
              />
            </FormField>
          )}
          {editingRel.targetType === "table" && editingRel.cardinality === "many-to-one" && (
            <FormField size="full">
              <Text c="orange.7" fz="0.78rem">
                {t("relationshipRow.cardinalityWarning")}
              </Text>
            </FormField>
          )}
          {editingRel.targetType === "table" && (
            <FormField size="full">
              <ParametersPanel
                form={editingRel}
                setForm={setEditingRel}
                tables={tables}
                testIdPrefix="rel-edit"
              />
            </FormField>
          )}
          {editingRel.targetType === "table" && (
            <FormField size="full">
              <JunctionPanel
                form={editingRel}
                setForm={setEditingRel}
                tables={tables}
                testIdPrefix="rel-edit"
              />
            </FormField>
          )}
          <FormFooter
            panel
            problem={problem || undefined}
            onSave={onEditSave}
            saving={!!saving}
            onCancel={() => setEditingRel(null)}
          />
        </FormLayout>
      )}
    </>
  );
}
