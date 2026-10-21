// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3e91a2-5b48-4d6f-a0e7-2f81d9b46c35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Group,
  MultiSelect,
  Paper,
  Select,
  Stack,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import type { RegisteredTable } from "../../types/admin";
import type { TrackedFunction } from "../../api/actions";
import { CustomPropertiesEditor } from "./CustomPropertiesEditor";
import type { DataProductForm } from "./types";
import { relationName } from "../../naming";
import { FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";
import { assignableDomainIds } from "../../lib/internalDomains";

export interface DataProductFormCardProps {
  editingId: string | null;
  form: DataProductForm;
  setForm: React.Dispatch<React.SetStateAction<DataProductForm>>;
  domainOptions: string[];
  roleOptions: string[];
  tables: RegisteredTable[];
  selectedTableIds: string[];
  setSelectedTableIds: React.Dispatch<React.SetStateAction<string[]>>;
  functions: TrackedFunction[];
  selectedFunctionNames: string[];
  setSelectedFunctionNames: React.Dispatch<React.SetStateAction<string[]>>;
  saving: boolean;
  msg: string;
  onSave: () => void;
  onCancel: () => void;
}

// One titled panel. `errors` is how many of its fields currently show an error; the panel shows it
// has one by its border and badge, so a problem in a panel the eye is not on is still found.
function Panel({
  id,
  title,
  errors,
  children,
}: {
  id: string;
  title: string;
  errors: number;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  return (
    <Paper
      withBorder
      p="md"
      data-testid={`data-product-panel-${id}`}
      data-has-error={errors > 0 ? "true" : undefined}
      style={errors > 0 ? { borderColor: "var(--mantine-color-red-6)" } : undefined}
    >
      <Group justify="space-between" mb="sm" wrap="nowrap">
        <Title order={6}>{title}</Title>
        {errors > 0 && (
          <Badge color="red" variant="light" data-testid={`data-product-panel-${id}-error`}>
            {t("dataProductsTab.panelHasError")}
          </Badge>
        )}
      </Group>
      {/* The panel’s children are the shared form layout; it flows by the panel’s own width. */}
      <Stack gap="sm">{children}</Stack>
    </Paper>
  );
}

// Inline create/edit form — the app-wide pattern: no modal, the form renders in
// place (creation card above the table, edit inside the expanded detail row).
export function DataProductFormCard({
  editingId,
  form,
  setForm,
  domainOptions,
  roleOptions,
  tables,
  selectedTableIds,
  setSelectedTableIds,
  functions,
  selectedFunctionNames,
  setSelectedFunctionNames,
  saving,
  msg,
  onSave,
  onCancel,
}: DataProductFormCardProps) {
  const { t } = useTranslation();
  // Required fields are checked when Save is pressed, so the empty ones say so on the field instead
  // of leaving a disabled button with no reason.
  const [submitted, setSubmitted] = useState(false);
  const missing = {
    id: !form.id.trim(),
    domainId: !form.domainId.trim(),
    name: !form.name.trim(),
  };
  const err = (isMissing: boolean) =>
    submitted && isMissing ? t("dataProductsTab.fieldRequired") : undefined;
  const identityErrors = submitted
    ? Number(missing.id) + Number(missing.domainId) + Number(missing.name)
    : 0;
  const handleSave = () => {
    setSubmitted(true);
    if (missing.id || missing.domainId || missing.name) return;
    onSave();
  };
  return (
    <Stack gap="md" data-testid="data-product-form">
      {/* REQ-1634: the panels flow into columns by the width of the form itself, not the viewport:
          the same form sits in a full-width creation card and in the narrower expanded detail
          row. Inside a panel the fields flow in rows by the shared form layout. */}
      <div className="form-panels">
        <Panel id="identity" title={t("dataProductsTab.panelIdentity")} errors={identityErrors}>
          <FormLayout>
            <FormField size="medium">
              <TextInput
                label={t("dataProductsTab.idLabel")}
                required
                value={form.id}
                disabled={editingId !== null}
                onChange={(e) => setForm((f) => ({ ...f, id: e.target.value }))}
                placeholder={t("dataProductsTab.idPlaceholder")}
                error={err(missing.id)}
                data-testid="data-product-id-input"
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("dataProductsTab.nameLabel")}
                required
                value={form.name}
                onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                placeholder={t("dataProductsTab.namePlaceholder")}
                error={err(missing.name)}
                data-testid="data-product-name-input"
              />
            </FormField>
            <FormField size="medium">
              <Select
                label={t("dataProductsTab.domainLabel")}
                required
                value={form.domainId}
                onChange={(v) => setForm((f) => ({ ...f, domainId: v ?? "" }))}
                data={assignableDomainIds(domainOptions)}
                searchable
                error={err(missing.domainId)}
                data-testid="data-product-domain-input"
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("dataProductsTab.versionLabel")}
                value={form.version}
                onChange={(e) => setForm((f) => ({ ...f, version: e.target.value }))}
                placeholder={t("dataProductsTab.versionPlaceholder")}
                data-testid="data-product-version-input"
              />
            </FormField>
          </FormLayout>
          {/* The backend holds status as free text (DataProduct.status: str | None), so it stays a
              text input; the placeholder lists the usual values. */}
          <FormLayout>
            <FormField size="medium">
              <TextInput
                label={t("dataProductsTab.statusLabel")}
                value={form.status}
                onChange={(e) => setForm((f) => ({ ...f, status: e.target.value }))}
                placeholder={t("dataProductsTab.statusPlaceholder")}
                data-testid="data-product-status-input"
              />
            </FormField>
          </FormLayout>
        </Panel>
        <Panel id="ownership" title={t("dataProductsTab.panelOwnership")} errors={0}>
          <FormLayout>
            <FormField size="medium">
              <Select
                label={t("dataProductsTab.ownerLabel")}
                value={form.ownerRole || null}
                onChange={(v) => setForm((f) => ({ ...f, ownerRole: v ?? "" }))}
                data={roleOptions}
                placeholder={t("dataProductsTab.ownerPlaceholder")}
                searchable
                clearable
                data-testid="data-product-owner-input"
              />
            </FormField>
            <FormField size="medium">
              <Select
                label={t("dataProductsTab.teamLabel")}
                value={form.teamRole || null}
                onChange={(v) => setForm((f) => ({ ...f, teamRole: v ?? "" }))}
                data={roleOptions}
                placeholder={t("dataProductsTab.teamPlaceholder")}
                searchable
                clearable
                data-testid="data-product-team-input"
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("dataProductsTab.supportLabel")}
                value={form.support}
                onChange={(e) => setForm((f) => ({ ...f, support: e.target.value }))}
                placeholder={t("dataProductsTab.supportPlaceholder")}
                data-testid="data-product-support-input"
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("dataProductsTab.slaLabel")}
                value={form.sla}
                onChange={(e) => setForm((f) => ({ ...f, sla: e.target.value }))}
                placeholder={t("dataProductsTab.slaPlaceholder")}
                data-testid="data-product-sla-input"
              />
            </FormField>
          </FormLayout>
        </Panel>
        <Panel id="description" title={t("dataProductsTab.panelDescription")} errors={0}>
          <FormLayout>
            <FormField size="full">
              <Textarea
                label={t("dataProductsTab.purposeLabel")}
                value={form.purpose}
                onChange={(e) => setForm((f) => ({ ...f, purpose: e.target.value }))}
                placeholder={t("dataProductsTab.purposePlaceholder")}
                rows={2}
                data-testid="data-product-purpose-input"
              />
            </FormField>
            <FormField size="full">
              <Textarea
                label={t("dataProductsTab.limitationsLabel")}
                value={form.limitations}
                onChange={(e) => setForm((f) => ({ ...f, limitations: e.target.value }))}
                placeholder={t("dataProductsTab.limitationsPlaceholder")}
                rows={2}
                data-testid="data-product-limitations-input"
              />
            </FormField>
            <FormField size="full">
              <Textarea
                label={t("dataProductsTab.usageLabel")}
                value={form.usage}
                onChange={(e) => setForm((f) => ({ ...f, usage: e.target.value }))}
                placeholder={t("dataProductsTab.usagePlaceholder")}
                rows={2}
                data-testid="data-product-usage-input"
              />
            </FormField>
          </FormLayout>
          <div>
            <CustomPropertiesEditor
              value={form.customProperties}
              onChange={(v) => setForm((f) => ({ ...f, customProperties: v }))}
            />
          </div>
        </Panel>
        <Panel id="contents" title={t("dataProductsTab.panelContents")} errors={0}>
          <FormLayout>
            <FormField help={t("dataProductsTab.tablesDesc")} size="long">
              <MultiSelect
                label={t("dataProductsTab.tablesLabel")}
                placeholder={
                  form.domainId
                    ? t("dataProductsTab.tablesPlaceholder")
                    : t("dataProductsTab.tablesNoDomain")
                }
                disabled={!form.domainId}
                value={selectedTableIds}
                onChange={setSelectedTableIds}
                // REQ-1634: a table may only join a data product owned by its own domain. A checker
                // table is toggled as any member is (REQ-1443 PRODUCT ASSIGNMENT).
                data={tables
                  .filter((tb) => tb.domainId === form.domainId)
                  .map((tb) => ({ value: String(tb.id), label: relationName(tb) }))}
                searchable
                data-testid="data-product-tables-input"
              />
            </FormField>
            <FormField help={t("dataProductsTab.commandsDesc")} size="long">
              <MultiSelect
                label={t("dataProductsTab.commandsLabel")}
                placeholder={
                  form.domainId
                    ? t("dataProductsTab.commandsPlaceholder")
                    : t("dataProductsTab.commandsNoDomain")
                }
                disabled={!form.domainId}
                value={selectedFunctionNames}
                onChange={setSelectedFunctionNames}
                // REQ-1634: a command may only join a data product owned by its own domain.
                data={functions
                  .filter((fn) => fn.domainId === form.domainId)
                  .map((fn) => ({ value: fn.name, label: fn.name }))}
                searchable
                data-testid="data-product-commands-input"
              />
            </FormField>
          </FormLayout>
        </Panel>
      </div>
      {/* Save and Cancel stay at the bottom of the viewport while the panels scroll under them. */}
      <Stack
        gap="xs"
        p="sm"
        data-testid="data-product-form-footer"
        style={{
          position: "sticky",
          bottom: 0,
          zIndex: 2,
          background: "var(--mantine-color-body)",
          borderTop: "1px solid var(--mantine-color-default-border)",
        }}
      >
        {msg && (
          <Alert color="red" data-testid="data-product-form-error">
            {msg}
          </Alert>
        )}
        <FormFooter
          onSave={handleSave}
          saving={saving}
          onCancel={onCancel}
          saveProps={{ "data-testid": "data-product-save-button" }}
          cancelProps={{ "data-testid": "data-product-cancel-button" }}
        />
      </Stack>
    </Stack>
  );
}
