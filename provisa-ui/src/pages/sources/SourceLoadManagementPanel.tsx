// Copyright (c) 2026 Kenneth Stott
// Canary: 3b8f5e20-6c1d-4a97-8e43-f2d9a7c05b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The source form's "Load Management and Recency Controls" panel: every operator setting that
// governs how hard Provisa leans on this source and how fresh its data is — caching, the change
// signal, replication and load protection (REQ-826/1141), grouped and collapsed by default.

import { useTranslation } from "react-i18next";
import { Checkbox, Group, NumberInput, Select, Text, TextInput, Tooltip } from "@mantine/core";
import { sourceChangeSignals } from "../../liveCapability";
import { CHANGE_SIGNAL_LABELS } from "./constants";
import { CollapsibleSection } from "../tables/CollapsibleSection";
import { ReplicateSelect } from "../../components/admin/ReplicateSelect";
import type { SourceFormFieldsProps } from "./SourceFormFields";
import {
  maxLiveConcurrencyValid,
  sentinelPathValid,
  sourceFormCacheTtl,
  sourceFormLanded,
  ttlSignalMissingCacheTtl,
} from "./loadManagement";

export function SourceLoadManagementPanel({
  form,
  setForm,
}: Pick<SourceFormFieldsProps, "form" | "setForm">) {
  const { t } = useTranslation();
  return (
    <CollapsibleSection
      title={t("sourceFormFieldsExtended.loadManagementTitle")}
      testId="source-load-management-panel"
      info={{
        label: t("sourceFormFieldsExtended.loadManagementInfoLabel"),
        text: t("sourceFormFieldsExtended.loadManagementInfo"),
      }}
    >
      {/* The operator's load and recency controls for this source, grouped: caching, the
            change signal, replication and load protection (REQ-826/1141). */}
      <Text size="xs" c="dimmed" data-testid="source-load-management-help">
        {t("sourceFormFieldsExtended.loadManagementHelp")}
      </Text>
      <Group gap="lg" style={{ gridColumn: "1 / -1" }} wrap="wrap">
        <Checkbox
          label={t("sourceFormFieldsExtended.cacheEnabled")}
          checked={form.cacheEnabled}
          onChange={(e) => setForm({ ...form, cacheEnabled: e.currentTarget.checked })}
          data-testid="cache-enabled-checkbox"
        />
      </Group>
      <ReplicateSelect
        value={form.replicate}
        onChange={(replicate) => setForm({ ...form, replicate })}
        scope="source"
        loadProtected={form.loadProtected}
        testId="source-replicate-select"
      />
      <NumberInput
        label={t("sourceFormFieldsExtended.cacheTtlSeconds")}
        min={0}
        value={form.cacheTtl === "" ? "" : Number(form.cacheTtl)}
        onChange={(v) => setForm({ ...form, cacheTtl: v === "" ? "" : String(v) })}
        placeholder={t("sourceFormFieldsExtended.cacheTtlPlaceholder")}
        error={
          ttlSignalMissingCacheTtl(
            form.changeSignal,
            sourceFormCacheTtl(form.cacheTtl),
            sourceFormLanded(form),
          )
            ? t("sourceFormFieldsExtended.cacheTtlRequiredForSignal", {
                signal: form.changeSignal,
              })
            : undefined
        }
        data-testid="cache-ttl-input"
      />
      <Tooltip label={t("sourceFormFieldsExtended.changeSignalTooltip")} multiline w={280}>
        <Select
          label={t("sourceFormFieldsExtended.changeSignal")}
          value={form.changeSignal}
          onChange={(v) => setForm({ ...form, changeSignal: v ?? "" })}
          data={sourceChangeSignals(form.type).map((cs) => ({
            value: cs,
            label: CHANGE_SIGNAL_LABELS[cs] ?? cs,
          }))}
          allowDeselect={false}
          data-testid="change-signal-select"
        />
      </Tooltip>
      <Tooltip label={t("sourceFormFieldsExtended.sentinelPathTooltip")} multiline w={280}>
        <TextInput
          label={t("sourceFormFieldsExtended.sentinelPath")}
          value={form.sentinelPath}
          onChange={(e) => setForm({ ...form, sentinelPath: e.currentTarget.value })}
          placeholder="https://example.com/data/_SUCCESS"
          error={
            sentinelPathValid(form.sentinelPath)
              ? undefined
              : t("sourceFormFieldsExtended.sentinelPathInvalid")
          }
          data-testid="sentinel-path-input"
        />
      </Tooltip>
      <Tooltip label={t("sourceFormFieldsExtended.freshnessGateTooltip")} multiline w={280}>
        <Checkbox
          label={t("sourceFormFieldsExtended.freshnessGate")}
          checked={form.freshnessGate}
          onChange={(e) => setForm({ ...form, freshnessGate: e.currentTarget.checked })}
          data-testid="freshness-gate-checkbox"
        />
      </Tooltip>
      <NumberInput
        label={t("sourceFormFieldsExtended.maxLiveConcurrency")}
        description={t("sourceFormFieldsExtended.maxLiveConcurrencyHelp")}
        min={1}
        allowDecimal={false}
        allowNegative={false}
        value={form.maxLiveConcurrency === "" ? "" : Number(form.maxLiveConcurrency)}
        onChange={(v) => setForm({ ...form, maxLiveConcurrency: v === "" ? "" : String(v) })}
        placeholder={t("sourceFormFieldsExtended.maxLiveConcurrencyPlaceholder")}
        error={
          maxLiveConcurrencyValid(form.maxLiveConcurrency)
            ? undefined
            : t("sourceFormFieldsExtended.maxLiveConcurrencyInvalid")
        }
        data-testid="max-live-concurrency-input"
      />
      {/* REQ-1141: source-level load protection (scheduled-refresh-only, off-peak window). */}
      <Group gap="lg" style={{ gridColumn: "1 / -1" }} wrap="wrap" align="flex-end">
        <Tooltip label={t("sourceFormFieldsExtended.loadProtectedTooltip")} multiline w={280}>
          <Checkbox
            label={t("sourceFormFieldsExtended.loadProtected")}
            checked={form.loadProtected}
            onChange={(e) => setForm({ ...form, loadProtected: e.currentTarget.checked })}
            data-testid="load-protected-checkbox"
          />
        </Tooltip>
        {form.loadProtected && (
          <>
            <TextInput
              label={t("sourceFormFieldsExtended.offPeakWindow")}
              value={form.offPeakWindow}
              onChange={(e) => setForm({ ...form, offPeakWindow: e.currentTarget.value })}
              placeholder="01:00-05:00"
              data-testid="off-peak-window-input"
            />
            <TextInput
              label={t("sourceFormFieldsExtended.offPeakTz")}
              value={form.offPeakTz}
              onChange={(e) => setForm({ ...form, offPeakTz: e.currentTarget.value })}
              placeholder="UTC"
              data-testid="off-peak-tz-input"
            />
          </>
        )}
      </Group>
    </CollapsibleSection>
  );
}
