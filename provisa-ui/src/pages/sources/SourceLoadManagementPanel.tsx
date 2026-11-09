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

import { NEVER_EXPIRES, cacheTtlHint } from "../../components/form/cacheTtl";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Checkbox, NumberInput, Select, Text, TextInput } from "@mantine/core";
import { SOURCE_PROBE_SOURCE_TYPES, sourceChangeSignals } from "../../liveCapability";
import { CHANGE_SIGNAL_LABELS } from "./constants";
import { CollapsibleSection } from "../tables/CollapsibleSection";
import { ReplicateSelect } from "../../components/admin/ReplicateSelect";
import type { SourceFormFieldsProps, SourceFormState } from "./SourceFormFields";
import {
  maxLiveConcurrencyValid,
  sentinelPathValid,
  sourceFormCacheTtl,
  sourceFormLanded,
  ttlSignalMissingCacheTtl,
} from "./loadManagement";
import { useSourceKind } from "./sourceKind";
import { FormChecks, FormField, FormLayout } from "../../components/form/FormLayout";
import { OffPeakWindowFields } from "../../components/admin/OffPeakWindowFields";

export function SourceLoadManagementPanel({
  form,
  setForm,
  fixed,
}: Pick<SourceFormFieldsProps, "form" | "setForm"> & {
  /** The settings shown read-only: those the source's connector fixes (REQ-2221). */
  fixed?: ReadonlySet<keyof SourceFormState>;
}) {
  const { t } = useTranslation();
  const isFixed = (key: keyof SourceFormState) => fixed?.has(key) === true;
  const kind = useSourceKind(form.type);
  const kindLands = kind?.cache_ttl_required === true;
  // Offered unless the server says a source of this kind can have no sentinel (REQ-1148).
  const sentinelApplies = kind?.sentinel_path !== false;
  const staleSentinel = !sentinelApplies && form.sentinelPath !== "";
  useEffect(() => {
    // A value typed for another kind is not carried into one that cannot have it.
    if (staleSentinel) setForm({ ...form, sentinelPath: "" });
    // eslint-disable-next-line react-hooks/exhaustive-deps -- only when the value turns stale
  }, [staleSentinel]);
  return (
    <FormField size="full">
      <CollapsibleSection
        // Open, with no toggle: an expander inside a form opens flat (maintainer, 2026-10-12).
        flat
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
        {fixed && fixed.size > 0 && (
          <Text size="xs" c="dimmed" data-testid="source-load-management-fixed">
            {t("connectorSetup.fixedHint")}
          </Text>
        )}
        <FormLayout>
          {kind?.live_only !== true && (
            <FormField size="medium">
              <ReplicateSelect
                value={form.replicate}
                onChange={(replicate) => setForm({ ...form, replicate })}
                // REQ-1141: load protection is the drop-down's last choice, not a setting beside it.
                onScheduledChange={(replicate, loadProtected) =>
                  // A source inherits from nothing: the drop-down reports true or false here.
                  setForm({ ...form, replicate, loadProtected: loadProtected === true })
                }
                scope="source"
                loadProtected={form.loadProtected}
                // One drop-down sets both: either being fixed holds it.
                disabled={isFixed("replicate") || isFixed("loadProtected")}
                testId="source-replicate-select"
              />
            </FormField>
          )}
          {/* Medium, not short: its error is a sentence. */}
          <FormField hint={cacheTtlHint(form.cacheTtl, t)} size="medium">
            <NumberInput
              min={NEVER_EXPIRES}
              label={t("sourceFormFieldsExtended.cacheTtlSeconds")}
              value={form.cacheTtl === "" ? "" : Number(form.cacheTtl)}
              onChange={(v) => setForm({ ...form, cacheTtl: v === "" ? "" : String(v) })}
              placeholder={t("sourceFormFieldsExtended.cacheTtlPlaceholder")}
              required={kindLands}
              readOnly={isFixed("cacheTtl")}
              error={
                ttlSignalMissingCacheTtl(
                  form.changeSignal,
                  sourceFormCacheTtl(form.cacheTtl),
                  sourceFormLanded(form, kindLands),
                )
                  ? t("sourceFormFieldsExtended.cacheTtlRequiredForSignal", {
                      signal: form.changeSignal,
                    })
                  : undefined
              }
              data-testid="cache-ttl-input"
            />
          </FormField>
          <FormField help={t("sourceFormFieldsExtended.changeSignalTooltip")} size="medium">
            <Select
              label={t("sourceFormFieldsExtended.changeSignal")}
              value={form.changeSignal}
              onChange={(v) => setForm({ ...form, changeSignal: v ?? "" })}
              data={sourceChangeSignals(form.type).map((cs) => ({
                value: cs,
                // A source that asks its provider whether it changed says the choice in words.
                label: SOURCE_PROBE_SOURCE_TYPES.has(form.type)
                  ? t(`sourceFormFieldsExtended.sourceProbeSignal.${cs}`)
                  : (CHANGE_SIGNAL_LABELS[cs] ?? cs),
              }))}
              allowDeselect={false}
              disabled={isFixed("changeSignal")}
              data-testid="change-signal-select"
            />
          </FormField>
          {sentinelApplies && (
            <FormField help={t("sourceFormFieldsExtended.sentinelPathTooltip")} size="long">
              <TextInput
                label={t("sourceFormFieldsExtended.sentinelPath")}
                value={form.sentinelPath}
                onChange={(e) => setForm({ ...form, sentinelPath: e.currentTarget.value })}
                placeholder="https://example.com/data/_SUCCESS"
                readOnly={isFixed("sentinelPath")}
                error={
                  sentinelPathValid(form.sentinelPath)
                    ? undefined
                    : t("sourceFormFieldsExtended.sentinelPathInvalid")
                }
                data-testid="sentinel-path-input"
              />
            </FormField>
          )}
          {/* Medium, not short: its placeholder is a word, cut in a short box in some languages. */}
          <FormField help={t("sourceFormFieldsExtended.maxLiveConcurrencyHelp")} size="medium">
            <NumberInput
              label={t("sourceFormFieldsExtended.maxLiveConcurrency")}
              min={1}
              allowDecimal={false}
              allowNegative={false}
              value={form.maxLiveConcurrency === "" ? "" : Number(form.maxLiveConcurrency)}
              onChange={(v) => setForm({ ...form, maxLiveConcurrency: v === "" ? "" : String(v) })}
              placeholder={t("sourceFormFieldsExtended.maxLiveConcurrencyPlaceholder")}
              readOnly={isFixed("maxLiveConcurrency")}
              error={
                maxLiveConcurrencyValid(form.maxLiveConcurrency)
                  ? undefined
                  : t("sourceFormFieldsExtended.maxLiveConcurrencyInvalid")
              }
              data-testid="max-live-concurrency-input"
            />
          </FormField>
          {/* REQ-1141: when a source read only on schedule is refreshed. Both are optional: with
              no window the refresh follows the Cache TTL or the change probe. */}
          {form.loadProtected && (
            <OffPeakWindowFields
              window={form.offPeakWindow}
              tz={form.offPeakTz}
              onWindowChange={(offPeakWindow) => setForm({ ...form, offPeakWindow })}
              onTzChange={(offPeakTz) => setForm({ ...form, offPeakTz })}
              windowReadOnly={isFixed("offPeakWindow")}
              tzReadOnly={isFixed("offPeakTz")}
            />
          )}
          {/* The ticks sit together beneath the fields: caching, the freshness gate. */}
          <FormChecks>
            <FormField size="short" help={t("sourceFormFieldsExtended.cacheEnabledHelp")}>
              <Checkbox
                label={t("sourceFormFieldsExtended.cacheEnabled")}
                checked={form.cacheEnabled}
                onChange={(e) => setForm({ ...form, cacheEnabled: e.currentTarget.checked })}
                disabled={isFixed("cacheEnabled")}
                data-testid="cache-enabled-checkbox"
              />
            </FormField>
            <FormField help={t("sourceFormFieldsExtended.freshnessGateTooltip")} size="short">
              <Checkbox
                label={t("sourceFormFieldsExtended.freshnessGate")}
                checked={form.freshnessGate}
                onChange={(e) => setForm({ ...form, freshnessGate: e.currentTarget.checked })}
                disabled={isFixed("freshnessGate")}
                data-testid="freshness-gate-checkbox"
              />
            </FormField>
          </FormChecks>
        </FormLayout>
      </CollapsibleSection>
    </FormField>
  );
}
