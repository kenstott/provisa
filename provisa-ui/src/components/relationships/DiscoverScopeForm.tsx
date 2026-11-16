// Copyright (c) 2026 Kenneth Stott
// Canary: 8f3c6a21-5d7e-4b90-a2f4-c63e1b8d5a07
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: what a discovery run looks at, set before anything runs. The kinds (Semantics greyed
// out when no vendor has a key), the model for this run, the source domains and tables, and the
// target, the same as the source unless that is unticked. Tables default to every table of the
// chosen domains, domains to those ticked in the Domains picker; system domains are never offered.
// About how many requests the run makes is shown before Run, counted again as the scope changes.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Text } from "@mantine/core";
import {
  countDiscoveryRun,
  fetchDiscoveryModels,
  type DiscoveryCount,
  type DiscoveryKind,
  type DiscoveryModels,
  type DiscoveryScope,
  type TypeGroup,
} from "../../api/admin";
import { isInternalDomain } from "../../lib/internalDomains";
import { MultiSelect, type MultiSelectOption } from "../MultiSelect";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { DiscoverModelField } from "./DiscoverModelField";
import { typeGroupLabel } from "./discoveryResult";

/** A registered table, as the scope offers it. */
export interface ScopeTable {
  id: number;
  domainId: string;
  tableName: string;
}

/** How long the scope stays unchanged before its requests are counted. */
export const COUNT_DEBOUNCE_MS = 400;

const GROUP_ORDER: TypeGroup[] = [
  "whole_number",
  "decimal",
  "text",
  "uuid",
  "date",
  "timestamp",
  "timestamptz",
];

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

type Counted =
  | { state: "idle" }
  | { state: "counting" }
  | { state: "counted"; count: DiscoveryCount }
  | { state: "failed"; error: string };

export function DiscoverScopeForm({
  tables,
  defaultDomains,
  onCancel,
  onRun,
}: {
  tables: ScopeTable[];
  /** The domains ticked in the Domains picker. */
  defaultDomains: string[];
  onCancel: () => void;
  /** Start the run; a refusal is thrown with the server's words. */
  onRun: (scope: DiscoveryScope) => Promise<void>;
}) {
  const { t } = useTranslation();
  const [models, setModels] = useState<DiscoveryModels | null>(null);
  const [modelsError, setModelsError] = useState("");
  const [foreignKeys, setForeignKeys] = useState(true);
  const [semantics, setSemantics] = useState(true);
  const [vendor, setVendor] = useState("");
  const [model, setModel] = useState("");

  const domains = useMemo(
    () =>
      [...new Set(tables.map((tb) => tb.domainId))]
        .filter((d) => !isInternalDomain(d))
        .sort((a, b) => a.localeCompare(b)),
    [tables],
  );
  const [sourceDomains, setSourceDomains] = useState<string[]>(() =>
    defaultDomains.filter((d) => domains.includes(d)),
  );
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [sameAsSource, setSameAsSource] = useState(true);
  const [targetDomains, setTargetDomains] = useState<string[]>([]);
  const [targetTables, setTargetTables] = useState<string[]>([]);
  const [counted, setCounted] = useState<Counted>({ state: "idle" });
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState("");

  useEffect(() => {
    let live = true;
    fetchDiscoveryModels()
      .then((m) => {
        if (!live) return;
        setModels(m);
        if (!m.semantics_available) setSemantics(false);
        if (m.assigned) {
          setVendor(m.assigned.vendor);
          setModel(m.assigned.model);
        } else if (m.vendors.length > 0) {
          setVendor(m.vendors[0]);
        }
      })
      .catch((e) => {
        if (!live) return;
        // Which models can run is not known: semantics cannot be asked for, and the reason is shown.
        setModelsError(errorText(e));
        setSemantics(false);
      });
    return () => {
      live = false;
    };
  }, []);

  const semanticsAvailable = models?.semantics_available === true;
  const kinds: DiscoveryKind[] = [
    ...(foreignKeys ? (["foreign_keys"] as const) : []),
    ...(semantics && semanticsAvailable ? (["semantics"] as const) : []),
  ];
  const asksSemantics = kinds.includes("semantics");
  const assigned = models?.assigned ?? null;
  const isAssigned = assigned !== null && vendor === assigned.vendor && model === assigned.model;
  const modelIncomplete = asksSemantics && (!vendor || !model.trim());

  // Empty means every domain offered; tables, every table of the chosen domains.
  const effective = (chosen: string[]) => (chosen.length > 0 ? chosen : domains);
  const scope: DiscoveryScope | null =
    kinds.length === 0 || modelIncomplete
      ? null
      : {
          kinds,
          source_domains: effective(sourceDomains),
          source_tables: sourceTables.length > 0 ? sourceTables.map(Number) : null,
          target_same_as_source: sameAsSource,
          target_domains: sameAsSource ? [] : effective(targetDomains),
          target_tables: !sameAsSource && targetTables.length > 0 ? targetTables.map(Number) : null,
          vendor: asksSemantics && !isAssigned ? vendor : null,
          model: asksSemantics && !isAssigned ? model.trim() : null,
        };
  const scopeKey = scope === null ? null : JSON.stringify(scope);

  useEffect(() => {
    if (scopeKey === null || models === null) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- nothing to count
      setCounted({ state: "idle" });
      return;
    }
    let live = true;
    setCounted({ state: "counting" });
    const timer = setTimeout(() => {
      countDiscoveryRun(JSON.parse(scopeKey) as DiscoveryScope)
        .then((count) => live && setCounted({ state: "counted", count }))
        .catch((e) => live && setCounted({ state: "failed", error: errorText(e) }));
    }, COUNT_DEBOUNCE_MS);
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [scopeKey, models]);

  const domainOptions: MultiSelectOption[] = domains.map((d) => ({
    id: d,
    label: d || t("discoverDialog.noDomain"),
  }));
  const tableOptions = (chosen: string[]): MultiSelectOption[] => {
    const within = new Set(effective(chosen));
    return tables
      .filter((tb) => within.has(tb.domainId))
      .map((tb) => ({
        id: String(tb.id),
        label: tb.tableName,
        group: tb.domainId || t("discoverDialog.noDomain"),
      }));
  };
  // A table outside the domains now chosen leaves the selection with its domain.
  const keepWithin = (chosenTables: string[], chosenDomains: string[]) => {
    const within = new Set(effective(chosenDomains));
    return chosenTables.filter((id) =>
      tables.some((tb) => String(tb.id) === id && within.has(tb.domainId)),
    );
  };

  const run = async () => {
    if (scope === null) return;
    setStarting(true);
    setStartError("");
    try {
      await onRun(scope);
    } catch (e) {
      setStartError(errorText(e));
    } finally {
      setStarting(false);
    }
  };

  const countLine = (count: DiscoveryCount) => {
    const groups = GROUP_ORDER.flatMap((g) => {
      const n = count.by_group[g];
      return n ? [t("discoverDialog.groupCount", { group: typeGroupLabel(g, t), count: n })] : [];
    });
    return count.requests > 0
      ? t("discoverDialog.count", { count: count.requests, groups: groups.join(", ") })
      : t("discoverDialog.countNone");
  };

  return (
    <>
      <Text mb="md">{t("discoverDialog.intro")}</Text>
      {modelsError && (
        <Alert color="red" mb="md" data-testid="discover-dialog-error">
          {modelsError}
        </Alert>
      )}
      <FormLayout>
        <FormChecks>
          <Checkbox
            label={t("discoverDialog.foreignKeys")}
            checked={foreignKeys}
            onChange={(e) => setForeignKeys(e.currentTarget.checked)}
            data-testid="discover-kind-foreign-keys"
          />
          <Checkbox
            label={
              models !== null && !semanticsAvailable
                ? t("discoverDialog.semanticsNoKey")
                : t("discoverDialog.semantics")
            }
            checked={semantics && semanticsAvailable}
            disabled={!semanticsAvailable}
            onChange={(e) => setSemantics(e.currentTarget.checked)}
            data-testid="discover-kind-semantics"
          />
        </FormChecks>
        {models !== null && semanticsAvailable && (
          <DiscoverModelField
            models={models}
            vendor={vendor}
            model={model}
            onChange={(v, m) => {
              setVendor(v);
              setModel(m);
            }}
            disabled={!asksSemantics}
          />
        )}
        <FormField
          size="long"
          hint={t("discoverDialog.domainsHint")}
          data-testid="discover-source-domains"
        >
          <MultiSelect
            label={t("discoverDialog.sourceDomains")}
            options={domainOptions}
            value={sourceDomains}
            onChange={(next) => {
              setSourceDomains(next);
              setSourceTables((chosen) => keepWithin(chosen, next));
            }}
          />
        </FormField>
        <FormField
          size="long"
          hint={t("discoverDialog.tablesHint")}
          data-testid="discover-source-tables"
        >
          <MultiSelect
            label={t("discoverDialog.sourceTables")}
            options={tableOptions(sourceDomains)}
            value={sourceTables}
            onChange={setSourceTables}
          />
        </FormField>
        <FormChecks>
          <FormField size="long" help={t("discoverDialog.sameAsSourceHelp")}>
            <Checkbox
              label={t("discoverDialog.sameAsSource")}
              checked={sameAsSource}
              onChange={(e) => setSameAsSource(e.currentTarget.checked)}
              data-testid="discover-same-as-source"
            />
          </FormField>
        </FormChecks>
        {!sameAsSource && (
          <>
            <FormField
              size="long"
              hint={t("discoverDialog.domainsHint")}
              data-testid="discover-target-domains"
            >
              <MultiSelect
                label={t("discoverDialog.targetDomains")}
                options={domainOptions}
                value={targetDomains}
                onChange={(next) => {
                  setTargetDomains(next);
                  setTargetTables((chosen) => keepWithin(chosen, next));
                }}
              />
            </FormField>
            <FormField
              size="long"
              hint={t("discoverDialog.tablesHint")}
              data-testid="discover-target-tables"
            >
              <MultiSelect
                label={t("discoverDialog.targetTables")}
                options={tableOptions(targetDomains)}
                value={targetTables}
                onChange={setTargetTables}
              />
            </FormField>
          </>
        )}
        <FormField size="full">
          {counted.state === "failed" ? (
            <Alert color="red" data-testid="discover-count-error">
              {counted.error}
            </Alert>
          ) : (
            <Text fz="sm" c="dimmed" data-testid="discover-count" aria-live="polite">
              {counted.state === "counted" ? (
                <>
                  {asksSemantics ? countLine(counted.count) : t("discoverDialog.countNone")}{" "}
                  {t("discoverDialog.tables", {
                    source: counted.count.source_tables,
                    target: counted.count.target_tables,
                  })}
                </>
              ) : counted.state === "counting" ? (
                t("discoverDialog.counting")
              ) : null}
            </Text>
          )}
        </FormField>
        {startError && (
          <FormField size="full">
            <Alert color="red" data-testid="discover-start-error">
              {startError}
            </Alert>
          </FormField>
        )}
        <FormFooter
          onCancel={onCancel}
          cancelProps={{ "data-testid": "discover-cancel" }}
          action={
            <Button
              onClick={() => void run()}
              loading={starting}
              disabled={
                (models === null && !modelsError) ||
                scope === null ||
                counted.state === "failed" ||
                starting
              }
              data-testid="discover-run"
            >
              {t("discoverDialog.run")}
            </Button>
          }
        />
      </FormLayout>
    </>
  );
}
