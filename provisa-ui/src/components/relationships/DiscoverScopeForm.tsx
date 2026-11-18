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
// At most how many requests the run makes is shown before Run, counted again as the scope changes:
// the count reads no table, so it is an upper bound, and it says how many tables are never read.
//
// A run after the first starts from the fields of the one before (`initial`): its kinds, model and
// sets. A value that no longer holds (a domain or table no longer offered, a vendor with no key, a
// model its vendor no longer lists) is left out and said; the rest is kept. Tables registered since
// that run in domains outside the source are named, with one action adding their domains to the
// source (and to a target of its own). "Reset to defaults" sets the form up as a first run.
//
// Key value overlap (REQ-413, amended 2026-11-16) shows once Semantics is ticked: it measures the
// semantic pairs the model rated between its cutoff and the threshold, so it is offered only with
// Semantics. Ticked, it shows its cutoff and its sample size (the most distinct values read of each
// column). All three start as the person last set them, kept by the server when a run starts;
// "Reset to defaults" restores the defaults.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Group, NumberInput, Text } from "@mantine/core";
import {
  countDiscoveryRun,
  fetchDiscoveryKeyValues,
  fetchDiscoveryModels,
  fetchVendorModels,
  type DiscoveryCount,
  type DiscoveryModels,
  type DiscoveryRunKind,
  type DiscoveryScope,
  type KeyValueSettings,
} from "../../api/admin";
import { isInternalDomain } from "../../lib/internalDomains";
import { MultiSelect, type MultiSelectOption } from "../MultiSelect";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { DiscoverModelField } from "./DiscoverModelField";
import { registeredSince, restoreSets, type LastRun, type RestoreNote } from "./restoreScope";
import { GROUP_ORDER, typeGroupLabel } from "./discoveryResult";

/** A registered table, as the scope offers it. */
export interface ScopeTable {
  id: number;
  domainId: string;
  tableName: string;
  /** When it was registered (ISO 8601); null only for a draft not yet registered. */
  registeredAt: string | null;
}

/** How long the scope stays unchanged before its requests are counted. */
export const COUNT_DEBOUNCE_MS = 400;

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

type Counted =
  | { state: "idle" }
  | { state: "counting" }
  | { state: "counted"; count: DiscoveryCount }
  | { state: "failed"; error: string };

/** Key value overlap as the form starts it: the run "New run" left, the defaults after "Reset to
 *  defaults", else as the person last set it. A run that did not choose it keeps the person's
 *  last cutoff and sample size, unchosen. */
function startingKeyValues(
  read: KeyValueSettings & { defaults: KeyValueSettings },
  initial: LastRun | null,
  fromDefaults: boolean,
): KeyValueSettings {
  if (fromDefaults) return read.defaults;
  if (initial === null)
    return { chosen: read.chosen, cutoff: read.cutoff, sample_size: read.sample_size };
  const { kinds, measure_cutoff: cutoff, sample_size: size } = initial.scope;
  if (kinds.includes("key_values") && cutoff != null && size != null) {
    return { chosen: true, cutoff, sample_size: size };
  }
  return { chosen: false, cutoff: read.cutoff, sample_size: read.sample_size };
}

export function DiscoverScopeForm({
  tables,
  defaultDomains,
  initial,
  fromDefaults,
  onReset,
  onCancel,
  onRun,
}: {
  tables: ScopeTable[];
  /** The domains ticked in the Domains picker. */
  defaultDomains: string[];
  /** The run before, whose fields this one starts from; null: the defaults. */
  initial: LastRun | null;
  /** Set up by "Reset to defaults": Key value overlap takes its defaults, not the person's own. */
  fromDefaults: boolean;
  /** Set the form up from the defaults. */
  onReset: () => void;
  onCancel: () => void;
  /** Start the run; a refusal is thrown with the server's words. */
  onRun: (scope: DiscoveryScope) => Promise<void>;
}) {
  const { t } = useTranslation();
  const [models, setModels] = useState<DiscoveryModels | null>(null);
  const [modelsError, setModelsError] = useState("");
  const [foreignKeys, setForeignKeys] = useState(
    initial === null || initial.scope.kinds.includes("foreign_keys"),
  );
  const [semantics, setSemantics] = useState(
    initial === null || initial.scope.kinds.includes("semantics"),
  );
  const [vendor, setVendor] = useState("");
  const [model, setModel] = useState("");
  // Key value overlap; null until the person's own is read.
  const [keyValues, setKeyValues] = useState<KeyValueSettings | null>(null);
  const [keyValuesError, setKeyValuesError] = useState("");

  const domains = useMemo(
    () =>
      [...new Set(tables.map((tb) => tb.domainId))]
        .filter((d) => !isInternalDomain(d))
        .sort((a, b) => a.localeCompare(b)),
    [tables],
  );
  // Read once, as the form mounts: what the run before asked for, as the form can hold it now.
  const [restored] = useState(() =>
    initial === null ? null : restoreSets(initial, domains, tables),
  );
  const [modelNotes, setModelNotes] = useState<RestoreNote[]>([]);
  const [sourceDomains, setSourceDomains] = useState<string[]>(
    () => restored?.sourceDomains ?? defaultDomains.filter((d) => domains.includes(d)),
  );
  const [sourceTables, setSourceTables] = useState<string[]>(restored?.sourceTables ?? []);
  const [sameAsSource, setSameAsSource] = useState(restored?.sameAsSource ?? true);
  const [targetDomains, setTargetDomains] = useState<string[]>(restored?.targetDomains ?? []);
  const [targetTables, setTargetTables] = useState<string[]>(restored?.targetTables ?? []);
  const [counted, setCounted] = useState<Counted>({ state: "idle" });
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState("");

  useEffect(() => {
    let live = true;
    fetchDiscoveryKeyValues()
      .then((read) => live && setKeyValues(startingKeyValues(read, initial, fromDefaults)))
      .catch((e) => live && setKeyValuesError(errorText(e)));
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read once as the form mounts
  }, []);

  useEffect(() => {
    let live = true;
    fetchDiscoveryModels()
      .then((m) => {
        if (!live) return;
        setModels(m);
        if (!m.semantics_available) setSemantics(false);
        const assigned = () => {
          if (m.assigned) {
            setVendor(m.assigned.vendor);
            setModel(m.assigned.model);
          } else if (m.vendors.length > 0) {
            setVendor(m.vendors[0]);
            setModel("");
          }
        };
        // The run before named a model for itself (null: it ran the assigned one).
        const asked =
          initial !== null && initial.scope.vendor !== null && initial.scope.model !== null
            ? { vendor: initial.scope.vendor, model: initial.scope.model }
            : null;
        if (asked === null || !m.semantics_available) {
          assigned();
        } else if (!m.vendors.includes(asked.vendor)) {
          assigned();
          setModelNotes([{ kind: "vendorNoKey", vendor: asked.vendor }]);
        } else {
          setVendor(asked.vendor);
          setModel(asked.model);
          fetchVendorModels(asked.vendor)
            .then((listed) => {
              if (!live || listed.includes(asked.model)) return;
              assigned();
              setModelNotes([{ kind: "modelGone", ...asked }]);
            })
            // A vendor that lists no models takes a typed name, which nothing here can check:
            // the model stands, and the count or the run refuses it if the vendor does.
            .catch(() => undefined);
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
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read once as the form mounts
  }, []);

  const notes = [...(restored?.notes ?? []), ...modelNotes];
  const noteText = (note: RestoreNote) => {
    switch (note.kind) {
      case "domainsGone":
        return t("discoverDialog.restore.domainsGone", {
          count: note.names.length,
          names: note.names.join(", "),
        });
      case "tablesGone":
        return t("discoverDialog.restore.tablesGone", { count: note.count });
      case "vendorNoKey":
        return t("discoverDialog.restore.vendorNoKey", { vendor: note.vendor });
      case "modelGone":
        return t("discoverDialog.restore.modelGone", { vendor: note.vendor, model: note.model });
    }
  };

  const semanticsAvailable = models?.semantics_available === true;
  const asksSemantics = semantics && semanticsAvailable;
  const measures = asksSemantics && keyValues !== null && keyValues.chosen;
  const kinds: DiscoveryRunKind[] = [
    ...(foreignKeys ? (["foreign_keys"] as const) : []),
    ...(asksSemantics ? (["semantics"] as const) : []),
    ...(measures ? (["key_values"] as const) : []),
  ];
  const assigned = models?.assigned ?? null;
  const isAssigned = assigned !== null && vendor === assigned.vendor && model === assigned.model;
  const modelIncomplete = asksSemantics && (!vendor || !model.trim());

  // Empty means every domain offered; tables, every table of the chosen domains.
  const effective = (chosen: string[]) => (chosen.length > 0 ? chosen : domains);
  // Semantics waits for Key value overlap to be read, which it is offered with.
  const scope: DiscoveryScope | null =
    kinds.length === 0 || modelIncomplete || (asksSemantics && keyValues === null)
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
          measure_cutoff: measures ? keyValues.cutoff : null,
          sample_size: measures ? keyValues.sample_size : null,
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

  // Tables registered since the run before, in domains the source leaves out.
  const newOutside =
    initial === null
      ? []
      : tables.filter(
          (tb) =>
            domains.includes(tb.domainId) &&
            !effective(sourceDomains).includes(tb.domainId) &&
            registeredSince(tb, initial.startedAt),
        );
  const newDomains = [...new Set(newOutside.map((tb) => tb.domainId))].sort((a, b) =>
    a.localeCompare(b),
  );
  // Their domains join the source, and a target of its own; a table list set by hand takes every
  // table of them, since the list is what the run reads.
  const addNew = () => {
    const theirTables = tables
      .filter((tb) => newDomains.includes(tb.domainId))
      .map((tb) => String(tb.id));
    const widen = (chosenDomains: string[], chosenTables: string[]) => ({
      domains: [...chosenDomains, ...newDomains.filter((d) => !chosenDomains.includes(d))],
      tables: chosenTables.length > 0 ? [...chosenTables, ...theirTables] : chosenTables,
    });
    const source = widen(sourceDomains, sourceTables);
    setSourceDomains(source.domains);
    setSourceTables(source.tables);
    if (!sameAsSource && targetDomains.length > 0) {
      const target = widen(targetDomains, targetTables);
      setTargetDomains(target.domains);
      setTargetTables(target.tables);
    }
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
      {initial !== null && (
        <Group justify="space-between" align="center" mb="md" wrap="nowrap">
          <Text fz="sm" c="dimmed" data-testid="discover-from-last-run">
            {t("discoverDialog.restore.fromLastRun")}
          </Text>
          <Button
            variant="default"
            size="xs"
            onClick={onReset}
            data-testid="discover-reset-defaults"
            style={{ flexShrink: 0 }}
          >
            {t("discoverDialog.restore.reset")}
          </Button>
        </Group>
      )}
      {newOutside.length > 0 && (
        <Group justify="space-between" align="center" mb="md" wrap="nowrap">
          <Text fz="sm" data-testid="discover-new-tables">
            {t("discoverDialog.restore.newTables", {
              count: newOutside.length,
              domains: newDomains.map((d) => d || t("discoverDialog.noDomain")).join(", "),
            })}
          </Text>
          <Button
            variant="default"
            size="xs"
            onClick={addNew}
            data-testid="discover-add-new-tables"
            style={{ flexShrink: 0 }}
          >
            {t("discoverDialog.restore.addNewTables")}
          </Button>
        </Group>
      )}
      {notes.length > 0 && (
        <Alert color="yellow" mb="md" data-testid="discover-restore-notes">
          <Text fz="sm">{t("discoverDialog.restore.leftOut")}</Text>
          {notes.map((note) => (
            <Text key={noteText(note)} fz="sm">
              {noteText(note)}
            </Text>
          ))}
        </Alert>
      )}
      {modelsError && (
        <Alert color="red" mb="md" data-testid="discover-dialog-error">
          {modelsError}
        </Alert>
      )}
      {keyValuesError && (
        <Alert color="red" mb="md" data-testid="discover-key-values-error">
          {keyValuesError}
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
          {asksSemantics && keyValues !== null && (
            <Checkbox
              label={t("discoverDialog.keyValues")}
              checked={keyValues.chosen}
              onChange={(e) => {
                const chosen = e.currentTarget.checked;
                setKeyValues((kv) => (kv === null ? kv : { ...kv, chosen }));
              }}
              data-testid="discover-kind-key-values"
            />
          )}
        </FormChecks>
        {measures && (
          <>
            <FormField size="short" hint={t("discoverDialog.cutoffHint")}>
              <NumberInput
                label={t("discoverDialog.cutoff")}
                data-testid="discover-measure-cutoff"
                value={Math.round(keyValues.cutoff * 100)}
                onChange={(v) => {
                  if (typeof v === "number") {
                    setKeyValues((kv) => (kv === null ? kv : { ...kv, cutoff: v / 100 }));
                  }
                }}
                min={0}
                max={100}
                step={5}
                suffix="%"
                clampBehavior="strict"
                allowDecimal={false}
              />
            </FormField>
            <FormField size="short" hint={t("discoverDialog.sampleSizeHint")}>
              <NumberInput
                label={t("discoverDialog.sampleSize")}
                data-testid="discover-sample-size"
                value={keyValues.sample_size}
                onChange={(v) => {
                  if (typeof v === "number") {
                    setKeyValues((kv) => (kv === null ? kv : { ...kv, sample_size: v }));
                  }
                }}
                min={1}
                step={1000}
                thousandSeparator=","
                clampBehavior="strict"
                allowDecimal={false}
                allowNegative={false}
              />
            </FormField>
          </>
        )}
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
            <>
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
              {counted.state === "counted" && asksSemantics && counted.count.requests > 0 && (
                <Text fz="sm" c="dimmed" data-testid="discover-count-bound">
                  {t("discoverDialog.countBound")}
                </Text>
              )}
              {counted.state === "counted" && counted.count.tables_not_read > 0 && (
                <Text fz="sm" c="dimmed" data-testid="discover-not-read">
                  {t("discoverDialog.notRead", { count: counted.count.tables_not_read })}
                </Text>
              )}
            </>
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
